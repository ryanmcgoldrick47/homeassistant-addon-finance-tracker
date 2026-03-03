from __future__ import annotations

from datetime import datetime, timezone
from typing import Optional

import httpx
from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel
from sqlmodel import Session, select

from database import ShareHolding, get_session

router = APIRouter(prefix="/api/investments", tags=["investments"])

YAHOO_BASE = "https://query2.finance.yahoo.com/v8/finance/chart"
YAHOO_HEADERS = {"User-Agent": "Mozilla/5.0"}


async def _fetch_yahoo_price(client: httpx.AsyncClient, ticker: str) -> tuple[float, str]:
    """Return (price, currency) for a Yahoo Finance ticker."""
    r = await client.get(
        f"{YAHOO_BASE}/{ticker}",
        params={"interval": "1d", "range": "1d"},
        headers=YAHOO_HEADERS,
        timeout=10,
    )
    r.raise_for_status()
    meta = r.json()["chart"]["result"][0]["meta"]
    price = float(meta["regularMarketPrice"])
    currency = meta.get("currency", "USD").upper()
    return price, currency


async def _fetch_aud_per_usd(client: httpx.AsyncClient) -> float:
    """Return AUD per 1 USD using Yahoo's AUDUSD=X rate (inverted)."""
    try:
        price, _ = await _fetch_yahoo_price(client, "AUDUSD=X")
        return round(1 / price, 6)
    except Exception:
        return 1.58  # fallback


def _computed_fields(h: ShareHolding) -> dict:
    m = h.model_dump()
    m["gain_display"] = round(h.gain_aud, 2)
    m["gain_pct_display"] = round(h.gain_pct, 2)
    return m


@router.get("")
def list_holdings(session: Session = Depends(get_session)):
    holdings = session.exec(select(ShareHolding).order_by(ShareHolding.value_aud.desc())).all()
    return [_computed_fields(h) for h in holdings]


@router.get("/benchmark")
async def benchmark(session: Session = Depends(get_session)):
    """Compare portfolio total gain % vs ASX 200 and S&P 500 YTD performance."""
    holdings = session.exec(select(ShareHolding)).all()
    total_value = sum(h.value_aud for h in holdings)
    total_cost = sum(h.cost_basis_aud for h in holdings)
    portfolio_gain_pct = round((total_value - total_cost) / total_cost * 100, 2) if total_cost > 0 else 0.0

    indices = [
        {"ticker": "^AXJO", "name": "ASX 200"},
        {"ticker": "^GSPC", "name": "S&P 500"},
    ]
    results = []
    async with httpx.AsyncClient() as client:
        for idx in indices:
            try:
                r = await client.get(
                    f"{YAHOO_BASE}/{idx['ticker']}",
                    params={"interval": "1d", "range": "ytd"},
                    headers=YAHOO_HEADERS,
                    timeout=10,
                )
                r.raise_for_status()
                data = r.json()["chart"]["result"][0]
                closes = [c for c in data["indicators"]["quote"][0]["close"] if c is not None]
                ytd_pct = round((closes[-1] - closes[0]) / closes[0] * 100, 2) if len(closes) >= 2 else None
                results.append({"ticker": idx["ticker"], "name": idx["name"], "ytd_pct": ytd_pct})
            except Exception as e:
                results.append({"ticker": idx["ticker"], "name": idx["name"], "ytd_pct": None, "error": str(e)[:80]})

    return {
        "portfolio_gain_pct": portfolio_gain_pct,
        "portfolio_value_aud": round(total_value, 2),
        "portfolio_cost_aud": round(total_cost, 2),
        "indices": results,
    }


@router.get("/summary")
def holdings_summary(session: Session = Depends(get_session)):
    holdings = session.exec(select(ShareHolding)).all()
    total_value = round(sum(h.value_aud for h in holdings), 2)
    total_cost = round(sum(h.cost_basis_aud for h in holdings), 2)
    total_gain = round(total_value - total_cost, 2)
    gain_pct = round(total_gain / total_cost * 100, 2) if total_cost > 0 else 0.0
    fetched_at = max((h.price_fetched_at for h in holdings if h.price_fetched_at), default=None)
    return {
        "total_value_aud": total_value,
        "total_cost_basis_aud": total_cost,
        "total_gain_aud": total_gain,
        "total_gain_pct": gain_pct,
        "holdings_count": len(holdings),
        "price_fetched_at": fetched_at,
    }


class HoldingCreate(BaseModel):
    ticker: str
    name: Optional[str] = None
    qty: float
    avg_cost_aud: float
    broker: str = "stake"
    notes: Optional[str] = None


@router.post("")
def create_holding(body: HoldingCreate, session: Session = Depends(get_session)):
    ticker = body.ticker.upper().strip()
    cost_basis = round(body.qty * body.avg_cost_aud, 2)
    h = ShareHolding(
        ticker=ticker,
        name=body.name,
        qty=body.qty,
        avg_cost_aud=body.avg_cost_aud,
        cost_basis_aud=cost_basis,
        broker=body.broker,
        notes=body.notes,
    )
    session.add(h)
    session.commit()
    session.refresh(h)
    return _computed_fields(h)


class HoldingUpdate(BaseModel):
    ticker: Optional[str] = None
    name: Optional[str] = None
    qty: Optional[float] = None
    avg_cost_aud: Optional[float] = None
    broker: Optional[str] = None
    notes: Optional[str] = None


@router.patch("/{holding_id}")
def update_holding(holding_id: int, body: HoldingUpdate, session: Session = Depends(get_session)):
    h = session.get(ShareHolding, holding_id)
    if not h:
        raise HTTPException(404, "Not found")
    if body.ticker is not None:
        h.ticker = body.ticker.upper().strip()
    if body.name is not None:
        h.name = body.name
    if body.qty is not None:
        h.qty = body.qty
    if body.avg_cost_aud is not None:
        h.avg_cost_aud = body.avg_cost_aud
    if body.broker is not None:
        h.broker = body.broker
    if body.notes is not None:
        h.notes = body.notes
    # Recompute cost basis
    h.cost_basis_aud = round(h.qty * h.avg_cost_aud, 2)
    # Recompute gain if we have a price
    if h.price_aud:
        h.value_aud = round(h.qty * h.price_aud, 2)
        h.gain_aud = round(h.value_aud - h.cost_basis_aud, 2)
        h.gain_pct = round(h.gain_aud / h.cost_basis_aud * 100, 2) if h.cost_basis_aud > 0 else 0.0
    session.add(h)
    session.commit()
    session.refresh(h)
    return _computed_fields(h)


@router.delete("/{holding_id}")
def delete_holding(holding_id: int, session: Session = Depends(get_session)):
    h = session.get(ShareHolding, holding_id)
    if not h:
        raise HTTPException(404, "Not found")
    session.delete(h)
    session.commit()
    return {"ok": True}


@router.post("/refresh-prices")
async def refresh_prices(session: Session = Depends(get_session)):
    """Fetch latest prices from Yahoo Finance for all holdings."""
    holdings = session.exec(select(ShareHolding)).all()
    if not holdings:
        return {"ok": True, "updated": 0, "errors": []}

    now_str = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S")
    errors = []

    async with httpx.AsyncClient() as client:
        aud_per_usd = await _fetch_aud_per_usd(client)

        for h in holdings:
            try:
                price, currency = await _fetch_yahoo_price(client, h.ticker)
                h.currency = currency
                if currency == "AUD":
                    h.price_aud = round(price, 4)
                else:
                    # Convert USD (or other) to AUD — approximate via USD rate
                    h.price_aud = round(price * aud_per_usd, 4)
                h.value_aud = round(h.qty * h.price_aud, 2)
                h.cost_basis_aud = round(h.qty * h.avg_cost_aud, 2)
                h.gain_aud = round(h.value_aud - h.cost_basis_aud, 2)
                h.gain_pct = round(h.gain_aud / h.cost_basis_aud * 100, 2) if h.cost_basis_aud > 0 else 0.0
                h.price_fetched_at = now_str
                session.add(h)
            except Exception as e:
                errors.append({"ticker": h.ticker, "error": str(e)[:120]})

    session.commit()

    updated = len(holdings) - len(errors)
    holdings_out = session.exec(select(ShareHolding).order_by(ShareHolding.value_aud.desc())).all()
    total_value = round(sum(h.value_aud for h in holdings_out), 2)
    return {
        "ok": True,
        "updated": updated,
        "total_value_aud": total_value,
        "errors": errors,
    }
