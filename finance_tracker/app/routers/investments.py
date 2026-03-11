from __future__ import annotations

import calendar
from datetime import datetime, timezone, date as date_type
from typing import Optional

import httpx
from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel
from sqlmodel import Session, select

from database import ShareHolding, CryptoHolding, get_session, User
from deps import get_current_user

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


async def _fetch_historical_aud_per_usd(client: httpx.AsyncClient, purchase_date: date_type) -> float:
    """Return AUD per 1 USD for a specific date using Yahoo Finance historical data."""
    try:
        # Get a 3-day window around the purchase date to handle weekends/holidays
        ts_start = calendar.timegm(purchase_date.timetuple()) - 86400
        ts_end   = calendar.timegm(purchase_date.timetuple()) + 86400 * 3
        r = await client.get(
            f"{YAHOO_BASE}/AUDUSD=X",
            params={"interval": "1d", "period1": ts_start, "period2": ts_end},
            headers=YAHOO_HEADERS,
            timeout=10,
        )
        r.raise_for_status()
        closes = r.json()["chart"]["result"][0]["indicators"]["quote"][0]["close"]
        closes = [c for c in closes if c is not None]
        if closes:
            return round(1 / closes[0], 6)  # AUDUSD=X gives USD per AUD → invert for AUD per USD
    except Exception:
        pass
    return await _fetch_aud_per_usd(client)  # fall back to current rate


def _computed_fields(h: ShareHolding) -> dict:
    m = h.model_dump()
    m["gain_display"] = round(h.gain_aud, 2)
    m["gain_pct_display"] = round(h.gain_pct, 2)
    return m


@router.get("")
async def list_holdings(
    session: Session = Depends(get_session),
    current_user: User = Depends(get_current_user),
):
    holdings = session.exec(
        select(ShareHolding).where(
            ShareHolding.user_id == current_user.id,
        ).order_by(ShareHolding.value_aud.desc())
    ).all()
    fetched_at = max((h.price_fetched_at for h in holdings if h.price_fetched_at), default=None)
    stale = _prices_stale(fetched_at)
    # Auto-refresh if stale
    if stale and holdings:
        async with httpx.AsyncClient() as client:
            aud_per_usd = await _fetch_aud_per_usd(client)
            now_str = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S")
            for h in holdings:
                try:
                    price, currency = await _fetch_yahoo_price(client, h.ticker)
                    h.currency = currency
                    h.price_aud = round(price * aud_per_usd, 4) if currency != "AUD" else round(price, 4)
                    h.value_aud = round(h.qty * h.price_aud, 2)
                    h.cost_basis_aud = round(h.qty * h.avg_cost_aud, 2)
                    h.gain_aud = round(h.value_aud - h.cost_basis_aud, 2)
                    h.gain_pct = round(h.gain_aud / h.cost_basis_aud * 100, 2) if h.cost_basis_aud > 0 else 0.0
                    h.price_fetched_at = now_str
                    session.add(h)
                except Exception:
                    pass
        session.commit()
        # Re-query after refresh
        holdings = session.exec(
            select(ShareHolding).where(
                ShareHolding.user_id == current_user.id,
            ).order_by(ShareHolding.value_aud.desc())
        ).all()
    return [_computed_fields(h) for h in holdings]


@router.get("/benchmark")
async def benchmark(
    session: Session = Depends(get_session),
    current_user: User = Depends(get_current_user),
):
    """Compare portfolio total gain % vs ASX 200 and S&P 500 YTD performance."""
    holdings = session.exec(
        select(ShareHolding).where(ShareHolding.user_id == current_user.id)
    ).all()
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
def holdings_summary(
    session: Session = Depends(get_session),
    current_user: User = Depends(get_current_user),
):
    holdings = session.exec(
        select(ShareHolding).where(ShareHolding.user_id == current_user.id)
    ).all()
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
        "prices_stale": _prices_stale(fetched_at),
    }


def _prices_stale(fetched_at: Optional[str], max_age_hours: int = 24) -> bool:
    """Return True if prices haven't been refreshed within max_age_hours."""
    if not fetched_at:
        return True
    try:
        dt = datetime.fromisoformat(fetched_at.replace("Z", "+00:00"))
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        age_hours = (datetime.now(timezone.utc) - dt).total_seconds() / 3600
        return age_hours > max_age_hours
    except Exception:
        return True


class HoldingCreate(BaseModel):
    ticker: str
    name: Optional[str] = None
    qty: float
    avg_cost_aud: float
    purchase_currency: str = "AUD"  # "AUD" or "USD" — auto-converted to AUD on save
    purchase_date: Optional[str] = None  # YYYY-MM-DD — used for historical FX lookup
    broker: str = "stake"
    notes: Optional[str] = None


@router.post("")
async def create_holding(
    body: HoldingCreate,
    session: Session = Depends(get_session),
    current_user: User = Depends(get_current_user),
):
    ticker = body.ticker.upper().strip()
    avg_cost_aud = body.avg_cost_aud
    fx_rate_used: Optional[float] = None
    parsed_date: Optional[date_type] = None

    if body.purchase_date:
        try:
            parsed_date = date_type.fromisoformat(body.purchase_date)
        except ValueError:
            pass

    # If price was entered in USD, convert to AUD using historical or current rate
    if body.purchase_currency.upper() == "USD":
        async with httpx.AsyncClient() as client:
            if parsed_date:
                fx_rate_used = await _fetch_historical_aud_per_usd(client, parsed_date)
            else:
                fx_rate_used = await _fetch_aud_per_usd(client)
        avg_cost_aud = round(avg_cost_aud * fx_rate_used, 4)

    cost_basis = round(body.qty * avg_cost_aud, 2)
    h = ShareHolding(
        ticker=ticker,
        name=body.name,
        qty=body.qty,
        avg_cost_aud=avg_cost_aud,
        cost_basis_aud=cost_basis,
        purchase_date=parsed_date,
        purchase_fx_rate=fx_rate_used,
        broker=body.broker,
        notes=body.notes,
        user_id=current_user.id,
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
def update_holding(
    holding_id: int,
    body: HoldingUpdate,
    session: Session = Depends(get_session),
    current_user: User = Depends(get_current_user),
):
    h = session.get(ShareHolding, holding_id)
    if not h:
        raise HTTPException(404, "Not found")
    if h.user_id != current_user.id:
        raise HTTPException(403, "Access denied")
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
def delete_holding(
    holding_id: int,
    session: Session = Depends(get_session),
    current_user: User = Depends(get_current_user),
):
    h = session.get(ShareHolding, holding_id)
    if not h:
        raise HTTPException(404, "Not found")
    if h.user_id != current_user.id:
        raise HTTPException(403, "Access denied")
    session.delete(h)
    session.commit()
    return {"ok": True}


@router.post("/refresh-prices")
async def refresh_prices(
    session: Session = Depends(get_session),
    current_user: User = Depends(get_current_user),
):
    """Fetch latest prices from Yahoo Finance for all holdings."""
    holdings = session.exec(
        select(ShareHolding).where(ShareHolding.user_id == current_user.id)
    ).all()
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
    holdings_out = session.exec(
        select(ShareHolding).where(
            ShareHolding.user_id == current_user.id,
        ).order_by(ShareHolding.value_aud.desc())
    ).all()
    total_value = round(sum(h.value_aud for h in holdings_out), 2)
    return {
        "ok": True,
        "updated": updated,
        "total_value_aud": total_value,
        "errors": errors,
    }


@router.get("/tax-loss-alerts")
def tax_loss_alerts(
    session: Session = Depends(get_session),
    current_user: User = Depends(get_current_user),
):
    """Flag holdings with unrealised losses — useful for tax-loss harvesting before 30 June."""
    today = date_type.today()
    # Days until 30 June of current FY
    fy_end = date_type(today.year, 6, 30) if today.month < 7 else date_type(today.year + 1, 6, 30)
    days_to_fy_end = (fy_end - today).days

    share_holdings = session.exec(
        select(ShareHolding).where(ShareHolding.user_id == current_user.id)
    ).all()
    crypto_holdings = session.exec(
        select(CryptoHolding).where(CryptoHolding.user_id == current_user.id)
    ).all()

    losses = []
    total_unrealised_loss = 0.0

    for h in share_holdings:
        if h.gain_aud < 0:
            total_unrealised_loss += h.gain_aud
            losses.append({
                "id": h.id,
                "asset_type": "share",
                "ticker": h.ticker,
                "name": h.name or h.ticker,
                "qty": h.qty,
                "avg_cost_aud": round(h.avg_cost_aud, 4),
                "price_aud": round(h.price_aud, 4),
                "cost_basis_aud": round(h.cost_basis_aud, 2),
                "value_aud": round(h.value_aud, 2),
                "gain_aud": round(h.gain_aud, 2),
                "gain_pct": round(h.gain_pct, 1),
            })

    for c in crypto_holdings:
        if c.gain_aud < 0 and c.cost_basis_aud > 0:
            total_unrealised_loss += c.gain_aud
            losses.append({
                "id": c.id,
                "asset_type": "crypto",
                "ticker": c.symbol,
                "name": c.symbol,
                "qty": c.qty,
                "avg_cost_aud": round(c.avg_cost_aud, 4),
                "price_aud": round(c.price_aud, 4),
                "cost_basis_aud": round(c.cost_basis_aud, 2),
                "value_aud": round(c.value_aud, 2),
                "gain_aud": round(c.gain_aud, 2),
                "gain_pct": round(c.gain_pct, 1),
            })

    losses.sort(key=lambda x: x["gain_aud"])  # worst first

    return {
        "fy_end": str(fy_end),
        "days_to_fy_end": days_to_fy_end,
        "total_unrealised_loss": round(total_unrealised_loss, 2),
        "loss_count": len(losses),
        "losses": losses,
    }
