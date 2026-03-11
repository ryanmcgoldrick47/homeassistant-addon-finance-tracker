from __future__ import annotations

import hashlib
import hmac
import time
from datetime import datetime, timezone
from typing import Optional

import httpx
from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel
from sqlmodel import Session, select

from database import CryptoHolding, CryptoTrade, AcquisitionLot, get_session, User
from deps import get_setting, get_current_user

router = APIRouter(prefix="/api/crypto", tags=["crypto"])

BINANCE_BASE = "https://api.binance.com"
# Minimum qty to include (filter dust)
DUST_THRESHOLD = 0.0001


def _binance_sign(secret: str, params: str) -> str:
    return hmac.new(secret.encode(), params.encode(), hashlib.sha256).hexdigest()


async def _fetch_aud_per_usdt(client: httpx.AsyncClient) -> float:
    """Return AUD per 1 USDT. Uses USDTAUD pair on Binance; falls back to Yahoo."""
    try:
        r = await client.get(
            f"{BINANCE_BASE}/api/v3/ticker/price",
            params={"symbol": "USDTAUD"},
            timeout=8,
        )
        if r.status_code == 200:
            return float(r.json()["price"])
    except Exception:
        pass
    # Fall back to Yahoo Finance AUDUSD=X (inverted)
    try:
        r = await client.get(
            "https://query2.finance.yahoo.com/v8/finance/chart/AUDUSD=X",
            params={"interval": "1d", "range": "1d"},
            headers={"User-Agent": "Mozilla/5.0"},
            timeout=8,
        )
        if r.status_code == 200:
            price = r.json()["chart"]["result"][0]["meta"]["regularMarketPrice"]
            return round(1 / price, 6)
    except Exception:
        pass
    # Last resort: rough fallback
    return 1.58


@router.get("")
def list_crypto(
    session: Session = Depends(get_session),
    current_user: User = Depends(get_current_user),
):
    holdings = session.exec(
        select(CryptoHolding).where(
            CryptoHolding.user_id == current_user.id,
        ).order_by(CryptoHolding.value_aud.desc())
    ).all()
    total = round(sum(h.value_aud for h in holdings), 2)
    synced_at = holdings[0].synced_at if holdings else None
    return {
        "holdings": [h.model_dump() for h in holdings],
        "total_aud": total,
        "synced_at": synced_at,
    }


@router.post("/sync")
async def sync_binance(
    session: Session = Depends(get_session),
    current_user: User = Depends(get_current_user),
):
    """Fetch Binance spot balances + AUD prices, replace all binance-sourced holdings."""
    api_key = get_setting(session, "binance_api_key", "")
    api_secret = get_setting(session, "binance_api_secret", "")
    if not api_key or not api_secret:
        raise HTTPException(400, "Binance API key and secret not configured. Add them in Settings.")

    ts = int(time.time() * 1000)
    params_str = f"timestamp={ts}"
    sig = _binance_sign(api_secret, params_str)

    async with httpx.AsyncClient(timeout=15) as client:
        # 1. Fetch account balances
        r = await client.get(
            f"{BINANCE_BASE}/api/v3/account",
            params={"timestamp": ts, "signature": sig},
            headers={"X-MBX-APIKEY": api_key},
        )
        if r.status_code != 200:
            raise HTTPException(502, f"Binance API error: {r.text[:300]}")

        balances = [
            b for b in r.json()["balances"]
            if float(b["free"]) + float(b["locked"]) > DUST_THRESHOLD
        ]
        if not balances:
            return {"ok": True, "synced": 0, "total_aud": 0.0, "holdings": []}

        # 2. Fetch AUD/USDT rate once
        aud_per_usdt = await _fetch_aud_per_usdt(client)

        # 3. Fetch prices for each symbol
        new_holdings: list[CryptoHolding] = []
        now_str = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S")

        # Stable coins treated as 1 USDT each
        stable_coins = {"USDT", "USDC", "BUSD", "TUSD", "FDUSD", "DAI"}

        for b in balances:
            sym = b["asset"]
            qty = float(b["free"]) + float(b["locked"])

            if sym in stable_coins:
                price_aud = aud_per_usdt
            elif sym == "AUD":
                price_aud = 1.0
            else:
                try:
                    pr = await client.get(
                        f"{BINANCE_BASE}/api/v3/ticker/price",
                        params={"symbol": f"{sym}USDT"},
                        timeout=8,
                    )
                    if pr.status_code == 200:
                        price_usdt = float(pr.json()["price"])
                        price_aud = price_usdt * aud_per_usdt
                    else:
                        continue  # skip symbols with no USDT pair
                except Exception:
                    continue

            value_aud = round(qty * price_aud, 2)
            if value_aud < 0.01:
                continue  # skip genuine dust in AUD terms

            new_holdings.append(CryptoHolding(
                symbol=sym,
                qty=round(qty, 8),
                price_aud=round(price_aud, 4),
                value_aud=value_aud,
                synced_at=now_str,
                source="binance",
                user_id=current_user.id,
            ))

    # 4. Replace binance holdings in DB for this user
    old = session.exec(
        select(CryptoHolding).where(
            CryptoHolding.user_id == current_user.id,
            CryptoHolding.source == "binance",
        )
    ).all()
    for h in old:
        session.delete(h)
    for h in new_holdings:
        session.add(h)
    session.commit()

    total_aud = round(sum(h.value_aud for h in new_holdings), 2)
    return {
        "ok": True,
        "synced": len(new_holdings),
        "total_aud": total_aud,
        "holdings": [h.model_dump() for h in new_holdings],
    }


class ManualCryptoBody(BaseModel):
    symbol: str
    qty: float
    price_aud: float


@router.post("/manual")
def add_manual_crypto(
    body: ManualCryptoBody,
    session: Session = Depends(get_session),
    current_user: User = Depends(get_current_user),
):
    """Add or update a manual crypto holding (not from Binance)."""
    # Upsert by symbol for manual entries per user
    existing = session.exec(
        select(CryptoHolding).where(
            CryptoHolding.user_id == current_user.id,
            CryptoHolding.symbol == body.symbol.upper(),
            CryptoHolding.source == "manual",
        )
    ).first()
    now_str = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S")
    if existing:
        existing.qty = body.qty
        existing.price_aud = body.price_aud
        existing.value_aud = round(body.qty * body.price_aud, 2)
        existing.synced_at = now_str
        session.add(existing)
    else:
        session.add(CryptoHolding(
            symbol=body.symbol.upper(),
            qty=body.qty,
            price_aud=body.price_aud,
            value_aud=round(body.qty * body.price_aud, 2),
            synced_at=now_str,
            source="manual",
            user_id=current_user.id,
        ))
    session.commit()
    return {"ok": True}


@router.delete("/{holding_id}")
def delete_crypto(
    holding_id: int,
    session: Session = Depends(get_session),
    current_user: User = Depends(get_current_user),
):
    h = session.get(CryptoHolding, holding_id)
    if not h:
        raise HTTPException(404, "Not found")
    if h.user_id != current_user.id:
        raise HTTPException(403, "Access denied")
    session.delete(h)
    session.commit()
    return {"ok": True}


# ---------------------------------------------------------------------------
# Trade history helpers
# ---------------------------------------------------------------------------

async def _fetch_audusd_history(client: httpx.AsyncClient) -> dict:
    """Return dict of ISO-date -> AUD_per_USD using Yahoo Finance 5y daily data."""
    try:
        r = await client.get(
            "https://query2.finance.yahoo.com/v8/finance/chart/AUDUSD=X",
            params={"interval": "1d", "range": "5y"},
            headers={"User-Agent": "Mozilla/5.0"},
            timeout=15,
        )
        if r.status_code != 200:
            return {}
        result = r.json()["chart"]["result"][0]
        timestamps = result["timestamp"]
        closes = result["indicators"]["quote"][0]["close"]
        out = {}
        for ts, close in zip(timestamps, closes):
            if close and close > 0:
                d = datetime.fromtimestamp(ts, tz=timezone.utc).date().isoformat()
                out[d] = round(1.0 / close, 6)  # AUD per USD
        return out
    except Exception:
        return {}


async def _fetch_my_trades(
    client: httpx.AsyncClient, api_key: str, api_secret: str, pair: str
) -> list | None:
    """Fetch up to 1000 trades for a Binance trading pair. Returns None if pair invalid."""
    ts = int(time.time() * 1000)
    params_str = f"symbol={pair}&timestamp={ts}&limit=1000"
    sig = _binance_sign(api_secret, params_str)
    try:
        r = await client.get(
            f"{BINANCE_BASE}/api/v3/myTrades",
            params={"symbol": pair, "timestamp": ts, "limit": 1000, "signature": sig},
            headers={"X-MBX-APIKEY": api_key},
            timeout=12,
        )
        if r.status_code == 200:
            return r.json()
        # 400 = invalid symbol — skip silently
        return None
    except Exception:
        return None


def _recompute_cost_basis(session: Session, symbol: str, holding: CryptoHolding, user_id: int) -> None:
    """Recompute weighted avg cost for a symbol from stored CryptoTrades and update holding."""
    trades = session.exec(
        select(CryptoTrade)
        .where(
            CryptoTrade.user_id == user_id,
            CryptoTrade.symbol == symbol,
        )
        .order_by(CryptoTrade.trade_time)
    ).all()

    running_qty = 0.0
    running_cost = 0.0
    for t in trades:
        if t.side == "BUY":
            running_cost += t.qty * t.price_aud
            running_qty += t.qty
        elif t.side == "SELL" and running_qty > 0:
            # Proportionally reduce cost pool (average cost method)
            ratio = min(t.qty / running_qty, 1.0)
            running_cost -= running_cost * ratio
            running_qty = max(running_qty - t.qty, 0.0)

    avg_cost = round(running_cost / running_qty, 6) if running_qty > 0.0001 else 0.0
    holding.avg_cost_aud   = avg_cost
    holding.cost_basis_aud = round(holding.qty * avg_cost, 2)
    holding.gain_aud       = round(holding.value_aud - holding.cost_basis_aud, 2)
    holding.gain_pct       = (
        round(holding.gain_aud / holding.cost_basis_aud * 100, 2)
        if holding.cost_basis_aud > 0 else 0.0
    )
    session.add(holding)


# ---------------------------------------------------------------------------
# GET /api/crypto/trades  — list stored trades (optionally filtered by symbol)
# ---------------------------------------------------------------------------

@router.get("/trades")
def list_trades(
    symbol: str = "",
    session: Session = Depends(get_session),
    current_user: User = Depends(get_current_user),
):
    q = select(CryptoTrade).where(CryptoTrade.user_id == current_user.id).order_by(CryptoTrade.trade_time.desc())
    if symbol:
        q = q.where(CryptoTrade.symbol == symbol.upper())
    trades = session.exec(q).all()
    return [t.model_dump() for t in trades]


# ---------------------------------------------------------------------------
# POST /api/crypto/sync-trades  — fetch Binance history, build cost basis
# ---------------------------------------------------------------------------

@router.post("/sync-trades")
async def sync_binance_trades(
    session: Session = Depends(get_session),
    current_user: User = Depends(get_current_user),
):
    """
    Fetch full trade history from Binance for every held symbol.
    - Stores raw trades in CryptoTrade (idempotent by binance_id).
    - Auto-creates AcquisitionLot for each BUY (idempotent by notes ref).
    - Recomputes weighted avg cost basis and updates CryptoHolding.
    """
    api_key    = get_setting(session, "binance_api_key", "")
    api_secret = get_setting(session, "binance_api_secret", "")
    if not api_key or not api_secret:
        raise HTTPException(400, "Binance API key and secret not configured in Settings.")

    holdings = session.exec(
        select(CryptoHolding).where(
            CryptoHolding.user_id == current_user.id,
            CryptoHolding.source == "binance",
        )
    ).all()
    if not holdings:
        return {"ok": True, "message": "No Binance holdings found. Run Sync Binance first."}

    async with httpx.AsyncClient(timeout=20) as client:
        # Fetch historical AUDUSD rates once (5 year daily)
        audusd_history = await _fetch_audusd_history(client)
        current_aud_per_usdt = await _fetch_aud_per_usdt(client)

        raw_trades: list[dict] = []
        for holding in holdings:
            sym = holding.symbol
            # Try USDT pair first, then AUD pair
            for quote in ("USDT", "AUD"):
                pair   = f"{sym}{quote}"
                result = await _fetch_my_trades(client, api_key, api_secret, pair)
                if result is not None:
                    for t in result:
                        trade_dt   = datetime.fromtimestamp(t["time"] / 1000, tz=timezone.utc)
                        date_str   = trade_dt.date().isoformat()
                        price_native = float(t["price"])

                        if quote == "AUD":
                            price_aud = price_native
                        else:
                            rate = audusd_history.get(date_str, current_aud_per_usdt)
                            price_aud = price_native * rate

                        raw_trades.append({
                            "symbol":     sym,
                            "pair":       pair,
                            "binance_id": str(t["id"]),
                            "trade_time": trade_dt.strftime("%Y-%m-%dT%H:%M:%S"),
                            "trade_date": trade_dt.date(),
                            "side":       "BUY" if t["isBuyer"] else "SELL",
                            "qty":        float(t["qty"]),
                            "price_usdt": price_native if quote == "USDT" else 0.0,
                            "price_aud":  round(price_aud, 6),
                            "fee_qty":    float(t["commission"]),
                            "fee_asset":  t["commissionAsset"],
                        })
                    break  # found trades for this symbol — skip remaining quote attempts

    # 1. Upsert trades into CryptoTrade
    trades_stored = 0
    for td in raw_trades:
        if session.exec(
            select(CryptoTrade).where(
                CryptoTrade.user_id == current_user.id,
                CryptoTrade.binance_id == td["binance_id"],
            )
        ).first():
            continue
        session.add(CryptoTrade(
            binance_id  = td["binance_id"],
            symbol      = td["symbol"],
            pair        = td["pair"],
            trade_time  = td["trade_time"],
            side        = td["side"],
            qty         = td["qty"],
            price_usdt  = td["price_usdt"],
            price_aud   = td["price_aud"],
            fee_qty     = td["fee_qty"],
            fee_asset   = td["fee_asset"],
            user_id     = current_user.id,
        ))
        trades_stored += 1
    session.commit()

    # 2. Auto-create AcquisitionLots for BUY trades (idempotent by notes ref)
    lots_created = 0
    for td in raw_trades:
        if td["side"] != "BUY":
            continue
        ref = f"binance:{td['binance_id']}"
        if session.exec(
            select(AcquisitionLot).where(
                AcquisitionLot.user_id == current_user.id,
                AcquisitionLot.notes == ref,
            )
        ).first():
            continue
        session.add(AcquisitionLot(
            ticker            = td["symbol"],
            asset_type        = "crypto",
            acquired_date     = td["trade_date"],
            qty               = td["qty"],
            cost_per_unit_aud = td["price_aud"],
            notes             = ref,
            user_id           = current_user.id,
        ))
        lots_created += 1
    session.commit()

    # 3. Recompute cost basis for each holding
    updated = []
    for holding in holdings:
        _recompute_cost_basis(session, holding.symbol, holding, current_user.id)
        updated.append({
            "symbol":         holding.symbol,
            "avg_cost_aud":   round(holding.avg_cost_aud, 4),
            "cost_basis_aud": holding.cost_basis_aud,
            "gain_aud":       holding.gain_aud,
            "gain_pct":       holding.gain_pct,
        })
    session.commit()

    return {
        "ok":               True,
        "total_trades":     len(raw_trades),
        "trades_stored":    trades_stored,
        "lots_created":     lots_created,
        "holdings_updated": len(updated),
        "updated":          updated,
    }
