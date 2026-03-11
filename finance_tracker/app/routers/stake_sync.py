from __future__ import annotations

"""Stake portfolio auto-sync using stake-python."""

import os
from datetime import datetime, timezone

import httpx
from fastapi import APIRouter, Depends, HTTPException
from sqlmodel import Session, select

from database import ShareHolding, get_session, User
from deps import get_setting, get_current_user

router = APIRouter(prefix="/api/investments", tags=["stake"])


async def _aud_per_usd() -> float:
    """Current AUD/USD rate from Yahoo Finance."""
    try:
        async with httpx.AsyncClient(timeout=8) as client:
            r = await client.get(
                "https://query2.finance.yahoo.com/v8/finance/chart/AUDUSD=X",
                params={"interval": "1d", "range": "1d"},
                headers={"User-Agent": "Mozilla/5.0"},
            )
            price = r.json()["chart"]["result"][0]["meta"]["regularMarketPrice"]
            return round(1.0 / price, 6)
    except Exception:
        return 1.58


def _upsert_holding(session: Session, ticker: str, name: str, qty: float,
                    avg_cost_aud: float, price_aud: float, broker: str,
                    user_id: int) -> ShareHolding:
    h = session.exec(
        select(ShareHolding).where(
            ShareHolding.ticker == ticker,
            ShareHolding.broker == broker,
            ShareHolding.user_id == user_id,
        )
    ).first()
    now = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S")
    cost_basis = round(qty * avg_cost_aud, 2)
    value      = round(qty * price_aud, 2)
    gain       = round(value - cost_basis, 2)
    gain_pct   = round(gain / cost_basis * 100, 2) if cost_basis > 0 else 0.0

    if h:
        h.name             = name
        h.qty              = qty
        h.avg_cost_aud     = avg_cost_aud
        h.price_aud        = price_aud
        h.value_aud        = value
        h.cost_basis_aud   = cost_basis
        h.gain_aud         = gain
        h.gain_pct         = gain_pct
        h.price_fetched_at = now
    else:
        h = ShareHolding(
            ticker=ticker, name=name, qty=qty,
            avg_cost_aud=avg_cost_aud, price_aud=price_aud,
            value_aud=value, cost_basis_aud=cost_basis,
            gain_aud=gain, gain_pct=gain_pct,
            currency="AUD", broker=broker, price_fetched_at=now,
            user_id=user_id,
        )
    session.add(h)
    return h


@router.post("/stake-sync")
async def sync_stake(
    session: Session = Depends(get_session),
    current_user: User = Depends(get_current_user),
):
    """
    Sync Stake portfolio (US + ASX) into ShareHolding via stake-python.
    Requires stake_session_token in Settings (copy from browser DevTools).
    """
    token = get_setting(session, "stake_session_token", "")
    if not token:
        raise HTTPException(400, "Stake session token not configured in Settings.")

    try:
        import stake as stake_lib
        from stake.client import StakeClient, HttpClient
    except ImportError:
        raise HTTPException(500, "stake-python not installed.")

    aud_rate = await _aud_per_usd()
    synced: list[dict] = []
    errors: list[str] = []

    # Stake uses env var for token
    orig_token = os.environ.get("STAKE_TOKEN", "")
    os.environ["STAKE_TOKEN"] = token

    try:
        for exchange, broker, currency in [
            ("nyse",  "stake_us",  "USD"),
            ("asx",   "stake_asx", "AUD"),
        ]:
            try:
                async with StakeClient() as client:
                    client.set_exchange(exchange)
                    positions = await client.equities.list()

                for pos in positions.equity_positions:
                    ticker = pos.symbol
                    if exchange == "asx":
                        # Stake returns ASX tickers without .AX suffix; add it for Yahoo/display
                        ticker = ticker + ".AX" if not ticker.endswith(".AX") else ticker

                    avg_cost_native = pos.average_price
                    price_native    = pos.market_price
                    qty             = pos.open_qty

                    if currency == "USD":
                        avg_cost_aud = round(avg_cost_native * aud_rate, 4)
                        price_aud    = round(price_native    * aud_rate, 4)
                    else:
                        avg_cost_aud = round(avg_cost_native, 4)
                        price_aud    = round(price_native, 4)

                    _upsert_holding(
                        session, ticker, pos.name, qty,
                        avg_cost_aud, price_aud, broker,
                        current_user.id,
                    )
                    synced.append({
                        "ticker": ticker, "name": pos.name,
                        "qty": qty, "price_aud": price_aud,
                        "avg_cost_aud": avg_cost_aud,
                        "exchange": exchange,
                    })

            except Exception as e:
                errors.append(f"{exchange}: {str(e)[:200]}")

        session.commit()

    finally:
        os.environ["STAKE_TOKEN"] = orig_token

    return {
        "ok":     True,
        "synced": len(synced),
        "errors": errors,
        "holdings": synced,
    }


@router.get("/stake-status")
def stake_status(
    session: Session = Depends(get_session),
    current_user: User = Depends(get_current_user),
):
    token = get_setting(session, "stake_session_token", "")
    holdings = session.exec(
        select(ShareHolding).where(
            ShareHolding.broker.in_(["stake_us", "stake_asx"]),
            ShareHolding.user_id == current_user.id,
        )
    ).all()
    return {
        "configured": bool(token),
        "holdings_count": len(holdings),
        "last_synced": max((h.price_fetched_at for h in holdings if h.price_fetched_at), default=None),
    }
