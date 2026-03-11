"""Paper trading router — AI-managed simulated investment portfolio."""
from __future__ import annotations

import asyncio
import json
import os
from datetime import datetime, timezone
from typing import Optional

import httpx
from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel
from sqlmodel import Session, select

from database import (
    PaperPortfolio, PaperHolding, PaperTrade, PaperAnalysis,
    User, get_session,
)
from deps import get_current_user, get_setting, set_setting

router = APIRouter(prefix="/api/paper", tags=["paper_trading"])

YAHOO_BASE = "https://query2.finance.yahoo.com/v8/finance/chart"
YAHOO_HEADERS = {"User-Agent": "Mozilla/5.0"}

ASX_UNIVERSE = [
    "VAS.AX", "NDQ.AX", "VGS.AX", "CBA.AX", "BHP.AX", "CSL.AX",
    "WES.AX", "MQG.AX", "XRO.AX", "FMG.AX", "WDS.AX", "ANZ.AX",
    "RIO.AX", "NAB.AX", "TLS.AX",
]
US_UNIVERSE = [
    "AAPL", "MSFT", "NVDA", "META", "GOOGL", "AMZN", "TSLA",
    "AMD", "PLTR", "SPY", "QQQ", "MSTR", "TSM", "AVGO", "ARM",
]
ALL_UNIVERSE = ASX_UNIVERSE + US_UNIVERSE


# ---------------------------------------------------------------------------
# Yahoo Finance helpers
# ---------------------------------------------------------------------------

async def _fetch_price(client: httpx.AsyncClient, ticker: str) -> tuple[float, str]:
    """Return (price_in_native_currency, currency)."""
    r = await client.get(
        f"{YAHOO_BASE}/{ticker}",
        params={"interval": "1d", "range": "1d"},
        headers=YAHOO_HEADERS,
        timeout=10,
    )
    r.raise_for_status()
    meta = r.json()["chart"]["result"][0]["meta"]
    return float(meta["regularMarketPrice"]), meta.get("currency", "USD").upper()


async def _fetch_audusd(client: httpx.AsyncClient) -> float:
    """Return AUD per 1 USD."""
    try:
        price, _ = await _fetch_price(client, "AUDUSD=X")
        return round(1 / price, 6)
    except Exception:
        return 1.58  # fallback


async def _fetch_universe_data(client: httpx.AsyncClient) -> dict[str, dict]:
    """
    Fetch current price + 1W + 1M return % for each ticker in the universe.
    Returns {ticker: {price, currency, w1_pct, m1_pct}}.
    """
    async def _fetch_one(ticker: str) -> tuple[str, dict | None]:
        try:
            r = await client.get(
                f"{YAHOO_BASE}/{ticker}",
                params={"interval": "1d", "range": "1mo"},
                headers=YAHOO_HEADERS,
                timeout=15,
            )
            r.raise_for_status()
            data = r.json()["chart"]["result"][0]
            meta = data["meta"]
            closes = [c for c in data["indicators"]["quote"][0]["close"] if c is not None]
            price = float(meta["regularMarketPrice"])
            currency = meta.get("currency", "USD").upper()
            m1_pct = round((closes[-1] - closes[0]) / closes[0] * 100, 2) if len(closes) >= 2 else 0.0
            # 1-week ≈ last 5 trading days
            w1_pct = round((closes[-1] - closes[-6]) / closes[-6] * 100, 2) if len(closes) >= 6 else m1_pct
            return ticker, {"price": price, "currency": currency, "w1_pct": w1_pct, "m1_pct": m1_pct}
        except Exception as e:
            return ticker, None

    tasks = [_fetch_one(t) for t in ALL_UNIVERSE]
    results = await asyncio.gather(*tasks)
    return {t: d for t, d in results if d is not None}


async def _fetch_benchmark_pct(client: httpx.AsyncClient) -> dict[str, float | None]:
    """Return 1-week % change for ASX 200 and S&P 500."""
    out = {}
    for ticker, name in [("^AXJO", "asx200"), ("^GSPC", "sp500")]:
        try:
            r = await client.get(
                f"{YAHOO_BASE}/{ticker}",
                params={"interval": "1d", "range": "1mo"},
                headers=YAHOO_HEADERS,
                timeout=10,
            )
            r.raise_for_status()
            data = r.json()["chart"]["result"][0]
            closes = [c for c in data["indicators"]["quote"][0]["close"] if c is not None]
            pct = round((closes[-1] - closes[-6]) / closes[-6] * 100, 2) if len(closes) >= 6 else None
            out[name] = pct
        except Exception:
            out[name] = None
    return out


def _calc_brokerage(ticker: str, qty: float, price_aud: float, aud_per_usd: float) -> float:
    """Calculate IBKR-style brokerage fee in AUD."""
    if ticker.endswith(".AX"):
        trade_value = qty * price_aud
        return round(max(3.50, trade_value * 0.0008), 4)
    else:
        # US stock — convert USD rate to AUD
        fee_usd = max(1.00, qty * 0.005)
        return round(fee_usd * aud_per_usd, 4)


def _price_to_aud(price: float, currency: str, aud_per_usd: float) -> float:
    if currency == "AUD":
        return price
    return round(price * aud_per_usd, 4)


# ---------------------------------------------------------------------------
# Portfolio helpers
# ---------------------------------------------------------------------------

def _get_or_create_portfolio(session: Session, user_id: int,
                              strategy: str = "aggressive",
                              starting_cash: float = 1000.0,
                              markets: str = "asx,us") -> PaperPortfolio:
    p = session.exec(
        select(PaperPortfolio).where(PaperPortfolio.user_id == user_id)
    ).first()
    if not p:
        p = PaperPortfolio(
            user_id=user_id,
            strategy=strategy,
            starting_cash=starting_cash,
            cash_aud=starting_cash,
            markets=markets,
        )
        session.add(p)
        session.commit()
        session.refresh(p)
    return p


def _portfolio_summary(p: PaperPortfolio, holdings: list[PaperHolding]) -> dict:
    holdings_value = sum(h.value_aud for h in holdings)
    total_value = round(p.cash_aud + holdings_value, 2)
    total_gain = round(total_value - p.starting_cash, 2)
    return_pct = round(total_gain / p.starting_cash * 100, 2) if p.starting_cash > 0 else 0.0
    return {
        "id": p.id,
        "name": p.name,
        "strategy": p.strategy,
        "starting_cash": p.starting_cash,
        "cash_aud": round(p.cash_aud, 2),
        "holdings_value_aud": round(holdings_value, 2),
        "total_value_aud": total_value,
        "total_gain_aud": total_gain,
        "return_pct": return_pct,
        "markets": p.markets,
        "created_at": p.created_at,
        "updated_at": p.updated_at,
    }


# ---------------------------------------------------------------------------
# Endpoints
# ---------------------------------------------------------------------------

@router.get("/portfolio")
async def get_portfolio(
    session: Session = Depends(get_session),
    current_user: User = Depends(get_current_user),
):
    """Portfolio summary with live benchmark comparison."""
    p = session.exec(
        select(PaperPortfolio).where(PaperPortfolio.user_id == current_user.id)
    ).first()
    if not p:
        return {"exists": False}

    holdings = session.exec(
        select(PaperHolding).where(PaperHolding.portfolio_id == p.id)
    ).all()

    summary = _portfolio_summary(p, holdings)
    summary["exists"] = True

    # Benchmark comparison (best-effort — don't fail if Yahoo is down)
    try:
        async with httpx.AsyncClient() as client:
            bm = await _fetch_benchmark_pct(client)
        summary["asx200_w1_pct"] = bm.get("asx200")
        summary["sp500_w1_pct"] = bm.get("sp500")
    except Exception:
        summary["asx200_w1_pct"] = None
        summary["sp500_w1_pct"] = None

    return summary


class PortfolioCreate(BaseModel):
    strategy: str = "aggressive"
    starting_cash: float = 1000.0
    markets: str = "asx,us"
    reset: bool = False


@router.post("/portfolio")
def create_or_reset_portfolio(
    body: PortfolioCreate,
    session: Session = Depends(get_session),
    current_user: User = Depends(get_current_user),
):
    """Create a new portfolio or reset the existing one."""
    existing = session.exec(
        select(PaperPortfolio).where(PaperPortfolio.user_id == current_user.id)
    ).first()

    if existing and body.reset:
        # Delete all associated data
        for h in session.exec(select(PaperHolding).where(PaperHolding.portfolio_id == existing.id)).all():
            session.delete(h)
        for t in session.exec(select(PaperTrade).where(PaperTrade.portfolio_id == existing.id)).all():
            session.delete(t)
        for a in session.exec(select(PaperAnalysis).where(PaperAnalysis.portfolio_id == existing.id)).all():
            session.delete(a)
        session.delete(existing)
        session.commit()
        existing = None

    if existing and not body.reset:
        # Update strategy/markets without resetting capital
        existing.strategy = body.strategy
        existing.markets = body.markets
        existing.updated_at = datetime.now().isoformat(timespec="seconds")
        session.add(existing)
        session.commit()
        session.refresh(existing)
        return {"ok": True, "portfolio_id": existing.id, "reset": False}

    p = PaperPortfolio(
        user_id=current_user.id,
        strategy=body.strategy,
        starting_cash=body.starting_cash,
        cash_aud=body.starting_cash,
        markets=body.markets,
    )
    session.add(p)
    session.commit()
    session.refresh(p)
    return {"ok": True, "portfolio_id": p.id, "reset": body.reset}


@router.get("/holdings")
async def get_holdings(
    session: Session = Depends(get_session),
    current_user: User = Depends(get_current_user),
):
    """Holdings with refreshed live prices."""
    p = session.exec(
        select(PaperPortfolio).where(PaperPortfolio.user_id == current_user.id)
    ).first()
    if not p:
        return []

    holdings = session.exec(
        select(PaperHolding).where(PaperHolding.portfolio_id == p.id)
    ).all()
    if not holdings:
        return []

    # Refresh prices
    try:
        async with httpx.AsyncClient() as client:
            aud_per_usd = await _fetch_audusd(client)
            for h in holdings:
                try:
                    price, currency = await _fetch_price(client, h.ticker)
                    h.current_price_aud = _price_to_aud(price, currency, aud_per_usd)
                    h.value_aud = round(h.qty * h.current_price_aud, 2)
                    cost_basis = round(h.qty * h.avg_cost_aud, 2)
                    h.gain_aud = round(h.value_aud - cost_basis, 2)
                    h.gain_pct = round(h.gain_aud / cost_basis * 100, 2) if cost_basis > 0 else 0.0
                    session.add(h)
                except Exception:
                    pass
        session.commit()
    except Exception:
        pass

    # Re-query to get fresh data after commit
    holdings = session.exec(
        select(PaperHolding).where(PaperHolding.portfolio_id == p.id)
    ).all()
    return [h.model_dump() for h in holdings]


@router.get("/trades")
def get_trades(
    limit: int = 50,
    offset: int = 0,
    session: Session = Depends(get_session),
    current_user: User = Depends(get_current_user),
):
    """Trade history — newest first."""
    p = session.exec(
        select(PaperPortfolio).where(PaperPortfolio.user_id == current_user.id)
    ).first()
    if not p:
        return {"items": [], "total": 0}

    trades = session.exec(
        select(PaperTrade)
        .where(PaperTrade.portfolio_id == p.id)
        .order_by(PaperTrade.executed_at.desc())
        .offset(offset)
        .limit(limit)
    ).all()
    return {"items": [t.model_dump() for t in trades]}


@router.get("/analysis")
def get_analysis_list(
    limit: int = 10,
    session: Session = Depends(get_session),
    current_user: User = Depends(get_current_user),
):
    """List of past AI analyses (newest first)."""
    p = session.exec(
        select(PaperPortfolio).where(PaperPortfolio.user_id == current_user.id)
    ).first()
    if not p:
        return []

    analyses = session.exec(
        select(PaperAnalysis)
        .where(PaperAnalysis.portfolio_id == p.id)
        .order_by(PaperAnalysis.created_at.desc())
        .limit(limit)
    ).all()

    result = []
    for a in analyses:
        trade_ids = json.loads(a.trades_json) if a.trades_json else []
        result.append({
            "id": a.id,
            "created_at": a.created_at,
            "trades_count": len(trade_ids),
            "analysis_preview": a.analysis_text[:300] if a.analysis_text else "",
        })
    return result


@router.get("/analysis/{analysis_id}")
def get_analysis_detail(
    analysis_id: int,
    session: Session = Depends(get_session),
    current_user: User = Depends(get_current_user),
):
    """Full analysis text + trades executed."""
    a = session.get(PaperAnalysis, analysis_id)
    if not a or a.user_id != current_user.id:
        raise HTTPException(404, "Analysis not found")

    trade_ids = json.loads(a.trades_json) if a.trades_json else []
    trades = []
    for tid in trade_ids:
        t = session.get(PaperTrade, tid)
        if t:
            trades.append(t.model_dump())

    return {
        "id": a.id,
        "created_at": a.created_at,
        "analysis_text": a.analysis_text,
        "trades": trades,
    }


@router.get("/performance")
def get_performance(
    session: Session = Depends(get_session),
    current_user: User = Depends(get_current_user),
):
    """Portfolio value snapshots for charting (one data point per analysis run)."""
    p = session.exec(
        select(PaperPortfolio).where(PaperPortfolio.user_id == current_user.id)
    ).first()
    if not p:
        return []

    analyses = session.exec(
        select(PaperAnalysis)
        .where(PaperAnalysis.portfolio_id == p.id)
        .order_by(PaperAnalysis.created_at.asc())
    ).all()

    # Start with initial capital
    result = [{"date": p.created_at[:10], "value": p.starting_cash}]

    # Walk trade history to reconstruct portfolio value at each analysis
    trades = session.exec(
        select(PaperTrade)
        .where(PaperTrade.portfolio_id == p.id)
        .order_by(PaperTrade.executed_at.asc())
    ).all()

    # Build cumulative trade map: date → cumulative cash deducted
    for a in analyses:
        trade_ids = set(json.loads(a.trades_json) if a.trades_json else [])
        # Sum of buy costs minus sell proceeds for trades in this analysis
        analysis_trades = [t for t in trades if t.id in trade_ids]
        cash_change = sum(t.total_aud for t in analysis_trades)
        # Get holdings value snapshot: use current portfolio value at that point
        # (simple approximation: starting_cash minus cumulative cash deployed plus current holdings)
        result.append({
            "date": a.created_at[:10],
            "label": a.created_at[:10],
        })

    # Final point: current value
    holdings = session.exec(
        select(PaperHolding).where(PaperHolding.portfolio_id == p.id)
    ).all()
    holdings_value = sum(h.value_aud for h in holdings)
    current_total = round(p.cash_aud + holdings_value, 2)

    # Return simplified points: start + one per analysis + current
    points = [{"date": p.created_at[:10], "value": p.starting_cash}]
    for a in analyses:
        points.append({
            "date": a.created_at[:10],
            "value": current_total,  # best approximation without snapshots
        })
    if analyses:
        points[-1]["value"] = current_total
    else:
        points.append({"date": datetime.now().strftime("%Y-%m-%d"), "value": current_total})

    return points


# ---------------------------------------------------------------------------
# Core: AI Analysis + Trade Execution
# ---------------------------------------------------------------------------

async def _run_analysis(session: Session, user_id: int) -> dict:
    """Core analysis logic — callable from endpoint or background task."""
    p = _get_or_create_portfolio(session, user_id)

    async with httpx.AsyncClient() as client:
        aud_per_usd = await _fetch_audusd(client)

        # Refresh current holdings prices
        holdings = session.exec(
            select(PaperHolding).where(PaperHolding.portfolio_id == p.id)
        ).all()
        for h in holdings:
            try:
                price, currency = await _fetch_price(client, h.ticker)
                h.current_price_aud = _price_to_aud(price, currency, aud_per_usd)
                h.value_aud = round(h.qty * h.current_price_aud, 2)
                cost_basis = round(h.qty * h.avg_cost_aud, 2)
                h.gain_aud = round(h.value_aud - cost_basis, 2)
                h.gain_pct = round(h.gain_aud / cost_basis * 100, 2) if cost_basis > 0 else 0.0
                session.add(h)
            except Exception:
                pass
        session.commit()

        # Fetch universe data and benchmarks in parallel
        universe_task = asyncio.create_task(_fetch_universe_data(client))
        benchmark_task = asyncio.create_task(_fetch_benchmark_pct(client))
        universe_data, benchmarks = await asyncio.gather(universe_task, benchmark_task)

    holdings_value = sum(h.value_aud for h in holdings)
    total_value = round(p.cash_aud + holdings_value, 2)
    total_gain = round(total_value - p.starting_cash, 2)
    return_pct = round(total_gain / p.starting_cash * 100, 2) if p.starting_cash > 0 else 0.0

    # Build current holdings summary
    holdings_summary = [
        {
            "ticker": h.ticker,
            "qty": h.qty,
            "avg_cost_aud": round(h.avg_cost_aud, 4),
            "current_price_aud": round(h.current_price_aud, 4),
            "value_aud": round(h.value_aud, 2),
            "gain_pct": round(h.gain_pct, 2),
        }
        for h in holdings
    ]

    # Build universe summary
    universe_lines = []
    for ticker in ALL_UNIVERSE:
        d = universe_data.get(ticker)
        if d:
            price_aud = _price_to_aud(d["price"], d["currency"], aud_per_usd)
            universe_lines.append(
                f"  {ticker}: price AUD {price_aud:.4f}, 1W {d['w1_pct']:+.1f}%, 1M {d['m1_pct']:+.1f}%"
            )

    prompt = f"""You are an aggressive-growth AI investment manager running a simulated ASX + US portfolio.
Portfolio state:
- Starting capital: AUD {p.starting_cash:.2f}
- Available cash: AUD {p.cash_aud:.2f}
- Holdings value: AUD {holdings_value:.2f}
- Total value: AUD {total_value:.2f}
- Return since inception: {return_pct:+.2f}%

Current holdings:
{json.dumps(holdings_summary, indent=2)}

Market context (last 1 week):
- ASX 200: {benchmarks.get("asx200", "N/A")}%
- S&P 500: {benchmarks.get("sp500", "N/A")}%
- AUD/USD: {round(1/aud_per_usd, 4) if aud_per_usd else "N/A"}

Stock universe (price in AUD, 1W and 1M % changes):
{chr(10).join(universe_lines) if universe_lines else "  No data available"}

Strategy guidelines:
- Aggressive growth: maximise returns, accept higher risk
- Portfolio cap: max 5 positions at once
- Minimum trade size: AUD 50
- Brokerage: ASX max(AUD 3.50, 0.08% of trade), US max(AUD {round(1.00 * aud_per_usd, 2)}, USD 0.005/share)
- Don't hold more than 40% of portfolio in any single stock
- Consider selling underperformers to free cash for better opportunities
- You may hold cash if no compelling opportunities exist

Analyse the market data and decide what trades (if any) to make this week.
Return a single JSON object with exactly this format:
{{
  "analysis": "<markdown analysis: 2-4 paragraphs covering market conditions, portfolio performance, and trading rationale>",
  "trades": [
    {{"ticker": "AAPL", "side": "BUY", "qty": 2, "rationale": "Strong momentum, 3W high"}}
  ]
}}

Important:
- "qty" must be a positive number (whole units for stocks, can be fractional for ETFs)
- Only include trades you want to actually execute
- An empty trades array is valid if no action is warranted
- Return ONLY the JSON object, no other text"""

    # Call Claude API
    api_key = get_setting(session, "anthropic_api_key", "") or os.environ.get("ANTHROPIC_API_KEY", "")
    if not api_key:
        raise HTTPException(400, "Anthropic API key not configured. Add it in Settings.")

    model = "claude-sonnet-4-6"
    # Try Sonnet, fall back to Haiku if quota issue
    try:
        import anthropic as _anthropic
        client_ai = _anthropic.Anthropic(api_key=api_key)
        def _call_ai():
            return client_ai.messages.create(
                model=model,
                max_tokens=2048,
                messages=[{"role": "user", "content": prompt}],
            ).content[0].text
        raw = await asyncio.to_thread(_call_ai)
    except Exception as e:
        err_str = str(e).lower()
        if "overloaded" in err_str or "rate" in err_str:
            model = "claude-haiku-4-5-20251001"
            def _call_haiku():
                return client_ai.messages.create(
                    model=model,
                    max_tokens=2048,
                    messages=[{"role": "user", "content": prompt}],
                ).content[0].text
            raw = await asyncio.to_thread(_call_haiku)
        else:
            raise HTTPException(500, f"AI call failed: {str(e)[:200]}")

    # Parse JSON response
    try:
        raw = raw.strip()
        start = raw.find("{")
        end = raw.rfind("}") + 1
        if start < 0 or end <= 0:
            raise ValueError("No JSON object found")
        parsed = json.loads(raw[start:end])
    except Exception as e:
        raise HTTPException(500, f"Failed to parse AI response: {str(e)[:200]}")

    analysis_text = parsed.get("analysis", "")
    trade_requests = parsed.get("trades", [])

    # Re-fetch prices for trade execution
    async with httpx.AsyncClient() as client2:
        aud_per_usd2 = await _fetch_audusd(client2)
        executed_trade_ids = []
        errors = []

        for tr in trade_requests:
            ticker = str(tr.get("ticker", "")).upper().strip()
            side = str(tr.get("side", "")).upper()
            try:
                qty = float(tr.get("qty", 0))
            except (TypeError, ValueError):
                continue
            rationale = str(tr.get("rationale", ""))[:500]

            if not ticker or side not in ("BUY", "SELL") or qty <= 0:
                continue

            # Fetch live price
            try:
                price_native, currency = await _fetch_price(client2, ticker)
            except Exception as e:
                errors.append(f"{ticker}: price fetch failed — {str(e)[:80]}")
                continue

            price_aud = _price_to_aud(price_native, currency, aud_per_usd2)
            brokerage = _calc_brokerage(ticker, qty, price_aud, aud_per_usd2)
            trade_value = round(qty * price_aud, 4)

            if side == "BUY":
                total_cost = round(trade_value + brokerage, 4)
                if total_cost > p.cash_aud:
                    errors.append(f"{ticker}: insufficient cash (need AUD {total_cost:.2f}, have AUD {p.cash_aud:.2f})")
                    continue
                # Update or create holding
                h = session.exec(
                    select(PaperHolding).where(
                        PaperHolding.portfolio_id == p.id,
                        PaperHolding.ticker == ticker,
                    )
                ).first()
                if h:
                    new_qty = h.qty + qty
                    new_avg = round((h.qty * h.avg_cost_aud + qty * price_aud) / new_qty, 4)
                    h.qty = new_qty
                    h.avg_cost_aud = new_avg
                    h.current_price_aud = price_aud
                    h.value_aud = round(new_qty * price_aud, 2)
                    cost_basis = round(new_qty * new_avg, 2)
                    h.gain_aud = round(h.value_aud - cost_basis, 2)
                    h.gain_pct = round(h.gain_aud / cost_basis * 100, 2) if cost_basis > 0 else 0.0
                    session.add(h)
                else:
                    h = PaperHolding(
                        portfolio_id=p.id,
                        ticker=ticker,
                        qty=qty,
                        avg_cost_aud=price_aud,
                        current_price_aud=price_aud,
                        value_aud=round(qty * price_aud, 2),
                        currency=currency,
                        user_id=user_id,
                    )
                    session.add(h)
                p.cash_aud = round(p.cash_aud - total_cost, 4)
                total_aud = total_cost

            else:  # SELL
                h = session.exec(
                    select(PaperHolding).where(
                        PaperHolding.portfolio_id == p.id,
                        PaperHolding.ticker == ticker,
                    )
                ).first()
                if not h or h.qty < qty:
                    errors.append(f"{ticker}: cannot sell {qty} — only hold {h.qty if h else 0}")
                    continue
                proceeds = round(qty * price_aud - brokerage, 4)
                h.qty = round(h.qty - qty, 6)
                if h.qty <= 0.0001:
                    session.delete(h)
                else:
                    h.value_aud = round(h.qty * price_aud, 2)
                    cost_basis = round(h.qty * h.avg_cost_aud, 2)
                    h.gain_aud = round(h.value_aud - cost_basis, 2)
                    h.gain_pct = round(h.gain_aud / cost_basis * 100, 2) if cost_basis > 0 else 0.0
                    session.add(h)
                p.cash_aud = round(p.cash_aud + proceeds, 4)
                total_aud = -proceeds  # stored as positive deduction convention

            trade = PaperTrade(
                portfolio_id=p.id,
                ticker=ticker,
                side=side,
                qty=qty,
                price_aud=price_aud,
                brokerage_aud=brokerage,
                total_aud=total_aud,
                reason=rationale,
                user_id=user_id,
            )
            session.add(trade)
            session.commit()
            session.refresh(trade)
            executed_trade_ids.append(trade.id)

    # Update portfolio timestamp
    p.updated_at = datetime.now().isoformat(timespec="seconds")
    session.add(p)

    # Save analysis
    analysis = PaperAnalysis(
        portfolio_id=p.id,
        analysis_text=analysis_text,
        trades_json=json.dumps(executed_trade_ids),
        user_id=user_id,
    )
    session.add(analysis)
    session.commit()
    session.refresh(analysis)

    # Record last analysis time in settings
    set_setting(session, "paper_last_analysis", datetime.now().isoformat(timespec="seconds"))

    # Refresh holdings for response
    holdings = session.exec(
        select(PaperHolding).where(PaperHolding.portfolio_id == p.id)
    ).all()
    holdings_value = sum(h.value_aud for h in holdings)
    new_total = round(p.cash_aud + holdings_value, 2)
    new_return = round((new_total - p.starting_cash) / p.starting_cash * 100, 2) if p.starting_cash > 0 else 0.0

    return {
        "ok": True,
        "analysis_id": analysis.id,
        "analysis_text": analysis_text,
        "trades_executed": len(executed_trade_ids),
        "errors": errors,
        "portfolio": {
            "cash_aud": round(p.cash_aud, 2),
            "total_value_aud": new_total,
            "return_pct": new_return,
        },
    }


@router.post("/analyse")
async def run_analysis(
    session: Session = Depends(get_session),
    current_user: User = Depends(get_current_user),
):
    """Run AI analysis and execute simulated trades."""
    result = await _run_analysis(session, current_user.id)

    # Send HA notification
    try:
        from routers.notify import _send_notification, _get_notify_config
        ha_url, ha_tok, targets = _get_notify_config(session)
        if ha_tok and targets:
            n_trades = result["trades_executed"]
            ret = result["portfolio"]["return_pct"]
            sign = "+" if ret >= 0 else ""
            msg = (
                f"AI Trader ran analysis — {n_trades} trade{'s' if n_trades != 1 else ''} executed. "
                f"Portfolio return: {sign}{ret:.2f}%"
            )
            asyncio.create_task(_send_notification(
                ha_url, ha_tok, targets,
                "Finance Tracker — AI Trader",
                msg,
            ))
    except Exception:
        pass

    return result


class ManualTradeBody(BaseModel):
    ticker: str
    side: str   # BUY | SELL
    qty: float
    reason: Optional[str] = None


@router.post("/manual-trade")
async def manual_trade(
    body: ManualTradeBody,
    session: Session = Depends(get_session),
    current_user: User = Depends(get_current_user),
):
    """Execute a manual trade (for testing or overrides)."""
    p = _get_or_create_portfolio(session, current_user.id)
    ticker = body.ticker.upper().strip()
    side = body.side.upper()
    if side not in ("BUY", "SELL"):
        raise HTTPException(400, "side must be BUY or SELL")
    if body.qty <= 0:
        raise HTTPException(400, "qty must be positive")

    async with httpx.AsyncClient() as client:
        aud_per_usd = await _fetch_audusd(client)
        try:
            price_native, currency = await _fetch_price(client, ticker)
        except Exception as e:
            raise HTTPException(400, f"Could not fetch price for {ticker}: {e}")

    price_aud = _price_to_aud(price_native, currency, aud_per_usd)
    brokerage = _calc_brokerage(ticker, body.qty, price_aud, aud_per_usd)

    if side == "BUY":
        total_cost = round(body.qty * price_aud + brokerage, 4)
        if total_cost > p.cash_aud:
            raise HTTPException(400, f"Insufficient cash: need AUD {total_cost:.2f}, have AUD {p.cash_aud:.2f}")
        h = session.exec(
            select(PaperHolding).where(PaperHolding.portfolio_id == p.id, PaperHolding.ticker == ticker)
        ).first()
        if h:
            new_qty = h.qty + body.qty
            h.avg_cost_aud = round((h.qty * h.avg_cost_aud + body.qty * price_aud) / new_qty, 4)
            h.qty = new_qty
            h.current_price_aud = price_aud
            h.value_aud = round(new_qty * price_aud, 2)
            session.add(h)
        else:
            session.add(PaperHolding(
                portfolio_id=p.id, ticker=ticker, qty=body.qty,
                avg_cost_aud=price_aud, current_price_aud=price_aud,
                value_aud=round(body.qty * price_aud, 2), currency=currency,
                user_id=current_user.id,
            ))
        p.cash_aud = round(p.cash_aud - total_cost, 4)
        total_aud = total_cost
    else:
        h = session.exec(
            select(PaperHolding).where(PaperHolding.portfolio_id == p.id, PaperHolding.ticker == ticker)
        ).first()
        if not h or h.qty < body.qty:
            raise HTTPException(400, f"Cannot sell {body.qty}: only hold {h.qty if h else 0}")
        proceeds = round(body.qty * price_aud - brokerage, 4)
        h.qty = round(h.qty - body.qty, 6)
        if h.qty <= 0.0001:
            session.delete(h)
        else:
            h.value_aud = round(h.qty * price_aud, 2)
            session.add(h)
        p.cash_aud = round(p.cash_aud + proceeds, 4)
        total_aud = -proceeds

    trade = PaperTrade(
        portfolio_id=p.id, ticker=ticker, side=side,
        qty=body.qty, price_aud=price_aud, brokerage_aud=brokerage,
        total_aud=total_aud, reason=body.reason, user_id=current_user.id,
    )
    session.add(trade)
    p.updated_at = datetime.now().isoformat(timespec="seconds")
    session.add(p)
    session.commit()
    session.refresh(trade)

    return {"ok": True, "trade": trade.model_dump()}
