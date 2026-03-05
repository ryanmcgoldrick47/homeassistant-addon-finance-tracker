from __future__ import annotations

from datetime import date
from typing import Optional
from fastapi import APIRouter, Depends, HTTPException
from sqlmodel import Session, select
from pydantic import BaseModel

from database import get_session, Dividend, User
from deps import get_current_user

router = APIRouter()


# ---------------------------------------------------------------------------
# Schema
# ---------------------------------------------------------------------------

class DividendIn(BaseModel):
    ticker: str
    pay_date: str                       # ISO date
    ex_date: Optional[str] = None
    amount_aud: float = 0.0
    franking_pct: float = 0.0          # 0–100
    notes: Optional[str] = None


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _franking_credits(amount_aud: float, franking_pct: float) -> float:
    """
    Franking credits = cash_dividend × (franking_pct/100) × (30/70)
    The 30% corporate tax rate is the ATO standard for fully-franked calculation.
    """
    if franking_pct <= 0 or amount_aud <= 0:
        return 0.0
    return round(amount_aud * (franking_pct / 100) * (30 / 70), 4)


# ---------------------------------------------------------------------------
# CRUD
# ---------------------------------------------------------------------------

@router.get("/api/dividends")
def list_dividends(
    ticker: Optional[str] = None,
    session: Session = Depends(get_session),
    current_user: User = Depends(get_current_user),
):
    q = select(Dividend).where(Dividend.user_id == current_user.id)
    if ticker:
        q = q.where(Dividend.ticker == ticker.upper())
    divs = session.exec(q.order_by(Dividend.pay_date.desc())).all()
    return [
        {
            "id": d.id,
            "ticker": d.ticker,
            "pay_date": str(d.pay_date),
            "ex_date": str(d.ex_date) if d.ex_date else None,
            "amount_aud": d.amount_aud,
            "franking_credits_aud": d.franking_credits_aud,
            "franking_pct": d.franking_pct,
            "grossed_up_aud": round(d.amount_aud + d.franking_credits_aud, 2),
            "notes": d.notes,
        }
        for d in divs
    ]


@router.post("/api/dividends")
def add_dividend(
    body: DividendIn,
    session: Session = Depends(get_session),
    current_user: User = Depends(get_current_user),
):
    franking_credits = _franking_credits(body.amount_aud, body.franking_pct)
    div = Dividend(
        ticker=body.ticker.upper(),
        pay_date=date.fromisoformat(body.pay_date),
        ex_date=date.fromisoformat(body.ex_date) if body.ex_date else None,
        amount_aud=body.amount_aud,
        franking_credits_aud=franking_credits,
        franking_pct=body.franking_pct,
        notes=body.notes,
        user_id=current_user.id,
    )
    session.add(div)
    session.commit()
    session.refresh(div)
    return {"ok": True, "id": div.id, "franking_credits_aud": franking_credits}


@router.delete("/api/dividends/{div_id}")
def delete_dividend(
    div_id: int,
    session: Session = Depends(get_session),
    current_user: User = Depends(get_current_user),
):
    d = session.get(Dividend, div_id)
    if not d:
        raise HTTPException(404, "Dividend not found")
    if d.user_id != current_user.id:
        raise HTTPException(403, "Access denied")
    session.delete(d)
    session.commit()
    return {"ok": True}


# ---------------------------------------------------------------------------
# Summary
# ---------------------------------------------------------------------------

@router.get("/api/dividends/summary")
def dividends_summary(
    fy: int = None,
    session: Session = Depends(get_session),
    current_user: User = Depends(get_current_user),
):
    """
    Aggregate dividends for a financial year.
    fy=2025 → FY 2024-25 (1 Jul 2024 – 30 Jun 2025).
    """
    from datetime import date as date_t
    today = date_t.today()
    if not fy:
        fy = today.year if today.month >= 7 else today.year - 1

    fy_start = date_t(fy - 1, 7, 1)
    fy_end   = date_t(fy, 6, 30)

    divs = session.exec(
        select(Dividend).where(
            Dividend.user_id == current_user.id,
            Dividend.pay_date >= fy_start,
            Dividend.pay_date <= fy_end,
        )
    ).all()

    total_cash        = round(sum(d.amount_aud for d in divs), 2)
    total_franking    = round(sum(d.franking_credits_aud for d in divs), 2)
    total_grossed_up  = round(total_cash + total_franking, 2)

    # Per-ticker breakdown
    by_ticker: dict[str, dict] = {}
    for d in divs:
        t = d.ticker
        if t not in by_ticker:
            by_ticker[t] = {"ticker": t, "cash": 0.0, "franking": 0.0, "count": 0}
        by_ticker[t]["cash"]    = round(by_ticker[t]["cash"] + d.amount_aud, 2)
        by_ticker[t]["franking"] = round(by_ticker[t]["franking"] + d.franking_credits_aud, 2)
        by_ticker[t]["count"]   += 1

    for v in by_ticker.values():
        v["grossed_up"] = round(v["cash"] + v["franking"], 2)

    return {
        "fy": f"{fy-1}-{str(fy)[2:]}",
        "fy_start": str(fy_start),
        "fy_end": str(fy_end),
        "total_cash_aud": total_cash,
        "total_franking_credits_aud": total_franking,
        "total_grossed_up_aud": total_grossed_up,
        "dividend_count": len(divs),
        "by_ticker": sorted(by_ticker.values(), key=lambda x: x["cash"], reverse=True),
    }
