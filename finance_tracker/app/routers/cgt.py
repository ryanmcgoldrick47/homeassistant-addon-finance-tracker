from __future__ import annotations

from datetime import date, datetime
from typing import List, Optional
from fastapi import APIRouter, Depends, HTTPException
from sqlmodel import Session, select
from pydantic import BaseModel

from database import get_session, AcquisitionLot, Disposal, User
from deps import get_current_user

router = APIRouter()


# ---------------------------------------------------------------------------
# Schemas
# ---------------------------------------------------------------------------

class LotIn(BaseModel):
    ticker: str
    asset_type: str = "share"          # "share" | "crypto"
    acquired_date: str                  # ISO date string
    qty: float
    cost_per_unit_aud: float
    brokerage_aud: float = 0.0
    notes: Optional[str] = None


class DisposeIn(BaseModel):
    lot_id: int
    disposed_date: str                  # ISO date string
    qty: float
    proceeds_per_unit_aud: float
    brokerage_aud: float = 0.0
    notes: Optional[str] = None


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _compute_disposal(
    lot: AcquisitionLot,
    disposed_date: date,
    qty: float,
    proceeds_per_unit_aud: float,
    brokerage_aud: float,
) -> tuple[float, bool]:
    """Return (gain_aud, discount_eligible)."""
    # Proportional brokerage on the lot for the units sold
    if lot.qty > 0:
        proportion = qty / lot.qty
    else:
        proportion = 0.0
    purchase_brokerage = lot.brokerage_aud * proportion

    proceeds = proceeds_per_unit_aud * qty
    cost     = lot.cost_per_unit_aud * qty + purchase_brokerage + brokerage_aud
    gain_aud = round(proceeds - cost, 2)

    held_days = (disposed_date - lot.acquired_date).days
    discount_eligible = held_days >= 365

    return gain_aud, discount_eligible


# ---------------------------------------------------------------------------
# Lot endpoints
# ---------------------------------------------------------------------------

@router.get("/api/cgt/lots")
def list_lots(
    ticker: Optional[str] = None,
    session: Session = Depends(get_session),
    current_user: User = Depends(get_current_user),
):
    q = select(AcquisitionLot).where(AcquisitionLot.user_id == current_user.id)
    if ticker:
        q = q.where(AcquisitionLot.ticker == ticker.upper())
    lots = session.exec(q.order_by(AcquisitionLot.acquired_date.desc())).all()
    # Attach disposals summary per lot
    result = []
    for lot in lots:
        disposals = session.exec(
            select(Disposal).where(Disposal.lot_id == lot.id)
        ).all()
        disposed_qty = sum(d.qty for d in disposals)
        result.append({
            "id": lot.id,
            "ticker": lot.ticker,
            "asset_type": lot.asset_type,
            "acquired_date": str(lot.acquired_date),
            "qty": lot.qty,
            "disposed_qty": round(disposed_qty, 8),
            "remaining_qty": round(lot.qty - disposed_qty, 8),
            "cost_per_unit_aud": lot.cost_per_unit_aud,
            "brokerage_aud": lot.brokerage_aud,
            "notes": lot.notes,
        })
    return result


@router.post("/api/cgt/lots")
def add_lot(
    body: LotIn,
    session: Session = Depends(get_session),
    current_user: User = Depends(get_current_user),
):
    lot = AcquisitionLot(
        ticker=body.ticker.upper(),
        asset_type=body.asset_type,
        acquired_date=date.fromisoformat(body.acquired_date),
        qty=body.qty,
        cost_per_unit_aud=body.cost_per_unit_aud,
        brokerage_aud=body.brokerage_aud,
        notes=body.notes,
        user_id=current_user.id,
    )
    session.add(lot)
    session.commit()
    session.refresh(lot)
    return {"ok": True, "id": lot.id}


@router.delete("/api/cgt/lots/{lot_id}")
def delete_lot(
    lot_id: int,
    session: Session = Depends(get_session),
    current_user: User = Depends(get_current_user),
):
    lot = session.get(AcquisitionLot, lot_id)
    if not lot:
        raise HTTPException(404, "Lot not found")
    if lot.user_id != current_user.id:
        raise HTTPException(403, "Access denied")
    # Block deletion if disposals exist
    disposals = session.exec(select(Disposal).where(Disposal.lot_id == lot_id)).all()
    if disposals:
        raise HTTPException(400, "Cannot delete lot with recorded disposals. Delete disposals first.")
    session.delete(lot)
    session.commit()
    return {"ok": True}


# ---------------------------------------------------------------------------
# Disposal endpoints
# ---------------------------------------------------------------------------

@router.get("/api/cgt/disposals")
def list_disposals(
    ticker: Optional[str] = None,
    session: Session = Depends(get_session),
    current_user: User = Depends(get_current_user),
):
    q = select(Disposal).where(Disposal.user_id == current_user.id)
    if ticker:
        q = q.where(Disposal.ticker == ticker.upper())
    disposals = session.exec(q.order_by(Disposal.disposed_date.desc())).all()
    return [
        {
            "id": d.id,
            "lot_id": d.lot_id,
            "ticker": d.ticker,
            "asset_type": d.asset_type,
            "disposed_date": str(d.disposed_date),
            "qty": d.qty,
            "proceeds_per_unit_aud": d.proceeds_per_unit_aud,
            "brokerage_aud": d.brokerage_aud,
            "gain_aud": d.gain_aud,
            "discount_eligible": d.discount_eligible,
            "notes": d.notes,
        }
        for d in disposals
    ]


@router.post("/api/cgt/dispose")
def record_disposal(
    body: DisposeIn,
    session: Session = Depends(get_session),
    current_user: User = Depends(get_current_user),
):
    lot = session.get(AcquisitionLot, body.lot_id)
    if not lot:
        raise HTTPException(404, "Acquisition lot not found")
    if lot.user_id != current_user.id:
        raise HTTPException(403, "Access denied")

    disposed_date = date.fromisoformat(body.disposed_date)

    # Check remaining qty
    existing = session.exec(select(Disposal).where(Disposal.lot_id == body.lot_id)).all()
    already_disposed = sum(d.qty for d in existing)
    if body.qty > round(lot.qty - already_disposed, 8) + 1e-9:
        raise HTTPException(400, f"Cannot dispose {body.qty} units — only {round(lot.qty - already_disposed, 8)} remaining in lot")

    gain_aud, discount_eligible = _compute_disposal(
        lot, disposed_date, body.qty, body.proceeds_per_unit_aud, body.brokerage_aud
    )

    disposal = Disposal(
        lot_id=body.lot_id,
        ticker=lot.ticker,
        asset_type=lot.asset_type,
        disposed_date=disposed_date,
        qty=body.qty,
        proceeds_per_unit_aud=body.proceeds_per_unit_aud,
        brokerage_aud=body.brokerage_aud,
        gain_aud=gain_aud,
        discount_eligible=discount_eligible,
        notes=body.notes,
        user_id=current_user.id,
    )
    session.add(disposal)
    session.commit()
    session.refresh(disposal)
    return {
        "ok": True,
        "id": disposal.id,
        "gain_aud": gain_aud,
        "discount_eligible": discount_eligible,
    }


@router.delete("/api/cgt/disposals/{disposal_id}")
def delete_disposal(
    disposal_id: int,
    session: Session = Depends(get_session),
    current_user: User = Depends(get_current_user),
):
    d = session.get(Disposal, disposal_id)
    if not d:
        raise HTTPException(404, "Disposal not found")
    if d.user_id != current_user.id:
        raise HTTPException(403, "Access denied")
    session.delete(d)
    session.commit()
    return {"ok": True}


# ---------------------------------------------------------------------------
# CGT Summary
# ---------------------------------------------------------------------------

@router.get("/api/cgt/summary")
def cgt_summary(
    fy: int = None,
    session: Session = Depends(get_session),
    current_user: User = Depends(get_current_user),
):
    """
    Summarise capital gains for a financial year.
    fy=2025 means FY 2024-25 (1 Jul 2024 – 30 Jun 2025).
    Defaults to current FY.
    """
    today = date.today()
    if not fy:
        fy = today.year if today.month >= 7 else today.year - 1

    fy_start = date(fy - 1, 7, 1)
    fy_end   = date(fy, 6, 30)

    disposals = session.exec(
        select(Disposal).where(
            Disposal.user_id == current_user.id,
            Disposal.disposed_date >= fy_start,
            Disposal.disposed_date <= fy_end,
        )
    ).all()

    short_term_gains  = 0.0   # held ≤ 12 months, gain > 0
    long_term_gains   = 0.0   # held > 12 months, gain > 0 (before discount)
    capital_losses    = 0.0   # negative gains (absolute value)

    share_gains  = 0.0
    crypto_gains = 0.0

    rows = []
    for d in disposals:
        if d.gain_aud >= 0:
            if d.discount_eligible:
                long_term_gains += d.gain_aud
            else:
                short_term_gains += d.gain_aud
        else:
            capital_losses += abs(d.gain_aud)

        if d.asset_type == "crypto":
            crypto_gains += d.gain_aud
        else:
            share_gains += d.gain_aud

        rows.append({
            "id": d.id,
            "ticker": d.ticker,
            "asset_type": d.asset_type,
            "disposed_date": str(d.disposed_date),
            "qty": d.qty,
            "gain_aud": d.gain_aud,
            "discount_eligible": d.discount_eligible,
        })

    # CGT discount: 50% of long-term gains (individuals, not SMSF)
    discount_amount  = round(long_term_gains * 0.5, 2)
    # Net gains after discount, before applying losses
    gross_after_discount = round(short_term_gains + long_term_gains - discount_amount, 2)
    # Apply capital losses
    net_capital_gain = round(max(0.0, gross_after_discount - capital_losses), 2)
    # Any unused losses carry forward
    carried_forward_loss = round(max(0.0, capital_losses - gross_after_discount), 2)

    return {
        "fy": f"{fy-1}-{str(fy)[2:]}",
        "fy_start": str(fy_start),
        "fy_end": str(fy_end),
        "short_term_gains": round(short_term_gains, 2),
        "long_term_gains": round(long_term_gains, 2),
        "discount_amount": discount_amount,
        "capital_losses": round(capital_losses, 2),
        "net_capital_gain": net_capital_gain,
        "carried_forward_loss": carried_forward_loss,
        "share_gains": round(share_gains, 2),
        "crypto_gains": round(crypto_gains, 2),
        "disposals": rows,
        "disposal_count": len(rows),
    }
