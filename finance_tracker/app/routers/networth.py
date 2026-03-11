from __future__ import annotations

from datetime import datetime, timezone
from typing import Optional

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel
from sqlmodel import Session, select, func

from database import NetWorthSnapshot, CryptoHolding, ShareHolding, Transaction, get_session, User
from deps import get_setting, get_current_user

router = APIRouter(prefix="/api/networth", tags=["networth"])


def _compute_totals(body: dict) -> dict:
    assets = (
        body.get("cash_savings", 0) +
        body.get("super_balance", 0) +
        body.get("property_value", 0) +
        body.get("shares_value", 0) +
        body.get("crypto_value", 0) +
        body.get("other_assets", 0)
    )
    liabilities = (
        body.get("mortgage_balance", 0) +
        body.get("car_loan", 0) +
        body.get("credit_card", 0) +
        body.get("hecs_debt", 0) +
        body.get("other_liabilities", 0)
    )
    return {
        "total_assets": round(assets, 2),
        "total_liabilities": round(liabilities, 2),
        "net_worth": round(assets - liabilities, 2),
    }


@router.get("")
def list_snapshots(
    session: Session = Depends(get_session),
    current_user: User = Depends(get_current_user),
):
    snaps = session.exec(
        select(NetWorthSnapshot).where(
            NetWorthSnapshot.user_id == current_user.id,
        ).order_by(NetWorthSnapshot.snapshot_date.desc())
    ).all()
    return [s.model_dump() for s in snaps]


@router.get("/latest")
def latest_snapshot(
    session: Session = Depends(get_session),
    current_user: User = Depends(get_current_user),
):
    snap = session.exec(
        select(NetWorthSnapshot).where(
            NetWorthSnapshot.user_id == current_user.id,
        ).order_by(NetWorthSnapshot.snapshot_date.desc())
    ).first()
    if not snap:
        raise HTTPException(404, "No snapshots yet")
    return snap.model_dump()


@router.get("/chart")
def chart_data(
    session: Session = Depends(get_session),
    current_user: User = Depends(get_current_user),
):
    snaps = session.exec(
        select(NetWorthSnapshot).where(
            NetWorthSnapshot.user_id == current_user.id,
        ).order_by(NetWorthSnapshot.snapshot_date.asc())
    ).all()
    return [
        {
            "date": str(s.snapshot_date),
            "label": s.label or str(s.snapshot_date),
            "net_worth": s.net_worth,
            "total_assets": s.total_assets,
            "total_liabilities": s.total_liabilities,
            "shares_value": s.shares_value or 0,
            "crypto_value": s.crypto_value or 0,
        }
        for s in snaps
    ]


class SnapshotCreate(BaseModel):
    snapshot_date: str              # "YYYY-MM-DD"
    label: Optional[str] = None
    cash_savings: float = 0.0
    super_balance: float = 0.0
    property_value: float = 0.0
    shares_value: float = 0.0
    crypto_value: float = 0.0
    other_assets: float = 0.0
    mortgage_balance: float = 0.0
    car_loan: float = 0.0
    credit_card: float = 0.0
    hecs_debt: float = 0.0
    other_liabilities: float = 0.0


@router.post("")
def create_snapshot(
    body: SnapshotCreate,
    session: Session = Depends(get_session),
    current_user: User = Depends(get_current_user),
):
    from datetime import date as _date
    totals = _compute_totals(body.model_dump())
    now_str = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S")
    snap = NetWorthSnapshot(
        snapshot_date=_date.fromisoformat(body.snapshot_date),
        label=body.label,
        created_at=now_str,
        cash_savings=body.cash_savings,
        super_balance=body.super_balance,
        property_value=body.property_value,
        shares_value=body.shares_value,
        crypto_value=body.crypto_value,
        other_assets=body.other_assets,
        mortgage_balance=body.mortgage_balance,
        car_loan=body.car_loan,
        credit_card=body.credit_card,
        hecs_debt=body.hecs_debt,
        other_liabilities=body.other_liabilities,
        user_id=current_user.id,
        **totals,
    )
    session.add(snap)
    session.commit()
    session.refresh(snap)
    return snap.model_dump()


class SnapshotUpdate(BaseModel):
    snapshot_date: Optional[str] = None
    label: Optional[str] = None
    cash_savings: Optional[float] = None
    super_balance: Optional[float] = None
    property_value: Optional[float] = None
    shares_value: Optional[float] = None
    crypto_value: Optional[float] = None
    other_assets: Optional[float] = None
    mortgage_balance: Optional[float] = None
    car_loan: Optional[float] = None
    credit_card: Optional[float] = None
    hecs_debt: Optional[float] = None
    other_liabilities: Optional[float] = None


@router.patch("/{snap_id}")
def update_snapshot(
    snap_id: int,
    body: SnapshotUpdate,
    session: Session = Depends(get_session),
    current_user: User = Depends(get_current_user),
):
    from datetime import date as _date
    snap = session.get(NetWorthSnapshot, snap_id)
    if not snap:
        raise HTTPException(404, "Not found")
    if snap.user_id != current_user.id:
        raise HTTPException(403, "Access denied")
    data = body.model_dump(exclude_none=True)
    for k, v in data.items():
        if k == "snapshot_date":
            setattr(snap, k, _date.fromisoformat(v))
        else:
            setattr(snap, k, v)
    # Recompute totals
    current = snap.model_dump()
    totals = _compute_totals(current)
    for k, v in totals.items():
        setattr(snap, k, v)
    session.add(snap)
    session.commit()
    session.refresh(snap)
    return snap.model_dump()


@router.delete("/{snap_id}")
def delete_snapshot(
    snap_id: int,
    session: Session = Depends(get_session),
    current_user: User = Depends(get_current_user),
):
    snap = session.get(NetWorthSnapshot, snap_id)
    if not snap:
        raise HTTPException(404, "Not found")
    if snap.user_id != current_user.id:
        raise HTTPException(403, "Access denied")
    session.delete(snap)
    session.commit()
    return {"ok": True}


@router.get("/forecast")
def forecast(
    session: Session = Depends(get_session),
    current_user: User = Depends(get_current_user),
):
    """
    Project net worth at 1, 3, 5, 10 years.
    Uses current NW from latest snapshot + avg monthly net savings + assumed return rate.
    """
    from datetime import date

    snap = session.exec(
        select(NetWorthSnapshot).where(
            NetWorthSnapshot.user_id == current_user.id,
        ).order_by(NetWorthSnapshot.snapshot_date.desc())
    ).first()
    if not snap:
        return {"error": "No net worth snapshot found. Create one first."}

    current_nw = snap.net_worth
    today = date.today()

    # Average monthly net savings from last 6 complete months
    monthly_nets = []
    for i in range(1, 7):
        m = today.month - i
        y = today.year
        if m <= 0:
            m += 12
            y -= 1
        inc = float(session.exec(
            select(func.coalesce(func.sum(Transaction.amount), 0)).where(
                Transaction.user_id == current_user.id,
                Transaction.is_credit == True,
                func.strftime("%m", Transaction.date) == f"{m:02d}",
                func.strftime("%Y", Transaction.date) == str(y),
            )
        ).one())
        exp = float(session.exec(
            select(func.coalesce(func.sum(Transaction.amount), 0)).where(
                Transaction.user_id == current_user.id,
                Transaction.is_credit == False,
                func.strftime("%m", Transaction.date) == f"{m:02d}",
                func.strftime("%Y", Transaction.date) == str(y),
            )
        ).one())
        if inc > 0 or exp > 0:
            monthly_nets.append(inc - exp)

    monthly_savings = round(sum(monthly_nets) / len(monthly_nets), 2) if monthly_nets else 0

    annual_return_pct = float(get_setting(session, "forecast_return_pct", "7"))
    r = annual_return_pct / 100 / 12  # monthly rate

    projections = []
    for years in [1, 3, 5, 10]:
        n = years * 12
        if r > 0:
            growth_factor = (1 + r) ** n
            fv_pv = current_nw * growth_factor
            fv_pmt = monthly_savings * (growth_factor - 1) / r
        else:
            fv_pv = current_nw
            fv_pmt = monthly_savings * n
        projected = round(fv_pv + fv_pmt, 0)
        projections.append({
            "years": years,
            "projected_nw": projected,
            "from_growth": round(fv_pv - current_nw, 0),
            "from_savings": round(fv_pmt, 0),
        })

    return {
        "current_nw": round(current_nw, 2),
        "monthly_savings": monthly_savings,
        "annual_return_pct": annual_return_pct,
        "projections": projections,
    }


@router.post("/prefill")
def prefill_snapshot(
    session: Session = Depends(get_session),
    current_user: User = Depends(get_current_user),
):
    """Return live crypto + shares totals to pre-fill a new snapshot (does NOT save)."""
    crypto = session.exec(
        select(CryptoHolding).where(CryptoHolding.user_id == current_user.id)
    ).all()
    shares = session.exec(
        select(ShareHolding).where(ShareHolding.user_id == current_user.id)
    ).all()
    crypto_total = round(sum(h.value_aud for h in crypto), 2)
    shares_total = round(sum(h.value_aud for h in shares), 2)

    crypto_note = ""
    if crypto:
        synced = next((h.synced_at for h in crypto if h.synced_at), None)
        crypto_note = f"Binance sync at {synced}" if synced else f"{len(crypto)} holdings"

    shares_note = ""
    if shares:
        fetched = max((h.price_fetched_at for h in shares if h.price_fetched_at), default=None)
        shares_note = f"prices at {fetched}" if fetched else f"{len(shares)} holdings"

    note_parts = []
    if shares_note:
        note_parts.append(f"Shares from {shares_note}")
    if crypto_note:
        note_parts.append(f"Crypto from {crypto_note}")

    return {
        "shares_value": shares_total,
        "crypto_value": crypto_total,
        "prefill_note": "; ".join(note_parts) if note_parts else "No live investment data available",
    }
