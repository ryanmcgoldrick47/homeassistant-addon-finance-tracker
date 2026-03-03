from __future__ import annotations

from datetime import date
from typing import Optional

from fastapi import APIRouter, Depends, HTTPException, Request
from sqlmodel import Session, select

from database import SuperSnapshot, SuperContribution, get_session
from deps import get_setting

router = APIRouter(prefix="/api/super", tags=["super"])

# ---------------------------------------------------------------------------
# ABS / ASFA benchmark data
# Source: ASFA Superannuation Statistics 2023 / ABS Retirement Intentions 2020-21
# Median super balances by age group and gender (AUD)
# ---------------------------------------------------------------------------

ABS_SUPER_BALANCE = {
    "15-24": {"male": 4_000,   "female": 3_600,  "non_binary": 3_800},
    "25-34": {"male": 40_000,  "female": 30_000, "non_binary": 35_000},
    "35-44": {"male": 100_000, "female": 64_000, "non_binary": 80_000},
    "45-54": {"male": 185_000, "female": 105_000,"non_binary": 140_000},
    "55-64": {"male": 265_000, "female": 160_000,"non_binary": 210_000},
    "65+":   {"male": 320_000, "female": 200_000,"non_binary": 250_000},
}

# ASFA "Comfortable" retirement target (lump sum at 67) by gender
ASFA_TARGET = {"male": 595_000, "female": 595_000, "non_binary": 595_000}

# Australian Super Guarantee rates by FY-end year
SG_RATES = {2024: 11.0, 2025: 11.5, 2026: 11.5, 2027: 12.0}


def _age_group(age: int) -> str:
    if age < 25: return "15-24"
    if age < 35: return "25-34"
    if age < 45: return "35-44"
    if age < 55: return "45-54"
    if age < 65: return "55-64"
    return "65+"


def _balance_percentile(balance: float, median: float) -> int:
    """Estimate % of peers with a LOWER balance."""
    if median <= 0:
        return 50
    ratio = balance / median
    if ratio <= 0:
        return 5
    if ratio <= 1.0:
        return int(ratio * 50)
    if ratio <= 2.0:
        return int(50 + (ratio - 1.0) * 30)
    return min(95, int(80 + (ratio - 2.0) * 10))


# ---------------------------------------------------------------------------
# Endpoints
# ---------------------------------------------------------------------------

@router.get("/summary")
def super_summary(session: Session = Depends(get_session)):
    snapshots = session.exec(
        select(SuperSnapshot).order_by(SuperSnapshot.snapshot_date.desc())
    ).all()
    latest = snapshots[0] if snapshots else None

    age_str = get_setting(session, "profile_age", "")
    gender  = get_setting(session, "profile_gender", "") or "male"

    median_balance  = None
    asfa_target     = None
    balance_pct     = None
    years_to_retire = None
    projected_at_67 = None

    if age_str and age_str.isdigit():
        age = int(age_str)
        ag  = _age_group(age)
        median_balance = float(ABS_SUPER_BALANCE.get(ag, {}).get(gender, 0))
        asfa_target    = float(ASFA_TARGET.get(gender, 595_000))

        if latest and median_balance > 0:
            balance_pct = _balance_percentile(latest.balance_aud, median_balance)

        # Simple projection: assume 7% p.a. growth on existing balance
        if latest and age < 67:
            years_to_retire = 67 - age
            projected_at_67 = round(latest.balance_aud * (1.07 ** years_to_retire), 0)

    # Contributions for current FY
    today    = date.today()
    fy_start = date(today.year - 1 if today.month < 7 else today.year, 7, 1)
    contribs = session.exec(
        select(SuperContribution).where(SuperContribution.contribution_date >= fy_start)
    ).all()
    ytd_employer  = round(sum(c.amount_aud for c in contribs if c.type == "employer"),  2)
    ytd_voluntary = round(sum(c.amount_aud for c in contribs if c.type in ("employee", "voluntary")), 2)

    # All contributions (for contribution list)
    all_contribs = session.exec(
        select(SuperContribution).order_by(SuperContribution.contribution_date.desc()).limit(50)
    ).all()

    return {
        "has_data": latest is not None,
        "latest_balance": latest.balance_aud if latest else 0.0,
        "latest_date": str(latest.snapshot_date) if latest else None,
        "fund_name": latest.fund_name if latest else None,
        "median_balance": median_balance,
        "asfa_target": asfa_target,
        "balance_percentile": balance_pct,
        "years_to_retire": years_to_retire,
        "projected_at_67": projected_at_67,
        "ytd_employer_contributions": ytd_employer,
        "ytd_voluntary_contributions": ytd_voluntary,
        "snapshots_count": len(snapshots),
        "contributions": [
            {
                "id": c.id,
                "contribution_date": str(c.contribution_date),
                "amount_aud": c.amount_aud,
                "type": c.type,
                "source": c.source,
                "notes": c.notes,
            }
            for c in all_contribs
        ],
    }


@router.get("/chart")
def super_chart(session: Session = Depends(get_session)):
    snapshots = session.exec(
        select(SuperSnapshot).order_by(SuperSnapshot.snapshot_date)
    ).all()
    return [
        {
            "date": str(s.snapshot_date),
            "balance_aud": s.balance_aud,
            "fund_name": s.fund_name,
        }
        for s in snapshots
    ]


@router.get("/snapshots")
def list_snapshots(session: Session = Depends(get_session)):
    rows = session.exec(
        select(SuperSnapshot).order_by(SuperSnapshot.snapshot_date.desc())
    ).all()
    return [
        {
            "id": s.id,
            "snapshot_date": str(s.snapshot_date),
            "fund_name": s.fund_name,
            "balance_aud": s.balance_aud,
            "notes": s.notes,
        }
        for s in rows
    ]


@router.post("/snapshot")
async def add_snapshot(request: Request, session: Session = Depends(get_session)):
    from datetime import datetime
    data = await request.json()
    try:
        snap_date = date.fromisoformat(data["snapshot_date"])
    except (KeyError, ValueError):
        raise HTTPException(400, "snapshot_date is required (YYYY-MM-DD)")

    snap = SuperSnapshot(
        snapshot_date=snap_date,
        fund_name=data.get("fund_name") or None,
        balance_aud=float(data.get("balance_aud", 0)),
        notes=data.get("notes") or None,
        created_at=datetime.utcnow().isoformat(),
    )
    session.add(snap)
    session.commit()
    session.refresh(snap)
    return {"id": snap.id, "snapshot_date": str(snap.snapshot_date), "balance_aud": snap.balance_aud}


@router.delete("/snapshot/{snap_id}")
def delete_snapshot(snap_id: int, session: Session = Depends(get_session)):
    snap = session.get(SuperSnapshot, snap_id)
    if not snap:
        raise HTTPException(404, "Not found")
    session.delete(snap)
    session.commit()
    return {"ok": True}


@router.post("/contribution")
async def add_contribution(request: Request, session: Session = Depends(get_session)):
    data = await request.json()
    try:
        contrib_date = date.fromisoformat(data["contribution_date"])
    except (KeyError, ValueError):
        raise HTTPException(400, "contribution_date is required (YYYY-MM-DD)")

    contrib = SuperContribution(
        snapshot_id=data.get("snapshot_id") or None,
        contribution_date=contrib_date,
        amount_aud=float(data.get("amount_aud", 0)),
        type=data.get("type", "employer"),
        source=data.get("source") or None,
        notes=data.get("notes") or None,
    )
    session.add(contrib)
    session.commit()
    session.refresh(contrib)
    return {
        "id": contrib.id,
        "contribution_date": str(contrib.contribution_date),
        "amount_aud": contrib.amount_aud,
        "type": contrib.type,
    }


@router.delete("/contribution/{contrib_id}")
def delete_contribution(contrib_id: int, session: Session = Depends(get_session)):
    contrib = session.get(SuperContribution, contrib_id)
    if not contrib:
        raise HTTPException(404, "Not found")
    session.delete(contrib)
    session.commit()
    return {"ok": True}
