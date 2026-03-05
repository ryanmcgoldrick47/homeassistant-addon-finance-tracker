from __future__ import annotations

import os
from datetime import date, datetime
from typing import Optional
from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel
from sqlmodel import Session, select, func
import httpx

from database import get_session, Goal, GoalContribution, Category, Transaction, User
from deps import get_current_user

router = APIRouter()


# ---------------------------------------------------------------------------
# Schemas
# ---------------------------------------------------------------------------

class GoalIn(BaseModel):
    name: str
    goal_type: str = "long_term"  # "short_term" | "long_term"
    target_cents: int
    target_date: Optional[str] = None
    category_id: Optional[int] = None
    notes: Optional[str] = None


class ContributionIn(BaseModel):
    amount_cents: int
    contributed_date: Optional[str] = None
    notes: Optional[str] = None


class SavingsRateTargetIn(BaseModel):
    target_pct: float


# ---------------------------------------------------------------------------
# HA notification helper (fire-and-forget)
# ---------------------------------------------------------------------------

async def _notify_goal_complete(goal_name: str, amount: float, session: Session):
    from deps import get_setting
    ha_url = get_setting(session, "ha_url", "http://hassio/core")
    token = os.environ.get("SUPERVISOR_TOKEN", "") or get_setting(session, "ha_token", "")
    targets_str = get_setting(session, "ha_notify_targets", "mobile_app_ryans_iphone")
    if not token:
        return
    targets = [t.strip() for t in targets_str.split(",") if t.strip()]
    async with httpx.AsyncClient(timeout=8) as client:
        for t in targets:
            try:
                await client.post(
                    f"{ha_url}/api/services/notify/{t}",
                    headers={"Authorization": f"Bearer {token}"},
                    json={
                        "title": f"Goal reached: {goal_name}!",
                        "message": f"You've hit your ${amount:,.2f} savings goal. Well done!",
                    },
                )
            except Exception:
                pass


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _goal_dict(goal: Goal, session: Session) -> dict:
    cat = session.get(Category, goal.category_id) if goal.category_id else None
    contributions = session.exec(
        select(GoalContribution)
        .where(GoalContribution.goal_id == goal.id)
        .order_by(GoalContribution.contributed_date.desc())
    ).all()
    pct = round(goal.current_cents / goal.target_cents * 100, 1) if goal.target_cents > 0 else 0
    return {
        "id": goal.id,
        "name": goal.name,
        "goal_type": goal.goal_type or "long_term",
        "target_cents": goal.target_cents,
        "current_cents": goal.current_cents,
        "target_aud": round(goal.target_cents / 100, 2),
        "current_aud": round(goal.current_cents / 100, 2),
        "remaining_aud": round(max(0, goal.target_cents - goal.current_cents) / 100, 2),
        "pct": min(pct, 100),
        "target_date": str(goal.target_date) if goal.target_date else None,
        "category_id": goal.category_id,
        "category_name": cat.name if cat else None,
        "is_complete": goal.is_complete,
        "notes": goal.notes,
        "created_at": goal.created_at,
        "contributions": [
            {
                "id": c.id,
                "contributed_date": str(c.contributed_date),
                "amount_cents": c.amount_cents,
                "amount_aud": round(c.amount_cents / 100, 2),
                "notes": c.notes,
            }
            for c in contributions
        ],
    }


# ---------------------------------------------------------------------------
# Goal CRUD
# ---------------------------------------------------------------------------

@router.get("/api/goals")
def list_goals(
    session: Session = Depends(get_session),
    current_user: User = Depends(get_current_user),
):
    goals = session.exec(
        select(Goal).where(Goal.user_id == current_user.id).order_by(Goal.is_complete, Goal.target_date)
    ).all()
    return [_goal_dict(g, session) for g in goals]


@router.post("/api/goals")
def create_goal(
    body: GoalIn,
    session: Session = Depends(get_session),
    current_user: User = Depends(get_current_user),
):
    goal = Goal(
        name=body.name,
        goal_type=body.goal_type or "long_term",
        target_cents=body.target_cents,
        target_date=date.fromisoformat(body.target_date) if body.target_date else None,
        category_id=body.category_id,
        notes=body.notes,
        created_at=datetime.now().isoformat(timespec="seconds"),
        user_id=current_user.id,
    )
    session.add(goal)
    session.commit()
    session.refresh(goal)
    return {"ok": True, "id": goal.id}


@router.delete("/api/goals/{goal_id}")
def delete_goal(
    goal_id: int,
    session: Session = Depends(get_session),
    current_user: User = Depends(get_current_user),
):
    goal = session.get(Goal, goal_id)
    if not goal:
        raise HTTPException(404, "Goal not found")
    if goal.user_id != current_user.id:
        raise HTTPException(403, "Access denied")
    # cascade delete contributions
    for c in session.exec(select(GoalContribution).where(GoalContribution.goal_id == goal_id)).all():
        session.delete(c)
    session.delete(goal)
    session.commit()
    return {"ok": True}


@router.patch("/api/goals/{goal_id}/complete")
async def mark_complete(
    goal_id: int,
    session: Session = Depends(get_session),
    current_user: User = Depends(get_current_user),
):
    goal = session.get(Goal, goal_id)
    if not goal:
        raise HTTPException(404, "Goal not found")
    if goal.user_id != current_user.id:
        raise HTTPException(403, "Access denied")
    goal.is_complete = True
    session.add(goal)
    session.commit()
    await _notify_goal_complete(goal.name, goal.target_cents / 100, session)
    return {"ok": True}


# ---------------------------------------------------------------------------
# Contributions
# ---------------------------------------------------------------------------

@router.post("/api/goals/{goal_id}/contribute")
async def add_contribution(
    goal_id: int,
    body: ContributionIn,
    session: Session = Depends(get_session),
    current_user: User = Depends(get_current_user),
):
    goal = session.get(Goal, goal_id)
    if not goal:
        raise HTTPException(404, "Goal not found")
    if goal.user_id != current_user.id:
        raise HTTPException(403, "Access denied")

    contrib = GoalContribution(
        goal_id=goal_id,
        contributed_date=date.fromisoformat(body.contributed_date) if body.contributed_date else date.today(),
        amount_cents=body.amount_cents,
        notes=body.notes,
    )
    session.add(contrib)

    goal.current_cents += body.amount_cents
    newly_complete = not goal.is_complete and goal.current_cents >= goal.target_cents
    if newly_complete:
        goal.is_complete = True
    session.add(goal)
    session.commit()

    if newly_complete:
        await _notify_goal_complete(goal.name, goal.target_cents / 100, session)

    return {"ok": True, "current_cents": goal.current_cents, "is_complete": goal.is_complete}


@router.delete("/api/goals/{goal_id}/contributions/{contrib_id}")
def delete_contribution(
    goal_id: int,
    contrib_id: int,
    session: Session = Depends(get_session),
    current_user: User = Depends(get_current_user),
):
    contrib = session.get(GoalContribution, contrib_id)
    if not contrib or contrib.goal_id != goal_id:
        raise HTTPException(404, "Contribution not found")
    goal = session.get(Goal, goal_id)
    if not goal or goal.user_id != current_user.id:
        raise HTTPException(403, "Access denied")
    goal.current_cents = max(0, goal.current_cents - contrib.amount_cents)
    if goal.current_cents < goal.target_cents:
        goal.is_complete = False
    session.add(goal)
    session.delete(contrib)
    session.commit()
    return {"ok": True}


# ---------------------------------------------------------------------------
# Monthly savings rate target
# ---------------------------------------------------------------------------

@router.get("/api/goals/savings-rate")
def get_savings_rate_history(
    session: Session = Depends(get_session),
    current_user: User = Depends(get_current_user),
):
    from deps import get_setting
    target_pct = float(get_setting(session, "savings_rate_target") or "20")

    today = date.today()
    months = []
    for i in range(11, -1, -1):
        m = today.month - i
        y = today.year
        while m <= 0:
            m += 12
            y -= 1

        def _sum(is_credit: bool) -> float:
            return float(session.exec(
                select(func.coalesce(func.sum(Transaction.amount), 0)).where(
                    Transaction.user_id == current_user.id,
                    Transaction.is_credit == is_credit,
                    func.strftime("%m", Transaction.date) == f"{m:02d}",
                    func.strftime("%Y", Transaction.date) == str(y),
                )
            ).one())

        income = _sum(True)
        spending = _sum(False)
        net = income - spending
        actual_pct = round(net / income * 100, 1) if income > 0 else 0
        months.append({
            "year": y,
            "month": m,
            "label": date(y, m, 1).strftime("%b %y"),
            "income": round(income, 2),
            "spending": round(spending, 2),
            "net": round(net, 2),
            "actual_pct": actual_pct,
            "met_target": actual_pct >= target_pct,
            "has_data": income > 0 or spending > 0,
        })

    data_months = [mo for mo in months if mo["has_data"]]
    avg_pct = round(sum(mo["actual_pct"] for mo in data_months) / len(data_months), 1) if data_months else 0
    months_met = sum(1 for mo in data_months if mo["met_target"])
    all_pcts = [mo["actual_pct"] for mo in data_months] + [float(target_pct), 5.0]
    scale = round(max(all_pcts) * 1.2, 1)

    return {
        "target_pct": target_pct,
        "months": months,
        "avg_pct": avg_pct,
        "months_met": months_met,
        "total_months_with_data": len(data_months),
        "_scale": scale,
    }


@router.patch("/api/goals/savings-rate")
def set_savings_rate_target(
    body: SavingsRateTargetIn,
    session: Session = Depends(get_session),
    current_user: User = Depends(get_current_user),
):
    from deps import set_setting
    set_setting(session, "savings_rate_target", str(body.target_pct))
    return {"ok": True, "target_pct": body.target_pct}
