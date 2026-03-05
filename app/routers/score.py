from __future__ import annotations

"""
Finance Score, Achievements, Streaks, and Challenges.
"""

import json
from datetime import date, datetime
from typing import Optional
from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel
from sqlmodel import Session, select, func

from database import (
    get_session, Transaction, Category, Budget, Bill, BillPayment,
    Achievement, Challenge, User,
)
from deps import get_current_user

router = APIRouter()


# ---------------------------------------------------------------------------
# Achievement definitions
# ---------------------------------------------------------------------------

ACHIEVEMENT_DEFS = {
    "first_green_month": {
        "label": "First Green Month",
        "desc": "Had a month where income exceeded spending",
        "icon": "🌱",
    },
    "budget_master": {
        "label": "Budget Master",
        "desc": "Finished a month with all budgeted categories under 100%",
        "icon": "🎯",
    },
    "savings_streak_3": {
        "label": "Savings Streak",
        "desc": "3 consecutive months with positive savings",
        "icon": "🔥",
    },
    "inbox_zero": {
        "label": "Inbox Zero",
        "desc": "Cleared all flagged transactions for review",
        "icon": "✅",
    },
    "bill_buster": {
        "label": "Bill Buster",
        "desc": "All bills paid on time for a full month",
        "icon": "⚡",
    },
    "big_saver": {
        "label": "Big Saver",
        "desc": "Achieved a savings rate above 20% in a month",
        "icon": "💰",
    },
    "score_80": {
        "label": "Finance Pro",
        "desc": "Scored 80 or higher on the Finance Score",
        "icon": "🏆",
    },
    "categorised_all": {
        "label": "Organised",
        "desc": "All transactions categorised for a full month",
        "icon": "🗂️",
    },
}


# ---------------------------------------------------------------------------
# Score computation
# ---------------------------------------------------------------------------

def _month_totals(session: Session, month: int, year: int, user_id: int) -> tuple[float, float]:
    """Return (income, spend) for a given month."""
    def _sum(is_credit: bool) -> float:
        return float(session.exec(
            select(func.coalesce(func.sum(Transaction.amount), 0)).where(
                Transaction.user_id == user_id,
                Transaction.is_credit == is_credit,
                func.strftime("%m", Transaction.date) == f"{month:02d}",
                func.strftime("%Y", Transaction.date) == str(year),
            )
        ).one())
    return _sum(True), _sum(False)


def _compute_score(session: Session, month: int, year: int, user_id: int = 1) -> dict:
    income, spend = _month_totals(session, month, year, user_id)
    net = income - spend

    # ── 1. Savings rate (30 pts) — 20% savings rate = full score ──
    savings_rate = (net / income) if income > 0 else 0
    savings_score = round(min(30, max(0, savings_rate / 0.20 * 30)))

    # ── 2. Budget adherence (30 pts) ──
    budgets = session.exec(
        select(Budget).where(
            Budget.user_id == user_id,
            Budget.month == month,
            Budget.year == year,
        )
    ).all()
    budget_score = 30  # default full if no budgets set
    budget_detail = []
    if budgets:
        under = 0
        for b in budgets:
            cat_spend = float(session.exec(
                select(func.coalesce(func.sum(Transaction.amount), 0)).where(
                    Transaction.user_id == user_id,
                    Transaction.is_credit == False,
                    Transaction.category_id == b.category_id,
                    func.strftime("%m", Transaction.date) == f"{month:02d}",
                    func.strftime("%Y", Transaction.date) == str(year),
                )
            ).one())
            budget_amt = b.amount_cents / 100
            ok = cat_spend <= budget_amt
            if ok:
                under += 1
            budget_detail.append({"category_id": b.category_id, "ok": ok,
                                   "spend": round(cat_spend, 2), "budget": round(budget_amt, 2)})
        budget_score = round(under / len(budgets) * 30)

    # ── 3. Categorisation (20 pts) ──
    uncat_cat = session.exec(select(Category).where(Category.name == "Uncategorised")).first()
    total_txns = session.exec(
        select(func.count(Transaction.id)).where(
            Transaction.user_id == user_id,
            func.strftime("%m", Transaction.date) == f"{month:02d}",
            func.strftime("%Y", Transaction.date) == str(year),
            Transaction.is_credit == False,
        )
    ).one() or 0

    uncat_txns = 0
    if uncat_cat and total_txns > 0:
        uncat_txns = session.exec(
            select(func.count(Transaction.id)).where(
                Transaction.user_id == user_id,
                Transaction.category_id == uncat_cat.id,
                func.strftime("%m", Transaction.date) == f"{month:02d}",
                func.strftime("%Y", Transaction.date) == str(year),
                Transaction.is_credit == False,
            )
        ).one() or 0

    cat_rate = 1 - (uncat_txns / total_txns) if total_txns > 0 else 1.0
    cat_score = round(cat_rate * 20)

    # ── 4. Bills on time (20 pts) ──
    # Look for bills that were due this month and whether they have a payment
    bills_score = 20  # default full if no bills
    fy_start = date(year, month, 1)
    from calendar import monthrange
    fy_end = date(year, month, monthrange(year, month)[1])
    due_bills = session.exec(
        select(Bill).where(
            Bill.user_id == user_id,
            Bill.is_active == True,
            Bill.next_due != None,
        )
    ).all()
    # Bills due within the month window
    month_bills = [b for b in due_bills if fy_start <= (b.next_due or date.max) <= fy_end]
    if month_bills:
        paid_on_time = 0
        for b in month_bills:
            payment = session.exec(
                select(BillPayment).where(
                    BillPayment.bill_id == b.id,
                    BillPayment.paid_date >= fy_start,
                    BillPayment.paid_date <= fy_end,
                )
            ).first()
            if payment:
                paid_on_time += 1
        bills_score = round(paid_on_time / len(month_bills) * 20)

    total_score = savings_score + budget_score + cat_score + bills_score

    return {
        "month": month,
        "year": year,
        "score": total_score,
        "savings_score": savings_score,
        "budget_score": budget_score,
        "cat_score": cat_score,
        "bills_score": bills_score,
        "income": round(income, 2),
        "spend": round(spend, 2),
        "net": round(net, 2),
        "savings_rate_pct": round(savings_rate * 100, 1),
        "cat_rate_pct": round(cat_rate * 100, 1),
        "budget_detail": budget_detail,
        "has_data": total_txns > 0,
    }


# ---------------------------------------------------------------------------
# Streak helpers
# ---------------------------------------------------------------------------

def _green_streak(session: Session, as_of: date, user_id: int) -> dict:
    """Count consecutive months ending in as_of month where net > 0."""
    current = 0
    best = 0
    m, y = as_of.month, as_of.year
    # Walk backwards up to 36 months
    for _ in range(36):
        income, spend = _month_totals(session, m, y, user_id)
        if income == 0 and spend == 0:
            break  # no data, stop
        if income > spend:
            current += 1
            best = max(best, current)
        else:
            if current > 0:
                break  # streak broken
        m -= 1
        if m == 0:
            m = 12
            y -= 1
    return {"current": current, "best": best, "label": "Green months in a row"}


def _budget_streak(session: Session, as_of: date, user_id: int) -> dict:
    """Consecutive months where all budgets were under."""
    current = 0
    best = 0
    m, y = as_of.month, as_of.year
    for _ in range(24):
        budgets = session.exec(
            select(Budget).where(
                Budget.user_id == user_id,
                Budget.month == m,
                Budget.year == y,
            )
        ).all()
        if not budgets:
            if current > 0:
                break
            m -= 1
            if m == 0:
                m = 12
                y -= 1
            continue
        all_under = True
        for b in budgets:
            cat_spend = float(session.exec(
                select(func.coalesce(func.sum(Transaction.amount), 0)).where(
                    Transaction.user_id == user_id,
                    Transaction.is_credit == False,
                    Transaction.category_id == b.category_id,
                    func.strftime("%m", Transaction.date) == f"{m:02d}",
                    func.strftime("%Y", Transaction.date) == str(y),
                )
            ).one())
            if cat_spend > b.amount_cents / 100:
                all_under = False
                break
        if all_under:
            current += 1
            best = max(best, current)
        else:
            if current > 0:
                break
        m -= 1
        if m == 0:
            m = 12
            y -= 1
    return {"current": current, "best": best, "label": "Under budget months"}


# ---------------------------------------------------------------------------
# Achievement unlock logic
# ---------------------------------------------------------------------------

def _check_achievements(session: Session, month: int, year: int, score_data: dict, user_id: int):
    """Check and unlock any newly earned achievements. Keys are namespaced by user_id."""
    unlocked = []
    now = datetime.now().isoformat(timespec="seconds")

    def _ns(key: str) -> str:
        """Namespace an achievement key for this user."""
        return f"{user_id}:{key}"

    def _unlock(key: str, data: dict = None):
        ns_key = _ns(key)
        existing = session.exec(
            select(Achievement).where(Achievement.key == ns_key)
        ).first()
        if existing is None:
            a = Achievement(key=ns_key, unlocked_at=now,
                            data_json=json.dumps(data) if data else None)
            session.add(a)
            unlocked.append(key)

    # First green month
    if score_data["net"] > 0:
        _unlock("first_green_month", {"month": month, "year": year})

    # Big saver (>20% savings rate)
    if score_data["savings_rate_pct"] >= 20:
        _unlock("big_saver", {"month": month, "year": year, "rate": score_data["savings_rate_pct"]})

    # Budget master (all under budget)
    if score_data["budget_score"] == 30 and score_data["budget_detail"]:
        _unlock("budget_master", {"month": month, "year": year})

    # Categorised all
    if score_data["cat_rate_pct"] == 100 and score_data["has_data"]:
        _unlock("categorised_all", {"month": month, "year": year})

    # Score >= 80
    if score_data["score"] >= 80:
        _unlock("score_80", {"month": month, "year": year, "score": score_data["score"]})

    # Inbox zero — no flagged unreviewed transactions for this user
    flagged = session.exec(
        select(func.count()).where(
            Transaction.user_id == user_id,
            Transaction.is_flagged == True,
            Transaction.is_reviewed == False,
        )
    ).one()
    if flagged == 0 and score_data["has_data"]:
        _unlock("inbox_zero")

    # Savings streak 3
    today = date(year, month, 1)
    streak = _green_streak(session, today, user_id)
    if streak["current"] >= 3:
        _unlock("savings_streak_3", {"streak": streak["current"]})

    # Bill buster
    if score_data["bills_score"] == 20:
        _unlock("bill_buster", {"month": month, "year": year})

    if unlocked:
        session.commit()
    return unlocked


# ---------------------------------------------------------------------------
# Endpoints
# ---------------------------------------------------------------------------

@router.get("/api/score")
def get_score(
    month: int = None,
    year: int = None,
    session: Session = Depends(get_session),
    current_user: User = Depends(get_current_user),
):
    today = date.today()
    m = month or today.month
    y = year or today.year
    data = _compute_score(session, m, y, current_user.id)
    newly_unlocked = _check_achievements(session, m, y, data, current_user.id)
    data["newly_unlocked"] = newly_unlocked
    return data


@router.get("/api/score/achievements")
def get_achievements(
    session: Session = Depends(get_session),
    current_user: User = Depends(get_current_user),
):
    # Fetch only this user's namespaced achievements
    prefix = f"{current_user.id}:"
    all_achievements = session.exec(
        select(Achievement).where(Achievement.key.like(f"{prefix}%"))
    ).all()
    # Strip the namespace prefix for the key in the response
    unlocked = {a.key[len(prefix):]: a for a in all_achievements}
    result = []
    for key, defn in ACHIEVEMENT_DEFS.items():
        ach = unlocked.get(key)
        result.append({
            "key": key,
            "label": defn["label"],
            "desc": defn["desc"],
            "icon": defn["icon"],
            "unlocked": ach is not None,
            "unlocked_at": ach.unlocked_at if ach else None,
            "data": json.loads(ach.data_json) if ach and ach.data_json else None,
        })
    return result


@router.get("/api/score/streaks")
def get_streaks(
    session: Session = Depends(get_session),
    current_user: User = Depends(get_current_user),
):
    today = date.today()
    green = _green_streak(session, today, current_user.id)
    budget = _budget_streak(session, today, current_user.id)
    return {"green": green, "budget": budget}


# ---------------------------------------------------------------------------
# Challenges
# ---------------------------------------------------------------------------

class ChallengeIn(BaseModel):
    name: str
    challenge_type: str = "spend_limit"
    category_id: Optional[int] = None
    target_value: float
    month: Optional[int] = None
    year: Optional[int] = None


@router.get("/api/challenges")
def list_challenges(
    session: Session = Depends(get_session),
    current_user: User = Depends(get_current_user),
):
    today = date.today()
    challenges = session.exec(
        select(Challenge).where(
            Challenge.user_id == current_user.id,
            Challenge.month == today.month,
            Challenge.year == today.year,
        )
    ).all()
    result = []
    for c in challenges:
        cat = session.get(Category, c.category_id) if c.category_id else None

        # Compute progress
        progress = 0.0
        if c.challenge_type == "spend_limit" and c.category_id:
            progress = float(session.exec(
                select(func.coalesce(func.sum(Transaction.amount), 0)).where(
                    Transaction.user_id == current_user.id,
                    Transaction.is_credit == False,
                    Transaction.category_id == c.category_id,
                    func.strftime("%m", Transaction.date) == f"{c.month:02d}",
                    func.strftime("%Y", Transaction.date) == str(c.year),
                )
            ).one())
        elif c.challenge_type == "save_target":
            income, spend = _month_totals(session, c.month, c.year, current_user.id)
            progress = max(0.0, income - spend)

        pct = round(min(100, progress / c.target_value * 100), 1) if c.target_value > 0 else 0
        # For spend_limit: lower is better; for save_target: higher is better
        is_on_track = progress <= c.target_value if c.challenge_type == "spend_limit" else progress >= c.target_value

        result.append({
            "id": c.id,
            "name": c.name,
            "challenge_type": c.challenge_type,
            "category_id": c.category_id,
            "category_name": cat.name if cat else None,
            "target_value": c.target_value,
            "progress": round(progress, 2),
            "pct": pct,
            "is_on_track": is_on_track,
            "is_active": c.is_active,
            "is_complete": c.is_complete,
            "month": c.month,
            "year": c.year,
        })
    return result


@router.post("/api/challenges")
def create_challenge(
    body: ChallengeIn,
    session: Session = Depends(get_session),
    current_user: User = Depends(get_current_user),
):
    today = date.today()
    c = Challenge(
        name=body.name,
        challenge_type=body.challenge_type,
        category_id=body.category_id,
        target_value=body.target_value,
        month=body.month or today.month,
        year=body.year or today.year,
        user_id=current_user.id,
    )
    session.add(c)
    session.commit()
    session.refresh(c)
    return {"ok": True, "id": c.id}


@router.delete("/api/challenges/{challenge_id}")
def delete_challenge(
    challenge_id: int,
    session: Session = Depends(get_session),
    current_user: User = Depends(get_current_user),
):
    c = session.get(Challenge, challenge_id)
    if not c:
        raise HTTPException(404, "Challenge not found")
    if c.user_id != current_user.id:
        raise HTTPException(403, "Access denied")
    session.delete(c)
    session.commit()
    return {"ok": True}
