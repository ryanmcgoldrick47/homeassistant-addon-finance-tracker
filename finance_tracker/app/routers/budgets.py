from __future__ import annotations

from datetime import date, timedelta as _td
from typing import Optional
from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel
from sqlmodel import Session, select, func

from database import Budget, Transaction, Category, get_session, engine, User
from deps import get_current_user

router = APIRouter(prefix="/api/budgets", tags=["budgets"])


class BudgetCreate(BaseModel):
    category_id: int
    month: int
    year: int
    amount_cents: int  # e.g. 50000 = $500.00


class BudgetUpdate(BaseModel):
    amount_cents: int


@router.get("")
def list_budgets(
    month: Optional[int] = None,
    year: Optional[int] = None,
    session: Session = Depends(get_session),
    current_user: User = Depends(get_current_user),
):
    stmt = select(Budget).where(Budget.user_id == current_user.id)
    if month:
        stmt = stmt.where(Budget.month == month)
    if year:
        stmt = stmt.where(Budget.year == year)
    budgets = session.exec(stmt).all()
    result = []
    for b in budgets:
        cat = session.get(Category, b.category_id)
        result.append({
            **b.model_dump(),
            "category_name": cat.name if cat else None,
            "category_colour": cat.colour if cat else "#d1d5db",
        })
    return result


@router.get("/vs-spend")
def budgets_vs_spend(
    month: int,
    year: int,
    session: Session = Depends(get_session),
    current_user: User = Depends(get_current_user),
):
    """Return budgets for a month with actual spend calculated."""
    budgets = session.exec(
        select(Budget).where(
            Budget.user_id == current_user.id,
            Budget.month == month,
            Budget.year == year,
        )
    ).all()

    # Compute last-month and last-7-days ranges
    today = date.today()
    lm_month = month - 1 if month > 1 else 12
    lm_year = year if month > 1 else year - 1
    week_start = today - _td(days=6)

    result = []
    for b in budgets:
        cat = session.get(Category, b.category_id)
        if not cat:
            continue

        def _spend_query(mm: int, yy: int) -> float:
            return float(session.exec(
                select(func.coalesce(func.sum(Transaction.amount), 0)).where(
                    Transaction.user_id == current_user.id,
                    Transaction.category_id == b.category_id,
                    Transaction.is_credit == False,
                    Transaction.is_reimbursable == False,
                    func.strftime("%m", Transaction.date) == f"{mm:02d}",
                    func.strftime("%Y", Transaction.date) == str(yy),
                )
            ).one())

        spend = _spend_query(month, year)
        last_month_spend = _spend_query(lm_month, lm_year)

        # Last 7 days
        last_week_spend = float(session.exec(
            select(func.coalesce(func.sum(Transaction.amount), 0)).where(
                Transaction.user_id == current_user.id,
                Transaction.category_id == b.category_id,
                Transaction.is_credit == False,
                Transaction.is_reimbursable == False,
                Transaction.date >= week_start,
                Transaction.date <= today,
            )
        ).one())

        budget_amt = b.amount_cents / 100
        pct = round((spend / budget_amt * 100) if budget_amt > 0 else 0, 1)

        result.append({
            "id": b.id,
            "category_id": b.category_id,
            "category_name": cat.name,
            "category_colour": cat.colour,
            "budget": budget_amt,
            "spend": round(spend, 2),
            "remaining": round(budget_amt - spend, 2),
            "pct": pct,
            "status": "green" if pct < 75 else ("amber" if pct < 100 else "red"),
            "last_month_spend": round(last_month_spend, 2),
            "last_week_spend": round(last_week_spend, 2),
        })

    return result


@router.get("/forecast")
def budget_forecast(
    session: Session = Depends(get_session),
    current_user: User = Depends(get_current_user),
):
    """Project each budget category's spend to month-end based on daily pace."""
    import calendar
    today = date.today()
    month, year = today.month, today.year
    days_elapsed = today.day
    days_in_month = calendar.monthrange(year, month)[1]
    days_remaining = days_in_month - days_elapsed

    budgets = session.exec(
        select(Budget).where(
            Budget.user_id == current_user.id,
            Budget.month == month,
            Budget.year == year,
        )
    ).all()

    result = []
    for b in budgets:
        cat = session.get(Category, b.category_id)
        if not cat:
            continue
        spend = float(session.exec(
            select(func.coalesce(func.sum(Transaction.amount), 0)).where(
                Transaction.user_id == current_user.id,
                Transaction.category_id == b.category_id,
                Transaction.is_credit == False,
                Transaction.is_reimbursable == False,
                func.strftime("%m", Transaction.date) == f"{month:02d}",
                func.strftime("%Y", Transaction.date) == str(year),
            )
        ).one())

        daily_rate = spend / days_elapsed if days_elapsed > 0 else 0
        projected = round(spend + daily_rate * days_remaining, 2)
        budget_amt = b.amount_cents / 100
        proj_pct = round(projected / budget_amt * 100, 1) if budget_amt > 0 else 0

        result.append({
            "category_id": b.category_id,
            "category_name": cat.name,
            "category_colour": cat.colour,
            "budget": budget_amt,
            "spend_so_far": round(spend, 2),
            "projected_total": projected,
            "proj_pct": proj_pct,
            "days_elapsed": days_elapsed,
            "days_in_month": days_in_month,
            "status": "green" if proj_pct < 90 else ("amber" if proj_pct < 110 else "red"),
        })

    result.sort(key=lambda x: x["proj_pct"], reverse=True)
    return {
        "month": today.strftime("%B %Y"),
        "days_elapsed": days_elapsed,
        "days_in_month": days_in_month,
        "days_remaining": days_remaining,
        "items": result,
    }


@router.get("/zbb-summary")
def zbb_summary(
    month: int,
    year: int,
    session: Session = Depends(get_session),
    current_user: User = Depends(get_current_user),
):
    """Zero-Based Budget summary: income vs total allocated for the month."""
    income = float(session.exec(
        select(func.coalesce(func.sum(Transaction.amount), 0)).where(
            Transaction.user_id == current_user.id,
            Transaction.is_credit == True,
            func.strftime("%m", Transaction.date) == f"{month:02d}",
            func.strftime("%Y", Transaction.date) == str(year),
        )
    ).one())

    allocated_cents = float(session.exec(
        select(func.coalesce(func.sum(Budget.amount_cents), 0)).where(
            Budget.user_id == current_user.id,
            Budget.month == month,
            Budget.year == year,
        )
    ).one())
    allocated = allocated_cents / 100

    unallocated = round(income - allocated, 2)
    pct_allocated = round(allocated / income * 100, 1) if income > 0 else 0

    return {
        "income": round(income, 2),
        "allocated": round(allocated, 2),
        "unallocated": unallocated,
        "pct_allocated": pct_allocated,
    }


@router.post("/auto-fill")
def auto_fill_budgets(
    month: int,
    year: int,
    session: Session = Depends(get_session),
    current_user: User = Depends(get_current_user),
):
    """Create budgets for unbudgeted spend categories based on 3-month average spend."""
    # 3-month lookback window
    start_m, start_y = month - 3, year
    if start_m <= 0:
        start_m += 12
        start_y -= 1
    start = date(start_y, start_m, 1)
    end = date(year, month, 1)

    cats = session.exec(select(Category).where(Category.is_income == False)).all()
    existing_ids = {
        b.category_id for b in session.exec(
            select(Budget).where(
                Budget.user_id == current_user.id,
                Budget.month == month,
                Budget.year == year,
            )
        ).all()
    }

    SKIP = {"Uncategorised", "Investment Transfer"}
    created = 0
    for cat in cats:
        if cat.id in existing_ids or cat.name in SKIP:
            continue
        avg_spend = float(session.exec(
            select(func.coalesce(func.sum(Transaction.amount), 0)).where(
                Transaction.user_id == current_user.id,
                Transaction.category_id == cat.id,
                Transaction.is_credit == False,
                Transaction.date >= start,
                Transaction.date < end,
            )
        ).one()) / 3
        if avg_spend < 1:
            continue
        session.add(Budget(
            category_id=cat.id,
            month=month,
            year=year,
            amount_cents=int(round(avg_spend * 100)),
            user_id=current_user.id,
        ))
        created += 1
    session.commit()
    return {"created": created}


@router.post("")
def create_budget(
    body: BudgetCreate,
    session: Session = Depends(get_session),
    current_user: User = Depends(get_current_user),
):
    # Upsert: one budget per category per month/year per user
    existing = session.exec(
        select(Budget).where(
            Budget.user_id == current_user.id,
            Budget.category_id == body.category_id,
            Budget.month == body.month,
            Budget.year == body.year,
        )
    ).first()
    if existing:
        existing.amount_cents = body.amount_cents
        session.add(existing)
        session.commit()
        session.refresh(existing)
        return existing

    b = Budget(**body.model_dump(), user_id=current_user.id)
    session.add(b)
    session.commit()
    session.refresh(b)
    return b


@router.patch("/{budget_id}")
def update_budget(
    budget_id: int,
    body: BudgetUpdate,
    session: Session = Depends(get_session),
    current_user: User = Depends(get_current_user),
):
    b = session.get(Budget, budget_id)
    if not b:
        raise HTTPException(status_code=404, detail="Not found")
    if b.user_id != current_user.id:
        raise HTTPException(status_code=403, detail="Access denied")
    b.amount_cents = body.amount_cents
    session.add(b)
    session.commit()
    session.refresh(b)
    return b


@router.delete("/{budget_id}")
def delete_budget(
    budget_id: int,
    session: Session = Depends(get_session),
    current_user: User = Depends(get_current_user),
):
    b = session.get(Budget, budget_id)
    if not b:
        raise HTTPException(status_code=404, detail="Not found")
    if b.user_id != current_user.id:
        raise HTTPException(status_code=403, detail="Access denied")
    session.delete(b)
    session.commit()
    return {"ok": True}
