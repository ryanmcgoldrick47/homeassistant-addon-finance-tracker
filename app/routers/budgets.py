from __future__ import annotations

from datetime import date
from typing import Optional
from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel
from sqlmodel import Session, select, func

from database import Budget, Transaction, Category, get_session, engine

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
):
    stmt = select(Budget)
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
):
    """Return budgets for a month with actual spend calculated."""
    budgets = session.exec(
        select(Budget).where(Budget.month == month, Budget.year == year)
    ).all()

    result = []
    for b in budgets:
        cat = session.get(Category, b.category_id)
        if not cat:
            continue

        # Sum spend for this category in this month
        spend = session.exec(
            select(func.coalesce(func.sum(Transaction.amount), 0)).where(
                Transaction.category_id == b.category_id,
                Transaction.is_credit == False,
                func.strftime("%m", Transaction.date) == f"{month:02d}",
                func.strftime("%Y", Transaction.date) == str(year),
            )
        ).one()

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
        })

    return result


@router.get("/zbb-summary")
def zbb_summary(month: int, year: int, session: Session = Depends(get_session)):
    """Zero-Based Budget summary: income vs total allocated for the month."""
    income = float(session.exec(
        select(func.coalesce(func.sum(Transaction.amount), 0)).where(
            Transaction.is_credit == True,
            func.strftime("%m", Transaction.date) == f"{month:02d}",
            func.strftime("%Y", Transaction.date) == str(year),
        )
    ).one())

    allocated_cents = float(session.exec(
        select(func.coalesce(func.sum(Budget.amount_cents), 0)).where(
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
def auto_fill_budgets(month: int, year: int, session: Session = Depends(get_session)):
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
            select(Budget).where(Budget.month == month, Budget.year == year)
        ).all()
    }

    SKIP = {"Uncategorised", "Investment Transfer"}
    created = 0
    for cat in cats:
        if cat.id in existing_ids or cat.name in SKIP:
            continue
        avg_spend = float(session.exec(
            select(func.coalesce(func.sum(Transaction.amount), 0)).where(
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
        ))
        created += 1
    session.commit()
    return {"created": created}


@router.post("")
def create_budget(body: BudgetCreate, session: Session = Depends(get_session)):
    # Upsert: one budget per category per month/year
    existing = session.exec(
        select(Budget).where(
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

    b = Budget(**body.model_dump())
    session.add(b)
    session.commit()
    session.refresh(b)
    return b


@router.patch("/{budget_id}")
def update_budget(budget_id: int, body: BudgetUpdate, session: Session = Depends(get_session)):
    b = session.get(Budget, budget_id)
    if not b:
        raise HTTPException(status_code=404, detail="Not found")
    b.amount_cents = body.amount_cents
    session.add(b)
    session.commit()
    session.refresh(b)
    return b


@router.delete("/{budget_id}")
def delete_budget(budget_id: int, session: Session = Depends(get_session)):
    b = session.get(Budget, budget_id)
    if not b:
        raise HTTPException(status_code=404, detail="Not found")
    session.delete(b)
    session.commit()
    return {"ok": True}
