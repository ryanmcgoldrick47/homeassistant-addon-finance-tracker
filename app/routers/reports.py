"""Custom date-range reports."""
from __future__ import annotations

from datetime import date
from typing import Optional
from collections import defaultdict

from fastapi import APIRouter, Depends
from sqlmodel import Session, select, func

from database import Transaction, Category, Account, get_session, User
from deps import get_current_user

router = APIRouter(prefix="/api/reports", tags=["reports"])


@router.get("/custom")
def custom_report(
    date_from: date,
    date_to: date,
    account_id: Optional[int] = None,
    session: Session = Depends(get_session),
    current_user: User = Depends(get_current_user),
):
    stmt = select(Transaction).where(
        Transaction.user_id == current_user.id,
        Transaction.date >= date_from,
        Transaction.date <= date_to,
    )
    if account_id:
        stmt = stmt.where(Transaction.account_id == account_id)

    txns = session.exec(stmt).all()

    total_income = sum(t.amount for t in txns if t.is_credit)
    total_spend  = sum(t.amount for t in txns if not t.is_credit)
    net_savings  = total_income - total_spend

    # Spend by category
    by_cat: dict[int | None, float] = defaultdict(float)
    for t in txns:
        if not t.is_credit:
            by_cat[t.category_id] += float(t.amount)

    # Fetch category names
    cat_cache: dict[int, Category] = {}
    for cat_id in by_cat:
        if cat_id and cat_id not in cat_cache:
            c = session.get(Category, cat_id)
            if c:
                cat_cache[cat_id] = c

    spend_by_category = sorted([
        {
            "category_id": cat_id,
            "category_name": cat_cache[cat_id].name if cat_id and cat_id in cat_cache else "Uncategorised",
            "colour": cat_cache[cat_id].colour if cat_id and cat_id in cat_cache else "#d1d5db",
            "amount": round(amt, 2),
            "pct": round(amt / total_spend * 100, 1) if total_spend else 0,
        }
        for cat_id, amt in by_cat.items()
    ], key=lambda x: x["amount"], reverse=True)

    # Income by category
    by_income_cat: dict[int | None, float] = defaultdict(float)
    for t in txns:
        if t.is_credit:
            by_income_cat[t.category_id] += float(t.amount)

    income_by_category = sorted([
        {
            "category_id": cat_id,
            "category_name": cat_cache[cat_id].name if cat_id and cat_id in cat_cache else "Other Income",
            "amount": round(amt, 2),
        }
        for cat_id, amt in by_income_cat.items()
    ], key=lambda x: x["amount"], reverse=True)

    # Top merchants
    by_merchant: dict[str, float] = defaultdict(float)
    for t in txns:
        if not t.is_credit:
            by_merchant[t.description.strip()] += float(t.amount)

    top_merchants = sorted([
        {"merchant": m, "amount": round(a, 2)}
        for m, a in by_merchant.items()
    ], key=lambda x: x["amount"], reverse=True)[:10]

    # Day-by-day net (for sparkline)
    daily: dict[str, dict] = {}
    for t in txns:
        d = str(t.date)
        if d not in daily:
            daily[d] = {"date": d, "income": 0.0, "spend": 0.0}
        if t.is_credit:
            daily[d]["income"] += float(t.amount)
        else:
            daily[d]["spend"] += float(t.amount)
    daily_series = sorted(daily.values(), key=lambda x: x["date"])

    # Savings rate
    savings_rate = round(net_savings / total_income * 100, 1) if total_income > 0 else 0

    # Accounts available
    accounts = session.exec(
        select(Account).where(Account.user_id == current_user.id)
    ).all()

    return {
        "date_from": str(date_from),
        "date_to": str(date_to),
        "total_income": round(total_income, 2),
        "total_spend": round(total_spend, 2),
        "net_savings": round(net_savings, 2),
        "savings_rate": savings_rate,
        "transaction_count": len(txns),
        "spend_by_category": spend_by_category,
        "income_by_category": income_by_category,
        "top_merchants": top_merchants,
        "daily_series": daily_series,
        "accounts": [{"id": a.id, "name": a.name} for a in accounts],
    }
