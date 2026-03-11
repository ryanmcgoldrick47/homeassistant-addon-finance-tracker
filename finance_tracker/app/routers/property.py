"""Investment Property Tracker — CRUD, rental yield, equity, and FY tax summary."""
from __future__ import annotations

from datetime import date
from typing import Optional

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel
from sqlmodel import Session, select, func

from database import Property, PropertyExpense, get_session, User
from deps import get_current_user

router = APIRouter(prefix="/api/properties", tags=["properties"])

DEDUCTIBLE_CATEGORIES = {
    "interest", "rates", "water", "insurance", "strata",
    "management", "repairs", "maintenance", "depreciation", "advertising",
}


def _property_out(p: Property, session: Session) -> dict:
    purchase_price = p.purchase_price_cents / 100
    current_value = p.current_value_cents / 100
    mortgage = p.mortgage_outstanding_cents / 100
    weekly_rent = p.weekly_rent_cents / 100
    annual_rent = weekly_rent * 52

    equity = current_value - mortgage
    equity_pct = round(equity / current_value * 100, 1) if current_value > 0 else 0.0
    gross_yield = round(annual_rent / current_value * 100, 2) if current_value > 0 else 0.0
    capital_growth = current_value - purchase_price
    capital_growth_pct = round(capital_growth / purchase_price * 100, 1) if purchase_price > 0 else 0.0

    return {
        "id": p.id,
        "address": p.address,
        "property_type": p.property_type,
        "purchase_price": purchase_price,
        "purchase_date": str(p.purchase_date) if p.purchase_date else None,
        "current_value": current_value,
        "mortgage_outstanding": mortgage,
        "interest_rate": p.interest_rate,
        "weekly_rent": weekly_rent,
        "annual_rent": annual_rent,
        "ownership_pct": p.ownership_pct,
        "is_active": p.is_active,
        "notes": p.notes,
        # Computed
        "equity": round(equity, 2),
        "equity_pct": equity_pct,
        "gross_yield": gross_yield,
        "capital_growth": round(capital_growth, 2),
        "capital_growth_pct": capital_growth_pct,
        "lvr": round(mortgage / current_value * 100, 1) if current_value > 0 else 0.0,
    }


def _fy_dates(fy_year: int) -> tuple[date, date]:
    return date(fy_year - 1, 7, 1), date(fy_year, 6, 30)


# ── Pydantic models ───────────────────────────────────────────────────────────

class PropertyCreate(BaseModel):
    address: str
    property_type: str = "house"
    purchase_price_cents: int = 0
    purchase_date: Optional[str] = None
    current_value_cents: int = 0
    mortgage_outstanding_cents: int = 0
    interest_rate: float = 0.0
    weekly_rent_cents: int = 0
    ownership_pct: float = 100.0
    notes: Optional[str] = None


class PropertyUpdate(BaseModel):
    address: Optional[str] = None
    property_type: Optional[str] = None
    purchase_price_cents: Optional[int] = None
    purchase_date: Optional[str] = None
    current_value_cents: Optional[int] = None
    mortgage_outstanding_cents: Optional[int] = None
    interest_rate: Optional[float] = None
    weekly_rent_cents: Optional[int] = None
    ownership_pct: Optional[float] = None
    is_active: Optional[bool] = None
    notes: Optional[str] = None


class ExpenseCreate(BaseModel):
    date: str   # ISO date
    category: str = "other"
    description: Optional[str] = None
    amount_cents: int = 0
    is_deductible: bool = True


# ── Endpoints ─────────────────────────────────────────────────────────────────

@router.get("")
def list_properties(
    session: Session = Depends(get_session),
    current_user: User = Depends(get_current_user),
):
    props = session.exec(
        select(Property).where(
            Property.user_id == current_user.id,
            Property.is_active == True,
        ).order_by(Property.id)
    ).all()
    return [_property_out(p, session) for p in props]


@router.post("")
def create_property(
    body: PropertyCreate,
    session: Session = Depends(get_session),
    current_user: User = Depends(get_current_user),
):
    pd = date.fromisoformat(body.purchase_date) if body.purchase_date else None
    p = Property(
        address=body.address,
        property_type=body.property_type,
        purchase_price_cents=body.purchase_price_cents,
        purchase_date=pd,
        current_value_cents=body.current_value_cents,
        mortgage_outstanding_cents=body.mortgage_outstanding_cents,
        interest_rate=body.interest_rate,
        weekly_rent_cents=body.weekly_rent_cents,
        ownership_pct=body.ownership_pct,
        notes=body.notes,
        user_id=current_user.id,
    )
    session.add(p)
    session.commit()
    session.refresh(p)
    return _property_out(p, session)


@router.patch("/{property_id}")
def update_property(
    property_id: int,
    body: PropertyUpdate,
    session: Session = Depends(get_session),
    current_user: User = Depends(get_current_user),
):
    p = session.get(Property, property_id)
    if not p or p.user_id != current_user.id:
        raise HTTPException(404, "Property not found")
    for field, val in body.model_dump(exclude_none=True).items():
        if field == "purchase_date" and val:
            setattr(p, field, date.fromisoformat(val))
        else:
            setattr(p, field, val)
    session.add(p)
    session.commit()
    session.refresh(p)
    return _property_out(p, session)


@router.delete("/{property_id}")
def delete_property(
    property_id: int,
    session: Session = Depends(get_session),
    current_user: User = Depends(get_current_user),
):
    p = session.get(Property, property_id)
    if not p or p.user_id != current_user.id:
        raise HTTPException(404, "Property not found")
    session.delete(p)
    session.commit()
    return {"ok": True}


@router.get("/{property_id}/expenses")
def list_expenses(
    property_id: int,
    fy: Optional[int] = None,
    session: Session = Depends(get_session),
    current_user: User = Depends(get_current_user),
):
    p = session.get(Property, property_id)
    if not p or p.user_id != current_user.id:
        raise HTTPException(404, "Property not found")

    q = select(PropertyExpense).where(
        PropertyExpense.property_id == property_id,
        PropertyExpense.user_id == current_user.id,
    )
    if fy:
        start, end = _fy_dates(fy)
        q = q.where(PropertyExpense.date >= start, PropertyExpense.date <= end)
    expenses = session.exec(q.order_by(PropertyExpense.date.desc())).all()
    return [
        {
            "id": e.id,
            "property_id": e.property_id,
            "date": str(e.date),
            "category": e.category,
            "description": e.description,
            "amount": e.amount_cents / 100,
            "is_deductible": e.is_deductible,
        }
        for e in expenses
    ]


@router.post("/{property_id}/expenses")
def add_expense(
    property_id: int,
    body: ExpenseCreate,
    session: Session = Depends(get_session),
    current_user: User = Depends(get_current_user),
):
    p = session.get(Property, property_id)
    if not p or p.user_id != current_user.id:
        raise HTTPException(404, "Property not found")
    e = PropertyExpense(
        property_id=property_id,
        date=date.fromisoformat(body.date),
        category=body.category,
        description=body.description,
        amount_cents=body.amount_cents,
        is_deductible=body.is_deductible,
        user_id=current_user.id,
    )
    session.add(e)
    session.commit()
    session.refresh(e)
    return {
        "id": e.id,
        "property_id": e.property_id,
        "date": str(e.date),
        "category": e.category,
        "description": e.description,
        "amount": e.amount_cents / 100,
        "is_deductible": e.is_deductible,
    }


@router.delete("/{property_id}/expenses/{expense_id}")
def delete_expense(
    property_id: int,
    expense_id: int,
    session: Session = Depends(get_session),
    current_user: User = Depends(get_current_user),
):
    e = session.get(PropertyExpense, expense_id)
    if not e or e.property_id != property_id or e.user_id != current_user.id:
        raise HTTPException(404, "Expense not found")
    session.delete(e)
    session.commit()
    return {"ok": True}


@router.get("/{property_id}/summary")
def property_fy_summary(
    property_id: int,
    fy: Optional[int] = None,
    session: Session = Depends(get_session),
    current_user: User = Depends(get_current_user),
):
    """FY tax summary for a property — income, deductible expenses, net rental income."""
    p = session.get(Property, property_id)
    if not p or p.user_id != current_user.id:
        raise HTTPException(404, "Property not found")

    if not fy:
        today = date.today()
        fy = today.year + 1 if today.month >= 7 else today.year
    start, end = _fy_dates(fy)

    expenses = session.exec(
        select(PropertyExpense).where(
            PropertyExpense.property_id == property_id,
            PropertyExpense.user_id == current_user.id,
            PropertyExpense.date >= start,
            PropertyExpense.date <= end,
        )
    ).all()

    # Rent income from logged items (category = rent_income, amount < 0 by convention OR amount > 0 as income)
    rent_income = sum(e.amount_cents for e in expenses if e.category == "rent_income") / 100
    # Estimated rent income from property's weekly rate
    weeks_elapsed = max(0, min(52, (min(end, date.today()) - start).days // 7))
    estimated_rent = (p.weekly_rent_cents / 100) * weeks_elapsed

    deductible_total = sum(e.amount_cents for e in expenses if e.is_deductible and e.category != "rent_income") / 100
    total_expenses = sum(e.amount_cents for e in expenses if e.category != "rent_income") / 100

    # Breakdown by category
    by_category: dict[str, float] = {}
    for e in expenses:
        if e.category != "rent_income":
            by_category[e.category] = by_category.get(e.category, 0) + e.amount_cents / 100

    # Net rental income (negative = negatively geared)
    effective_rent = rent_income if rent_income > 0 else estimated_rent
    net_rental_income = round(effective_rent - deductible_total, 2)
    negatively_geared = net_rental_income < 0

    return {
        "fy": f"{fy-1}–{str(fy)[2:]}",
        "fy_year": fy,
        "property_id": property_id,
        "address": p.address,
        "rent_income_logged": round(rent_income, 2),
        "rent_income_estimated": round(estimated_rent, 2),
        "weeks_elapsed": weeks_elapsed,
        "deductible_total": round(deductible_total, 2),
        "total_expenses_logged": round(total_expenses, 2),
        "net_rental_income": net_rental_income,
        "negatively_geared": negatively_geared,
        "expense_count": len([e for e in expenses if e.category != "rent_income"]),
        "by_category": {k: round(v, 2) for k, v in sorted(by_category.items(), key=lambda x: -x[1])},
    }


@router.get("/portfolio-summary")
def portfolio_summary(
    session: Session = Depends(get_session),
    current_user: User = Depends(get_current_user),
):
    """Aggregate across all active properties."""
    props = session.exec(
        select(Property).where(
            Property.user_id == current_user.id,
            Property.is_active == True,
        )
    ).all()

    total_value = sum(p.current_value_cents for p in props) / 100
    total_purchase = sum(p.purchase_price_cents for p in props) / 100
    total_mortgage = sum(p.mortgage_outstanding_cents for p in props) / 100
    total_equity = total_value - total_mortgage
    total_annual_rent = sum(p.weekly_rent_cents * 52 for p in props) / 100
    portfolio_yield = round(total_annual_rent / total_value * 100, 2) if total_value > 0 else 0.0
    capital_growth = total_value - total_purchase
    capital_growth_pct = round(capital_growth / total_purchase * 100, 1) if total_purchase > 0 else 0.0

    return {
        "count": len(props),
        "total_value": round(total_value, 2),
        "total_purchase_price": round(total_purchase, 2),
        "total_mortgage": round(total_mortgage, 2),
        "total_equity": round(total_equity, 2),
        "total_annual_rent": round(total_annual_rent, 2),
        "portfolio_yield": portfolio_yield,
        "capital_growth": round(capital_growth, 2),
        "capital_growth_pct": capital_growth_pct,
    }
