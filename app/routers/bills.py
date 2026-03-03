from __future__ import annotations

from datetime import date, timedelta
from typing import Optional
import httpx

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel
from sqlmodel import Session, select, func

from database import Bill, BillPayment, Transaction, Category, Setting, get_session
from deps import get_setting

router = APIRouter(prefix="/api/bills", tags=["bills"])

FREQ_DAYS = {
    "weekly": 7,
    "fortnightly": 14,
    "monthly": 30,
    "quarterly": 91,
    "annual": 365,
}

FREQ_PER_YEAR = {
    "weekly": 52,
    "fortnightly": 26,
    "monthly": 12,
    "quarterly": 4,
    "annual": 1,
}


class BillCreate(BaseModel):
    name: str
    amount_cents: int
    frequency: str = "monthly"
    next_due: Optional[date] = None
    category_id: Optional[int] = None


class BillUpdate(BaseModel):
    name: Optional[str] = None
    amount_cents: Optional[int] = None
    frequency: Optional[str] = None
    next_due: Optional[date] = None
    is_active: Optional[bool] = None
    category_id: Optional[int] = None


@router.get("")
def list_bills(active_only: bool = True, session: Session = Depends(get_session)):
    stmt = select(Bill)
    if active_only:
        stmt = stmt.where(Bill.is_active == True)
    bills = session.exec(stmt.order_by(Bill.next_due)).all()
    result = []
    for b in bills:
        cat = session.get(Category, b.category_id) if b.category_id else None
        days_until = None
        if b.next_due:
            days_until = (b.next_due - date.today()).days
        annual_cost = round(b.amount_cents * FREQ_PER_YEAR.get(b.frequency, 12) / 100, 2)
        result.append({
            **b.model_dump(),
            "category_name": cat.name if cat else None,
            "amount": b.amount_cents / 100,
            "annual_cost": annual_cost,
            "days_until": days_until,
            "overdue": days_until is not None and days_until < 0,
        })
    return result


@router.get("/upcoming")
def upcoming_bills(days: int = 30, session: Session = Depends(get_session)):
    cutoff = date.today() + timedelta(days=days)
    bills = session.exec(
        select(Bill).where(
            Bill.is_active == True,
            Bill.next_due != None,
            Bill.next_due <= cutoff,
        ).order_by(Bill.next_due)
    ).all()
    return [
        {
            **b.model_dump(),
            "amount": b.amount_cents / 100,
            "days_until": (b.next_due - date.today()).days,
        }
        for b in bills
    ]


@router.post("")
def create_bill(body: BillCreate, session: Session = Depends(get_session)):
    b = Bill(**body.model_dump())
    session.add(b)
    session.commit()
    session.refresh(b)
    return b


@router.patch("/{bill_id}")
def update_bill(bill_id: int, body: BillUpdate, session: Session = Depends(get_session)):
    b = session.get(Bill, bill_id)
    if not b:
        raise HTTPException(status_code=404, detail="Not found")
    for k, v in body.model_dump(exclude_none=True).items():
        setattr(b, k, v)
    session.add(b)
    session.commit()
    session.refresh(b)
    return b


class PaidBody(BaseModel):
    amount_cents: Optional[int] = None  # actual amount paid; defaults to bill amount
    notes: Optional[str] = None
    paid_date: Optional[date] = None    # defaults to today


@router.post("/{bill_id}/paid")
def mark_paid(bill_id: int, body: PaidBody = PaidBody(), session: Session = Depends(get_session)):
    """Record a payment and advance next_due by frequency interval."""
    b = session.get(Bill, bill_id)
    if not b:
        raise HTTPException(status_code=404, detail="Not found")

    paid_date = body.paid_date or date.today()
    amount_cents = body.amount_cents if body.amount_cents is not None else b.amount_cents

    # Record payment history
    payment = BillPayment(
        bill_id=bill_id,
        paid_date=paid_date,
        amount_cents=amount_cents,
        notes=body.notes,
    )
    session.add(payment)

    # Advance next_due
    if b.next_due:
        delta = timedelta(days=FREQ_DAYS.get(b.frequency, 30))
        b.next_due = b.next_due + delta
    session.add(b)
    session.commit()
    session.refresh(b)
    return b


@router.get("/{bill_id}/history")
def bill_history(bill_id: int, limit: int = 24, session: Session = Depends(get_session)):
    """Return recent payment history for a bill."""
    b = session.get(Bill, bill_id)
    if not b:
        raise HTTPException(status_code=404, detail="Not found")
    payments = session.exec(
        select(BillPayment)
        .where(BillPayment.bill_id == bill_id)
        .order_by(BillPayment.paid_date.desc())
        .limit(limit)
    ).all()
    total_paid = sum(p.amount_cents for p in payments)
    return {
        "bill_name": b.name,
        "payments": [
            {
                "id": p.id,
                "paid_date": str(p.paid_date),
                "amount": round(p.amount_cents / 100, 2),
                "amount_cents": p.amount_cents,
                "notes": p.notes,
                "diff_cents": p.amount_cents - b.amount_cents,
            }
            for p in payments
        ],
        "total_paid": round(total_paid / 100, 2),
        "payment_count": len(payments),
    }


@router.delete("/{bill_id}")
def delete_bill(bill_id: int, session: Session = Depends(get_session)):
    b = session.get(Bill, bill_id)
    if not b:
        raise HTTPException(status_code=404, detail="Not found")
    session.delete(b)
    session.commit()
    return {"ok": True}


@router.post("/notify")
async def notify_upcoming(session: Session = Depends(get_session)):
    """Fire HA webhook for bills due within 7 days."""
    webhook_id = get_setting(session, "ha_webhook_id")
    ha_url = get_setting(session, "ha_url", "http://supervisor/core")
    if not webhook_id:
        return {"ok": False, "reason": "No webhook ID configured"}

    upcoming = session.exec(
        select(Bill).where(
            Bill.is_active == True,
            Bill.next_due != None,
            Bill.next_due <= date.today() + timedelta(days=7),
        )
    ).all()

    if not upcoming:
        return {"ok": True, "sent": 0}

    payload = {
        "bills": [
            {"name": b.name, "amount": b.amount_cents / 100, "due": str(b.next_due)}
            for b in upcoming
        ]
    }

    async with httpx.AsyncClient() as client:
        resp = await client.post(
            f"{ha_url}/api/webhook/{webhook_id}",
            json=payload,
            timeout=10,
        )
    return {"ok": resp.status_code == 200, "sent": len(upcoming)}


@router.post("/detect-recurring")
def detect_recurring(session: Session = Depends(get_session)):
    """
    Scan transactions for recurring patterns and suggest bills.
    Groups by merchant name, checks for consistent amounts and intervals.
    """
    from collections import defaultdict
    import statistics

    txns = session.exec(
        select(Transaction)
        .where(Transaction.is_credit == False)
        .order_by(Transaction.description, Transaction.date)
    ).all()

    # Group by normalised merchant
    groups: dict[str, list[Transaction]] = defaultdict(list)
    for t in txns:
        key = t.description.strip().upper()[:40]
        groups[key].append(t)

    suggestions = []
    for merchant, txn_list in groups.items():
        if len(txn_list) < 2:
            continue

        amounts = [t.amount for t in txn_list]
        avg_amount = statistics.mean(amounts)
        # Check amounts are consistent (within 10%)
        if max(amounts) / avg_amount > 1.15 or min(amounts) / avg_amount < 0.85:
            continue

        dates = sorted(t.date for t in txn_list)
        if len(dates) < 2:
            continue

        intervals = [(dates[i+1] - dates[i]).days for i in range(len(dates)-1)]
        avg_interval = statistics.mean(intervals)

        # Determine frequency
        if 6 <= avg_interval <= 8:
            freq = "weekly"
        elif 13 <= avg_interval <= 15:
            freq = "fortnightly"
        elif 28 <= avg_interval <= 35:
            freq = "monthly"
        elif 88 <= avg_interval <= 95:
            freq = "quarterly"
        elif 360 <= avg_interval <= 370:
            freq = "annual"
        else:
            continue

        # Check not already a bill
        existing = session.exec(
            select(Bill).where(Bill.name.ilike(f"%{txn_list[0].description[:20]}%"))
        ).first()
        if existing:
            continue

        suggestions.append({
            "merchant": txn_list[0].description,
            "avg_amount": round(avg_amount, 2),
            "frequency": freq,
            "occurrences": len(txn_list),
            "last_date": str(dates[-1]),
        })

    return {"suggestions": suggestions}
