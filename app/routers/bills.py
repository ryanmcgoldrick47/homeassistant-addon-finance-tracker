from __future__ import annotations

from datetime import date, timedelta
from typing import Optional
import httpx

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel
from sqlmodel import Session, select, func

import re
from database import Bill, BillPayment, Transaction, Category, Setting, RecurringPattern, get_session, User
from deps import get_setting, get_current_user

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
def list_bills(
    active_only: bool = True,
    session: Session = Depends(get_session),
    current_user: User = Depends(get_current_user),
):
    stmt = select(Bill).where(Bill.user_id == current_user.id)
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
def upcoming_bills(
    days: int = 30,
    session: Session = Depends(get_session),
    current_user: User = Depends(get_current_user),
):
    cutoff = date.today() + timedelta(days=days)
    bills = session.exec(
        select(Bill).where(
            Bill.user_id == current_user.id,
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
def create_bill(
    body: BillCreate,
    session: Session = Depends(get_session),
    current_user: User = Depends(get_current_user),
):
    b = Bill(**body.model_dump(), user_id=current_user.id)
    session.add(b)
    session.commit()
    session.refresh(b)
    return b


@router.patch("/{bill_id}")
def update_bill(
    bill_id: int,
    body: BillUpdate,
    session: Session = Depends(get_session),
    current_user: User = Depends(get_current_user),
):
    b = session.get(Bill, bill_id)
    if not b:
        raise HTTPException(status_code=404, detail="Not found")
    if b.user_id != current_user.id:
        raise HTTPException(status_code=403, detail="Access denied")
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
def mark_paid(
    bill_id: int,
    body: PaidBody = PaidBody(),
    session: Session = Depends(get_session),
    current_user: User = Depends(get_current_user),
):
    """Record a payment and advance next_due by frequency interval."""
    b = session.get(Bill, bill_id)
    if not b:
        raise HTTPException(status_code=404, detail="Not found")
    if b.user_id != current_user.id:
        raise HTTPException(status_code=403, detail="Access denied")

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
def bill_history(
    bill_id: int,
    limit: int = 24,
    session: Session = Depends(get_session),
    current_user: User = Depends(get_current_user),
):
    """Return recent payment history for a bill."""
    b = session.get(Bill, bill_id)
    if not b:
        raise HTTPException(status_code=404, detail="Not found")
    if b.user_id != current_user.id:
        raise HTTPException(status_code=403, detail="Access denied")
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


@router.get("/price-changes")
def detect_price_changes(
    session: Session = Depends(get_session),
    current_user: User = Depends(get_current_user),
):
    """
    Compare each bill's last 2 payments. Flag where the most recent payment
    differs from the previous average by more than 5%.
    """
    bills = session.exec(
        select(Bill).where(Bill.user_id == current_user.id, Bill.is_active == True)
    ).all()

    changes = []
    for b in bills:
        payments = session.exec(
            select(BillPayment)
            .where(BillPayment.bill_id == b.id)
            .order_by(BillPayment.paid_date.desc())
            .limit(6)
        ).all()
        if len(payments) < 2:
            continue
        latest = payments[0].amount_cents
        prior_avg = sum(p.amount_cents for p in payments[1:]) / len(payments[1:])
        if prior_avg == 0:
            continue
        pct_change = (latest - prior_avg) / prior_avg * 100
        if abs(pct_change) >= 5:
            changes.append({
                "bill_id":      b.id,
                "bill_name":    b.name,
                "frequency":    b.frequency,
                "latest_amount":   round(latest / 100, 2),
                "previous_avg":    round(prior_avg / 100, 2),
                "pct_change":      round(pct_change, 1),
                "increased":       pct_change > 0,
                "latest_date":     str(payments[0].paid_date),
            })

    changes.sort(key=lambda x: abs(x["pct_change"]), reverse=True)
    return {"changes": changes}


@router.get("/predict-due-dates")
def predict_due_dates(
    session: Session = Depends(get_session),
    current_user: User = Depends(get_current_user),
):
    """
    For each active bill with no next_due, look for a matching RecurringPattern
    (linked by bill_id or by name similarity) and predict next_due from
    last_date + frequency interval.  Returns predictions the user can confirm.
    """
    import statistics as _stats

    bills = session.exec(
        select(Bill).where(Bill.user_id == current_user.id, Bill.is_active == True)
    ).all()

    # Also look at transactions directly for bills that have no pattern yet
    # Build: bill_id -> pattern (from RecurringPattern table)
    patterns = session.exec(
        select(RecurringPattern).where(
            RecurringPattern.user_id == current_user.id,
            RecurringPattern.bill_id != None,
        )
    ).all()
    pattern_by_bill: dict[int, RecurringPattern] = {p.bill_id: p for p in patterns}

    today = date.today()
    predictions = []

    for b in bills:
        pat = pattern_by_bill.get(b.id)

        # If no linked pattern, try matching by transactions directly
        if not pat:
            txns = session.exec(
                select(Transaction)
                .where(
                    Transaction.user_id == current_user.id,
                    Transaction.is_credit == False,
                    Transaction.description.ilike(f"%{b.name[:15].strip()}%"),
                )
                .order_by(Transaction.date)
            ).all()
            if len(txns) >= 2:
                dates_sorted = sorted(t.date for t in txns)
                intervals = [(dates_sorted[i+1] - dates_sorted[i]).days for i in range(len(dates_sorted)-1)]
                avg_interval = sum(intervals) / len(intervals)
                # Determine matching frequency
                matched_freq = None
                for freq, days in FREQ_DAYS.items():
                    if abs(avg_interval - days) / days < 0.2:
                        matched_freq = freq
                        break
                if matched_freq:
                    interval_cv = (_stats.stdev(intervals) / avg_interval) if len(intervals) > 1 else 0
                    occ_score = min(len(txns) / 6, 1.0)
                    interval_score = max(0, 1 - interval_cv * 2)
                    confidence = round(occ_score * 0.5 + interval_score * 0.5, 2)
                    last_date = dates_sorted[-1]
                    predicted = last_date + timedelta(days=FREQ_DAYS[matched_freq])
                    # Advance past today if prediction is in the past
                    while predicted < today:
                        predicted += timedelta(days=FREQ_DAYS[matched_freq])
                    predictions.append({
                        "bill_id": b.id,
                        "bill_name": b.name,
                        "current_next_due": str(b.next_due) if b.next_due else None,
                        "predicted_next_due": str(predicted),
                        "frequency": matched_freq,
                        "confidence": confidence,
                        "last_seen": str(last_date),
                        "occurrences": len(txns),
                        "source": "transactions",
                    })
            continue

        # Use linked pattern
        if pat.last_date:
            predicted = pat.last_date + timedelta(days=FREQ_DAYS.get(pat.frequency, 30))
            # Advance past today if prediction is in the past
            while predicted < today:
                predicted += timedelta(days=FREQ_DAYS.get(pat.frequency, 30))
            predictions.append({
                "bill_id": b.id,
                "bill_name": b.name,
                "current_next_due": str(b.next_due) if b.next_due else None,
                "predicted_next_due": str(predicted),
                "frequency": pat.frequency,
                "confidence": pat.confidence,
                "last_seen": str(pat.last_date),
                "occurrences": pat.occurrences,
                "source": "recurring_pattern",
            })

    predictions.sort(key=lambda x: x["confidence"], reverse=True)
    return {"predictions": predictions}


@router.post("/apply-predicted-due")
def apply_predicted_due(
    data: dict,
    session: Session = Depends(get_session),
    current_user: User = Depends(get_current_user),
):
    """Apply a predicted next_due date to one or all bills at once."""
    bill_ids = data.get("bill_ids")  # list of {bill_id, predicted_next_due}
    if not bill_ids:
        return {"updated": 0}
    updated = 0
    for item in bill_ids:
        b = session.get(Bill, item["bill_id"])
        if b and b.user_id == current_user.id:
            from datetime import date as _date
            b.next_due = _date.fromisoformat(item["predicted_next_due"])
            if item.get("frequency"):
                b.frequency = item["frequency"]
            session.add(b)
            updated += 1
    session.commit()
    return {"updated": updated}


@router.delete("/{bill_id}")
def delete_bill(
    bill_id: int,
    session: Session = Depends(get_session),
    current_user: User = Depends(get_current_user),
):
    b = session.get(Bill, bill_id)
    if not b:
        raise HTTPException(status_code=404, detail="Not found")
    if b.user_id != current_user.id:
        raise HTTPException(status_code=403, detail="Access denied")
    session.delete(b)
    session.commit()
    return {"ok": True}


@router.post("/notify")
async def notify_upcoming(
    session: Session = Depends(get_session),
    current_user: User = Depends(get_current_user),
):
    """Fire HA webhook for bills due within 7 days."""
    webhook_id = get_setting(session, "ha_webhook_id")
    ha_url = get_setting(session, "ha_url", "http://supervisor/core")
    if not webhook_id:
        return {"ok": False, "reason": "No webhook ID configured"}

    upcoming = session.exec(
        select(Bill).where(
            Bill.user_id == current_user.id,
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


def _norm_key(desc: str) -> str:
    """Normalise a transaction description for recurring grouping.
    Strips numbers, reference codes, dates, and punctuation — leaving the merchant stem.
    """
    s = desc.strip().upper()
    # Remove trailing number-heavy tokens (transaction IDs, dates, amounts)
    s = re.sub(r'\b\d[\d*]{3,}\b', '', s)      # 4+ digit / masked numbers
    s = re.sub(r'\d{1,2}[/\-]\d{1,2}([/\-]\d{2,4})?', '', s)  # dates
    s = re.sub(r'[^A-Z\s&]', '', s)              # keep only letters, spaces, &
    s = re.sub(r'\s+', ' ', s).strip()
    return s[:40] if s else desc.strip().upper()[:40]


@router.post("/detect-recurring")
def detect_recurring(
    session: Session = Depends(get_session),
    current_user: User = Depends(get_current_user),
):
    """
    Scan transactions for recurring patterns. Stores results in RecurringPattern.
    Returns all patterns (suggested + confirmed + dismissed) for the user.
    """
    from collections import defaultdict
    import statistics

    txns = session.exec(
        select(Transaction)
        .where(
            Transaction.user_id == current_user.id,
            Transaction.is_credit == False,
        )
        .order_by(Transaction.description, Transaction.date)
    ).all()

    # Group by normalised key
    groups: dict[str, list[Transaction]] = defaultdict(list)
    for t in txns:
        key = _norm_key(t.description)
        groups[key].append(t)

    # Load existing patterns to preserve confirmed/dismissed status
    existing_patterns: dict[str, RecurringPattern] = {
        p.norm_key: p
        for p in session.exec(
            select(RecurringPattern).where(RecurringPattern.user_id == current_user.id)
        ).all()
    }

    upserted_keys: set[str] = set()

    for norm_key, txn_list in groups.items():
        if len(txn_list) < 2:
            continue

        amounts = [t.amount for t in txn_list]
        avg_amount = statistics.mean(amounts)
        amount_cv = statistics.stdev(amounts) / avg_amount if len(amounts) > 1 else 0

        # Amount tolerance: fixed subscriptions 5%, variable services 25%
        if amount_cv > 0.30:
            continue

        dates = sorted(t.date for t in txn_list)
        intervals = [(dates[i+1] - dates[i]).days for i in range(len(dates)-1)]
        # Skip if all transactions on same date (interval = 0)
        if not intervals or max(intervals) == 0:
            continue
        avg_interval = statistics.mean(intervals)
        if avg_interval == 0:
            continue
        interval_cv = statistics.stdev(intervals) / avg_interval if len(intervals) > 1 else 0

        # Determine frequency
        if 6 <= avg_interval <= 8:
            freq = "weekly"
        elif 13 <= avg_interval <= 16:
            freq = "fortnightly"
        elif 26 <= avg_interval <= 35:
            freq = "monthly"
        elif 85 <= avg_interval <= 100:
            freq = "quarterly"
        elif 355 <= avg_interval <= 380:
            freq = "annual"
        else:
            continue

        # Confidence: more occurrences + consistent intervals + consistent amounts = higher
        occ_score     = min(txn_list.__len__() / 6, 1.0)   # caps at 6 occurrences
        interval_score = max(0, 1 - interval_cv * 2)
        amount_score   = max(0, 1 - amount_cv * 3)
        confidence = round((occ_score * 0.4 + interval_score * 0.35 + amount_score * 0.25), 2)

        display = txn_list[-1].description  # use most recent as display name

        existing = existing_patterns.get(norm_key)
        if existing:
            # Preserve confirmed/dismissed status; just refresh stats
            existing.avg_amount  = round(avg_amount, 2)
            existing.frequency   = freq
            existing.occurrences = len(txn_list)
            existing.last_date   = dates[-1]
            existing.confidence  = confidence
            existing.display_name = display
            session.add(existing)
        else:
            # New pattern — check if already a bill (don't re-suggest)
            already_bill = session.exec(
                select(Bill).where(
                    Bill.user_id == current_user.id,
                    Bill.name.ilike(f"%{display[:20]}%"),
                )
            ).first()
            status = "confirmed" if already_bill else "suggested"
            bill_id = already_bill.id if already_bill else None

            session.add(RecurringPattern(
                norm_key=norm_key,
                display_name=display,
                avg_amount=round(avg_amount, 2),
                frequency=freq,
                occurrences=len(txn_list),
                last_date=dates[-1],
                confidence=confidence,
                status=status,
                bill_id=bill_id,
                user_id=current_user.id,
            ))

        upserted_keys.add(norm_key)

    session.commit()

    # Return all patterns for this user
    patterns = session.exec(
        select(RecurringPattern)
        .where(RecurringPattern.user_id == current_user.id)
        .order_by(RecurringPattern.confidence.desc())
    ).all()

    return {
        "patterns": [_pattern_dict(p) for p in patterns],
        "new_suggestions": sum(1 for p in patterns if p.status == "suggested"),
    }


@router.get("/recurring")
def list_recurring(
    session: Session = Depends(get_session),
    current_user: User = Depends(get_current_user),
):
    """Return all stored recurring patterns."""
    patterns = session.exec(
        select(RecurringPattern)
        .where(RecurringPattern.user_id == current_user.id)
        .order_by(RecurringPattern.confidence.desc())
    ).all()
    return {"patterns": [_pattern_dict(p) for p in patterns]}


@router.patch("/recurring/{pattern_id}")
def update_recurring_status(
    pattern_id: int,
    data: dict,
    session: Session = Depends(get_session),
    current_user: User = Depends(get_current_user),
):
    """Set status to 'confirmed' or 'dismissed'."""
    p = session.get(RecurringPattern, pattern_id)
    if not p or p.user_id != current_user.id:
        raise HTTPException(404, "Pattern not found")
    status = data.get("status")
    if status not in ("confirmed", "dismissed", "suggested"):
        raise HTTPException(400, "status must be confirmed, dismissed, or suggested")
    p.status = status
    session.add(p)
    session.commit()
    return _pattern_dict(p)


def _pattern_dict(p: RecurringPattern) -> dict:
    return {
        "id": p.id,
        "norm_key": p.norm_key,
        "display_name": p.display_name,
        "avg_amount": p.avg_amount,
        "frequency": p.frequency,
        "occurrences": p.occurrences,
        "last_date": str(p.last_date) if p.last_date else None,
        "confidence": p.confidence,
        "status": p.status,
        "bill_id": p.bill_id,
    }
