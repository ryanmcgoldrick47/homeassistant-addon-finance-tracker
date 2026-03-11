from __future__ import annotations

import csv
import hashlib
import io
from datetime import date
from typing import Optional, List
from fastapi import APIRouter, Depends, HTTPException, Query
from fastapi.responses import StreamingResponse
from sqlmodel import Session, select, func
from pydantic import BaseModel

from datetime import timedelta
from database import Transaction, Category, Account, Bill, MerchantEnrichment, Payslip, get_session, User
from deps import get_setting, get_current_user

router = APIRouter(prefix="/api/transactions", tags=["transactions"])


class TransactionCreate(BaseModel):
    account_id: Optional[int] = None
    date: date
    description: str
    amount: float
    is_credit: bool = False
    category_id: Optional[int] = None
    notes: Optional[str] = None
    tax_deductible: bool = False


class TransactionUpdate(BaseModel):
    category_id: Optional[int] = None
    is_flagged: Optional[bool] = None
    is_reviewed: Optional[bool] = None
    tax_deductible: Optional[bool] = None
    tax_category: Optional[str] = None
    notes: Optional[str] = None
    is_reimbursable: Optional[bool] = None
    reimbursement_received: Optional[bool] = None


class BulkUpdate(BaseModel):
    ids: List[int]
    category_id: Optional[int] = None
    is_reviewed: Optional[bool] = None
    is_flagged: Optional[bool] = None
    tax_deductible: Optional[bool] = None
    is_reimbursable: Optional[bool] = None
    delete: bool = False


@router.get("")
def list_transactions(
    account_id: Optional[int] = None,
    category_id: Optional[int] = None,
    is_flagged: Optional[bool] = None,
    is_reviewed: Optional[bool] = None,
    tax_deductible: Optional[bool] = None,
    is_overseas: Optional[bool] = None,
    is_credit: Optional[bool] = None,
    month: Optional[int] = None,
    year: Optional[int] = None,
    fy: Optional[int] = None,   # financial year ending, e.g. 2025 = Jul24–Jun25
    date_from: Optional[date] = None,
    date_to: Optional[date] = None,
    amount_min: Optional[float] = None,
    amount_max: Optional[float] = None,
    search: Optional[str] = None,
    limit: int = Query(default=200, le=1000),
    offset: int = 0,
    session: Session = Depends(get_session),
    current_user: User = Depends(get_current_user),
):
    stmt = select(Transaction).where(Transaction.user_id == current_user.id).order_by(Transaction.date.desc())

    if account_id:
        stmt = stmt.where(Transaction.account_id == account_id)
    if category_id is not None:
        stmt = stmt.where(Transaction.category_id == category_id)
    if is_flagged is not None:
        stmt = stmt.where(Transaction.is_flagged == is_flagged)
    if is_reviewed is not None:
        stmt = stmt.where(Transaction.is_reviewed == is_reviewed)
    if tax_deductible is not None:
        stmt = stmt.where(Transaction.tax_deductible == tax_deductible)
    if is_overseas is not None:
        stmt = stmt.where(Transaction.is_overseas == is_overseas)
    if is_credit is not None:
        stmt = stmt.where(Transaction.is_credit == is_credit)
    if date_from:
        stmt = stmt.where(Transaction.date >= date_from)
    if date_to:
        stmt = stmt.where(Transaction.date <= date_to)
    if amount_min is not None:
        stmt = stmt.where(Transaction.amount >= amount_min)
    if amount_max is not None:
        stmt = stmt.where(Transaction.amount <= amount_max)
    if month and year:
        stmt = stmt.where(
            func.strftime("%m", Transaction.date) == f"{month:02d}",
            func.strftime("%Y", Transaction.date) == str(year),
        )
    if fy:
        # FY ending June <fy>: Jul <fy-1> – Jun <fy>
        start = date(fy - 1, 7, 1)
        end = date(fy, 6, 30)
        stmt = stmt.where(Transaction.date >= start, Transaction.date <= end)
    if search:
        stmt = stmt.where(Transaction.description.ilike(f"%{search}%"))

    total = session.exec(select(func.count()).select_from(stmt.subquery())).one()
    txns = session.exec(stmt.offset(offset).limit(limit)).all()

    # Batch-fetch merchant enrichments for displayed transactions
    raw_keys = {t.description.strip().upper()[:50] for t in txns}
    enrichments: dict[str, MerchantEnrichment] = {}
    if raw_keys:
        for e in session.exec(
            select(MerchantEnrichment).where(MerchantEnrichment.raw_key.in_(raw_keys))
        ).all():
            enrichments[e.raw_key] = e

    result = []
    for t in txns:
        cat = session.get(Category, t.category_id) if t.category_id else None
        acc = session.get(Account, t.account_id) if t.account_id else None
        enrich = enrichments.get(t.description.strip().upper()[:50])
        result.append({
            **t.model_dump(),
            "category_name": cat.name if cat else None,
            "category_colour": cat.colour if cat else "#d1d5db",
            "account_name": acc.name if acc else None,
            "clean_name": enrich.clean_name if enrich else None,
            "logo_domain": enrich.domain if enrich else None,
        })

    return {"total": total, "items": result}


@router.post("")
def create_transaction(
    body: TransactionCreate,
    session: Session = Depends(get_session),
    current_user: User = Depends(get_current_user),
):
    raw = f"{body.date}|{body.description}|{body.amount:.2f}|manual"
    raw_hash = hashlib.sha256(raw.encode()).hexdigest()
    txn = Transaction(
        account_id=body.account_id or None,
        date=body.date,
        description=body.description,
        amount=abs(body.amount),
        is_credit=body.is_credit,
        category_id=body.category_id or None,
        notes=body.notes or None,
        tax_deductible=body.tax_deductible,
        is_reviewed=True,  # manually entered = already reviewed
        raw_hash=raw_hash,
        user_id=current_user.id,
    )
    session.add(txn)
    session.commit()
    session.refresh(txn)
    return txn


@router.get("/summary")
def spend_summary(
    month: Optional[int] = None,
    year: Optional[int] = None,
    fy: Optional[int] = None,
    session: Session = Depends(get_session),
    current_user: User = Depends(get_current_user),
):
    """Spend by category for a period."""
    stmt = select(Transaction).where(Transaction.user_id == current_user.id)
    if month and year:
        stmt = stmt.where(
            func.strftime("%m", Transaction.date) == f"{month:02d}",
            func.strftime("%Y", Transaction.date) == str(year),
        )
    if fy:
        start = date(fy - 1, 7, 1)
        end = date(fy, 6, 30)
        stmt = stmt.where(Transaction.date >= start, Transaction.date <= end)

    txns = session.exec(stmt).all()
    by_cat: dict[str, float] = {}
    for t in txns:
        if t.is_credit:
            continue
        cat = session.get(Category, t.category_id) if t.category_id else None
        cat_name = cat.name if cat else "Uncategorised"
        by_cat[cat_name] = by_cat.get(cat_name, 0) + t.amount

    return {"by_category": by_cat}


@router.get("/summary/recent-month")
def recent_month(
    session: Session = Depends(get_session),
    current_user: User = Depends(get_current_user),
):
    """Return the most recent month/year that has transactions."""
    row = session.exec(
        select(
            func.strftime("%m", Transaction.date),
            func.strftime("%Y", Transaction.date),
        ).where(Transaction.user_id == current_user.id).order_by(Transaction.date.desc()).limit(1)
    ).first()
    if not row:
        from datetime import date as dt
        return {"month": dt.today().month, "year": dt.today().year}
    return {"month": int(row[0]), "year": int(row[1])}


@router.get("/export")
def export_transactions(
    account_id: Optional[int] = None,
    category_id: Optional[int] = None,
    is_flagged: Optional[bool] = None,
    is_reviewed: Optional[bool] = None,
    tax_deductible: Optional[bool] = None,
    month: Optional[int] = None,
    year: Optional[int] = None,
    fy: Optional[int] = None,
    search: Optional[str] = None,
    session: Session = Depends(get_session),
    current_user: User = Depends(get_current_user),
):
    stmt = select(Transaction).where(Transaction.user_id == current_user.id).order_by(Transaction.date.desc())
    if account_id:
        stmt = stmt.where(Transaction.account_id == account_id)
    if category_id is not None:
        stmt = stmt.where(Transaction.category_id == category_id)
    if is_flagged is not None:
        stmt = stmt.where(Transaction.is_flagged == is_flagged)
    if is_reviewed is not None:
        stmt = stmt.where(Transaction.is_reviewed == is_reviewed)
    if tax_deductible is not None:
        stmt = stmt.where(Transaction.tax_deductible == tax_deductible)
    if month and year:
        stmt = stmt.where(
            func.strftime("%m", Transaction.date) == f"{month:02d}",
            func.strftime("%Y", Transaction.date) == str(year),
        )
    if fy:
        start = date(fy - 1, 7, 1)
        end = date(fy, 6, 30)
        stmt = stmt.where(Transaction.date >= start, Transaction.date <= end)
    if search:
        stmt = stmt.where(Transaction.description.ilike(f"%{search}%"))

    txns = session.exec(stmt).all()

    buf = io.StringIO()
    writer = csv.writer(buf)
    writer.writerow(['Date', 'Description', 'Amount', 'Type', 'Category', 'Account',
                     'Tax Deductible', 'Tax Category', 'Notes'])
    for t in txns:
        cat = session.get(Category, t.category_id) if t.category_id else None
        acc = session.get(Account, t.account_id) if t.account_id else None
        signed = t.amount if t.is_credit else -t.amount
        writer.writerow([
            t.date,
            t.description,
            f"{signed:.2f}",
            'Credit' if t.is_credit else 'Debit',
            cat.name if cat else '',
            acc.name if acc else '',
            'Yes' if t.tax_deductible else 'No',
            t.tax_category or '',
            t.notes or '',
        ])

    filename = "transactions"
    if month and year:
        filename += f"_{year}_{month:02d}"
    elif fy:
        filename += f"_FY{fy}"
    filename += ".csv"

    return StreamingResponse(
        io.BytesIO(buf.getvalue().encode('utf-8-sig')),  # BOM for Excel compatibility
        media_type='text/csv',
        headers={'Content-Disposition': f'attachment; filename="{filename}"'},
    )


@router.get("/upcoming")
def upcoming_transactions(
    days: int = Query(default=60, le=365),
    session: Session = Depends(get_session),
    current_user: User = Depends(get_current_user),
):
    """Return predicted upcoming transactions for the next N days, derived from active bill schedules."""
    from datetime import timedelta
    FREQ_DAYS = {"weekly": 7, "fortnightly": 14, "monthly": 30, "quarterly": 91, "annual": 365}

    today = date.today()
    cutoff = today + timedelta(days=days)

    bills = session.exec(
        select(Bill).where(
            Bill.user_id == current_user.id,
            Bill.is_active == True,
            Bill.next_due != None,
        )
    ).all()

    results = []
    cat_cache: dict[int, str] = {}
    for bill in bills:
        due = bill.next_due
        interval = timedelta(days=FREQ_DAYS.get(bill.frequency, 30))
        # Walk forward through bill schedule
        while due <= cutoff:
            if due >= today:
                cat_name = None
                if bill.category_id:
                    if bill.category_id not in cat_cache:
                        cat = session.get(Category, bill.category_id)
                        cat_cache[bill.category_id] = cat.name if cat else None
                    cat_name = cat_cache[bill.category_id]
                results.append({
                    "bill_id": bill.id,
                    "bill_name": bill.name,
                    "amount": round(bill.amount_cents / 100, 2),
                    "date": str(due),
                    "category_id": bill.category_id,
                    "category_name": cat_name,
                    "frequency": bill.frequency,
                    "is_predicted": True,
                })
            due = due + interval

    results.sort(key=lambda x: x["date"])
    return results


@router.get("/merchants")
def merchant_analytics(
    month: Optional[int] = None,
    year: Optional[int] = None,
    fy: Optional[int] = None,
    limit: int = Query(default=20, le=100),
    session: Session = Depends(get_session),
    current_user: User = Depends(get_current_user),
):
    stmt = select(Transaction).where(
        Transaction.user_id == current_user.id,
        Transaction.is_credit == False,
    )
    if month and year:
        stmt = stmt.where(
            func.strftime("%m", Transaction.date) == f"{month:02d}",
            func.strftime("%Y", Transaction.date) == str(year),
        )
    if fy:
        start = date(fy - 1, 7, 1)
        end = date(fy, 6, 30)
        stmt = stmt.where(Transaction.date >= start, Transaction.date <= end)

    txns = session.exec(stmt).all()

    merchants: dict[str, dict] = {}
    for t in txns:
        key = t.description.strip().upper()[:50]
        if key not in merchants:
            merchants[key] = {
                'raw_key': key,
                'name': t.description,
                'count': 0,
                'total': 0.0,
                'category_id': t.category_id,
            }
        merchants[key]['count'] += 1
        merchants[key]['total'] += t.amount

    # Bulk-load enrichments for all raw keys
    raw_keys = list(merchants.keys())
    enrichments: dict[str, MerchantEnrichment] = {}
    if raw_keys:
        for e in session.exec(select(MerchantEnrichment).where(MerchantEnrichment.raw_key.in_(raw_keys))).all():
            enrichments[e.raw_key] = e

    result = sorted(merchants.values(), key=lambda x: x['total'], reverse=True)[:limit]
    for m in result:
        cat = session.get(Category, m['category_id']) if m['category_id'] else None
        m['category_name'] = cat.name if cat else 'Uncategorised'
        m['category_colour'] = cat.colour if cat else '#d1d5db'
        m['avg'] = round(m['total'] / m['count'], 2)
        m['total'] = round(m['total'], 2)
        enr = enrichments.get(m['raw_key'])
        m['clean_name'] = enr.clean_name if enr and enr.clean_name else None
        m['domain'] = enr.domain if enr and enr.domain else None
        m['logo_url'] = f"https://www.google.com/s2/favicons?domain={enr.domain}&sz=64" if enr and enr.domain else None
        del m['category_id']
        del m['raw_key']

    return result


@router.patch("/bulk")
def bulk_update(
    body: BulkUpdate,
    session: Session = Depends(get_session),
    current_user: User = Depends(get_current_user),
):
    if not body.ids:
        return {"updated": 0, "deleted": 0}
    updated = 0
    deleted = 0
    for txn_id in body.ids:
        txn = session.get(Transaction, txn_id)
        if not txn or txn.user_id != current_user.id:
            continue
        if body.delete:
            session.delete(txn)
            deleted += 1
        else:
            if body.category_id is not None:
                txn.category_id = body.category_id
            if body.is_reviewed is not None:
                txn.is_reviewed = body.is_reviewed
            if body.is_flagged is not None:
                txn.is_flagged = body.is_flagged
            if body.tax_deductible is not None:
                txn.tax_deductible = body.tax_deductible
            if body.is_reimbursable is not None:
                txn.is_reimbursable = body.is_reimbursable
            session.add(txn)
            updated += 1
    session.commit()
    return {"updated": updated, "deleted": deleted}


@router.patch("/{txn_id}")
def update_transaction(
    txn_id: int,
    body: TransactionUpdate,
    session: Session = Depends(get_session),
    current_user: User = Depends(get_current_user),
):
    txn = session.get(Transaction, txn_id)
    if not txn:
        raise HTTPException(status_code=404, detail="Transaction not found")
    if txn.user_id != current_user.id:
        raise HTTPException(status_code=403, detail="Access denied")
    data = body.model_dump(exclude_none=True)
    for k, v in data.items():
        setattr(txn, k, v)
    session.add(txn)
    session.commit()
    session.refresh(txn)
    return txn


@router.get("/reimbursable-summary")
def reimbursable_summary(
    session: Session = Depends(get_session),
    current_user: User = Depends(get_current_user),
):
    """Summary of reimbursable work expenses: pending vs received, with transaction list."""
    txns = session.exec(
        select(Transaction).where(
            Transaction.user_id == current_user.id,
            Transaction.is_reimbursable == True,
        ).order_by(Transaction.date.desc())
    ).all()

    cats = {c.id: c for c in session.exec(select(Category)).all()}
    pending = []
    received = []
    for t in txns:
        cat = cats.get(t.category_id)
        row = {
            "id": t.id,
            "date": str(t.date),
            "description": t.description,
            "amount": float(t.amount),
            "category_name": cat.name if cat else "Uncategorised",
            "reimbursement_received": t.reimbursement_received,
        }
        if t.reimbursement_received:
            received.append(row)
        else:
            pending.append(row)

    return {
        "pending_count": len(pending),
        "pending_total": round(sum(r["amount"] for r in pending), 2),
        "received_count": len(received),
        "received_total": round(sum(r["amount"] for r in received), 2),
        "pending": pending,
        "received": received,
    }


@router.get("/reimbursement-match")
def reimbursement_match(
    session: Session = Depends(get_session),
    current_user: User = Depends(get_current_user),
):
    """Find credit transactions that look like employer reimbursement payments."""
    # Get pending reimbursable total
    pending_txns = session.exec(
        select(Transaction).where(
            Transaction.user_id == current_user.id,
            Transaction.is_reimbursable == True,
            Transaction.reimbursement_received == False,
        )
    ).all()
    if not pending_txns:
        return {"matches": [], "pending_total": 0.0, "pending_count": 0}

    pending_total = sum(float(t.amount) for t in pending_txns)

    # Employer names from payslips (for confidence scoring)
    payslips = session.exec(
        select(Payslip).where(Payslip.user_id == current_user.id)
    ).all()
    employer_names = list({
        p.employer.lower().strip()
        for p in payslips if p.employer and len(p.employer) > 2
    })

    # Recent credit transactions (last 90 days), not already flagged as reimbursable
    cutoff = date.today() - timedelta(days=90)
    credits = session.exec(
        select(Transaction).where(
            Transaction.user_id == current_user.id,
            Transaction.is_credit == True,
            Transaction.date >= cutoff,
            Transaction.is_reimbursable == False,
        ).order_by(Transaction.date.desc())
    ).all()

    matches = []
    for c in credits:
        amount = float(c.amount)
        if pending_total <= 0:
            continue
        diff_pct = abs(amount - pending_total) / pending_total
        if diff_pct > 0.20:  # within 20%
            continue
        desc_lower = (c.description or "").lower()
        employer_hit = any(emp in desc_lower for emp in employer_names)
        # Boost confidence: exact match = high, employer name match = medium
        confidence = "high" if diff_pct <= 0.02 else ("medium" if diff_pct <= 0.10 or employer_hit else "low")
        matches.append({
            "txn_id": c.id,
            "date": str(c.date),
            "description": c.description,
            "amount": amount,
            "diff_pct": round(diff_pct * 100, 1),
            "employer_match": employer_hit,
            "confidence": confidence,
        })

    matches.sort(key=lambda x: x["diff_pct"])
    return {
        "pending_total": round(pending_total, 2),
        "pending_count": len(pending_txns),
        "matches": matches,
    }


@router.get("/review-queue")
def review_queue(
    mode: str = "categorise",  # categorise | tax | receipts
    limit: int = 30,
    session: Session = Depends(get_session),
    current_user: User = Depends(get_current_user),
):
    """
    Return transactions for SortSwipe review.
    - categorise: uncategorised transactions (is_credit=False)
    - tax: unreviewed transactions in tax-relevant categories
    - receipts: tax_deductible, amount >= 300, no receipt_path
    """
    cats = session.exec(select(Category)).all()
    cat_map = {c.id: c for c in cats}
    acc_map_q = session.exec(select(Category).where(Category.name == "Uncategorised"))
    uncat = acc_map_q.first()

    TAX_RELEVANT_NAMES = {
        "work-related travel", "work equipment", "work clothing / ppe",
        "self-education", "investment fees", "donations", "sole trader expenses",
        "internet & phone", "subscriptions",
    }

    if mode == "categorise":
        stmt = select(Transaction).where(
            Transaction.user_id == current_user.id,
            Transaction.is_credit == False,
            Transaction.is_reviewed == False,
        )
        if uncat:
            stmt = stmt.where(Transaction.category_id == uncat.id)
        txns = session.exec(stmt.order_by(Transaction.date.desc()).limit(limit)).all()

    elif mode == "tax":
        # Transactions in tax-relevant categories not yet reviewed
        relevant_ids = [
            c.id for c in cats
            if any(name in c.name.lower() for name in TAX_RELEVANT_NAMES)
        ]
        if not relevant_ids:
            txns = []
        else:
            txns = session.exec(
                select(Transaction).where(
                    Transaction.user_id == current_user.id,
                    Transaction.is_credit == False,
                    Transaction.is_reviewed == False,
                    Transaction.tax_deductible == False,
                    Transaction.category_id.in_(relevant_ids),
                ).order_by(Transaction.date.desc()).limit(limit)
            ).all()

    elif mode == "receipts":
        txns = session.exec(
            select(Transaction).where(
                Transaction.user_id == current_user.id,
                Transaction.tax_deductible == True,
                Transaction.is_credit == False,
                Transaction.amount >= 300,
                Transaction.receipt_path == None,
            ).order_by(Transaction.date.desc()).limit(limit)
        ).all()
    else:
        txns = []

    result = []
    for t in txns:
        cat = cat_map.get(t.category_id)
        result.append({
            **t.model_dump(),
            "category_name": cat.name if cat else "Uncategorised",
            "category_colour": cat.colour if cat else "#d1d5db",
        })
    return {"items": result, "total": len(result)}


@router.patch("/{txn_id}/review")
def mark_reviewed(
    txn_id: int,
    session: Session = Depends(get_session),
    current_user: User = Depends(get_current_user),
):
    txn = session.get(Transaction, txn_id)
    if not txn:
        raise HTTPException(status_code=404, detail="Not found")
    if txn.user_id != current_user.id:
        raise HTTPException(status_code=403, detail="Access denied")
    txn.is_reviewed = True
    txn.is_flagged = False
    session.add(txn)
    session.commit()
    return {"ok": True}


@router.delete("/{txn_id}")
def delete_transaction(
    txn_id: int,
    session: Session = Depends(get_session),
    current_user: User = Depends(get_current_user),
):
    txn = session.get(Transaction, txn_id)
    if not txn:
        raise HTTPException(status_code=404, detail="Not found")
    if txn.user_id != current_user.id:
        raise HTTPException(status_code=403, detail="Access denied")
    session.delete(txn)
    session.commit()
    return {"ok": True}


# ── Transfer Detection ────────────────────────────────────────────────────────

class ConfirmTransfersBody(BaseModel):
    pairs: List[dict]   # [{debit_id: int, credit_id: int}]


@router.get("/transfer-candidates")
def transfer_candidates(
    days: int = 90,
    session: Session = Depends(get_session),
    current_user: User = Depends(get_current_user),
):
    """Find debit/credit pairs across different accounts that look like internal transfers."""
    since = date.today() - timedelta(days=days)

    # Category IDs that are already marked as transfers/excluded
    excl_cats = session.exec(
        select(Category).where(Category.exclude_from_spend == True)
    ).all()
    excl_ids = {c.id for c in excl_cats}

    # All recent non-transfer transactions
    all_txns = session.exec(
        select(Transaction).where(
            Transaction.user_id == current_user.id,
            Transaction.date >= since,
        )
    ).all()
    # Filter out already-categorised transfers
    all_txns = [t for t in all_txns if t.category_id not in excl_ids]

    debits  = [t for t in all_txns if not t.is_credit]
    credits = [t for t in all_txns if t.is_credit]

    # Index credits by amount (cents) for fast lookup
    credits_by_cents: dict[int, list] = {}
    for c in credits:
        k = round(c.amount * 100)
        credits_by_cents.setdefault(k, []).append(c)

    # Load accounts for name lookup
    accs = {a.id: a for a in session.exec(select(Account).where(Account.user_id == current_user.id)).all()}

    pairs = []
    seen_credit_ids: set[int] = set()
    seen_debit_ids:  set[int] = set()

    for debit in sorted(debits, key=lambda t: t.date, reverse=True):
        if debit.id in seen_debit_ids:
            continue
        k = round(debit.amount * 100)
        for credit in credits_by_cents.get(k, []):
            if credit.id in seen_credit_ids:
                continue
            if credit.account_id == debit.account_id:
                continue  # same account — not a transfer
            date_diff = abs((credit.date - debit.date).days)
            if date_diff > 7:
                continue

            seen_debit_ids.add(debit.id)
            seen_credit_ids.add(credit.id)

            def _fmt(t: Transaction) -> dict:
                acc = accs.get(t.account_id)
                return {
                    "id": t.id,
                    "date": str(t.date),
                    "description": t.description,
                    "amount": t.amount,
                    "account_id": t.account_id,
                    "account_name": acc.name if acc else "Unknown",
                }

            pairs.append({
                "debit":  _fmt(debit),
                "credit": _fmt(credit),
                "amount": debit.amount,
                "date_diff_days": date_diff,
                "confidence": "high" if date_diff == 0 else ("medium" if date_diff <= 3 else "low"),
            })
            break  # one credit match per debit

    return {"candidates": pairs, "count": len(pairs)}


@router.post("/confirm-transfers")
def confirm_transfers(
    body: ConfirmTransfersBody,
    session: Session = Depends(get_session),
    current_user: User = Depends(get_current_user),
):
    """Mark debit+credit pairs as 'Transfers' category."""
    transfers_cat = session.exec(
        select(Category).where(Category.name == "Transfers")
    ).first()
    if not transfers_cat:
        raise HTTPException(400, "Transfers category not found — create it in Categories first")

    confirmed = 0
    for pair in body.pairs:
        for txn_id in [pair.get("debit_id"), pair.get("credit_id")]:
            if not txn_id:
                continue
            txn = session.get(Transaction, txn_id)
            if txn and txn.user_id == current_user.id:
                txn.category_id = transfers_cat.id
                session.add(txn)
                confirmed += 1
    session.commit()
    return {"confirmed": confirmed // 2, "transactions_updated": confirmed}
