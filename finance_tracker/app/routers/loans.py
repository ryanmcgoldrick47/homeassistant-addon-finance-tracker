from __future__ import annotations

"""Mortgage & Loan Tracker — CRUD, amortisation, extra repayment simulator."""

from datetime import date
from math import ceil
from typing import Optional

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel
from sqlmodel import Session, select

from database import Loan, LoanPayment, get_session, User
from deps import get_current_user

router = APIRouter(tags=["loans"])


# ── Helpers ────────────────────────────────────────────────────────────────────

def _monthly_rate(annual_pct: float) -> float:
    return annual_pct / 100 / 12


def _calc_repayment(principal: float, annual_pct: float, term_months: int) -> float:
    """Standard P&I monthly repayment formula."""
    r = _monthly_rate(annual_pct)
    if r == 0:
        return principal / term_months
    return principal * r * (1 + r) ** term_months / ((1 + r) ** term_months - 1)


def _amortise(principal_cents: int, annual_pct: float, term_months: int,
               repayment_cents: int, offset_cents: int = 0) -> list[dict]:
    """Build full amortisation schedule."""
    r = _monthly_rate(annual_pct)
    balance = principal_cents
    schedule = []
    for m in range(1, term_months + 1):
        effective = max(0, balance - offset_cents)
        interest = round(effective * r)
        principal_paid = repayment_cents - interest
        if principal_paid <= 0:
            # Interest-only or negative — loan not paying down
            schedule.append({
                "month": m, "payment": repayment_cents / 100,
                "interest": interest / 100, "principal": 0,
                "balance": balance / 100,
            })
            continue
        balance -= principal_paid
        if balance <= 0:
            # Final payment
            final_principal = (balance + principal_paid)
            final_payment = final_principal + interest
            schedule.append({
                "month": m, "payment": final_payment / 100,
                "interest": interest / 100, "principal": final_principal / 100,
                "balance": 0,
            })
            break
        schedule.append({
            "month": m, "payment": repayment_cents / 100,
            "interest": interest / 100, "principal": principal_paid / 100,
            "balance": balance / 100,
        })
    return schedule


def _loan_summary(loan: Loan, schedule: list[dict]) -> dict:
    total_paid = sum(r["payment"] for r in schedule)
    total_interest = sum(r["interest"] for r in schedule)
    months_remaining = len(schedule)
    payoff_date = None
    if months_remaining > 0:
        from datetime import date
        d = date.today()
        m = d.month - 1 + months_remaining
        payoff_date = date(d.year + m // 12, m % 12 + 1, 1).isoformat()
    return {
        "id": loan.id,
        "name": loan.name,
        "loan_type": loan.loan_type,
        "principal": loan.principal_cents / 100,
        "outstanding": loan.outstanding_cents / 100,
        "interest_rate": loan.interest_rate,
        "start_date": str(loan.start_date),
        "term_months": loan.term_months,
        "monthly_repayment": loan.monthly_repayment_cents / 100,
        "offset": loan.offset_cents / 100,
        "is_active": loan.is_active,
        "notes": loan.notes,
        "months_remaining": months_remaining,
        "payoff_date": payoff_date,
        "total_interest_remaining": round(total_interest, 2),
        "total_cost_remaining": round(total_paid, 2),
    }


# ── Pydantic models ────────────────────────────────────────────────────────────

class LoanCreate(BaseModel):
    name: str
    loan_type: str = "mortgage"
    principal_cents: int
    outstanding_cents: int
    interest_rate: float
    start_date: str         # ISO date
    term_months: int
    monthly_repayment_cents: int
    offset_cents: int = 0
    notes: Optional[str] = None


class LoanUpdate(BaseModel):
    name: Optional[str] = None
    loan_type: Optional[str] = None
    principal_cents: Optional[int] = None
    outstanding_cents: Optional[int] = None
    interest_rate: Optional[float] = None
    start_date: Optional[str] = None
    term_months: Optional[int] = None
    monthly_repayment_cents: Optional[int] = None
    offset_cents: Optional[int] = None
    is_active: Optional[bool] = None
    notes: Optional[str] = None


class LoanPaymentIn(BaseModel):
    payment_date: str   # ISO date
    amount: float       # total payment in dollars
    principal: float = 0.0
    interest: float = 0.0
    notes: Optional[str] = None


# ── Endpoints ─────────────────────────────────────────────────────────────────

@router.get("/api/loans")
def list_loans(
    session: Session = Depends(get_session),
    current_user: User = Depends(get_current_user),
):
    loans = session.exec(
        select(Loan).where(Loan.user_id == current_user.id, Loan.is_active == True)
    ).all()
    result = []
    for loan in loans:
        schedule = _amortise(
            loan.outstanding_cents, loan.interest_rate,
            loan.term_months, loan.monthly_repayment_cents, loan.offset_cents,
        )
        result.append(_loan_summary(loan, schedule))
    return result


@router.post("/api/loans")
def create_loan(
    body: LoanCreate,
    session: Session = Depends(get_session),
    current_user: User = Depends(get_current_user),
):
    loan = Loan(
        name=body.name,
        loan_type=body.loan_type,
        principal_cents=body.principal_cents,
        outstanding_cents=body.outstanding_cents,
        interest_rate=body.interest_rate,
        start_date=date.fromisoformat(body.start_date),
        term_months=body.term_months,
        monthly_repayment_cents=body.monthly_repayment_cents,
        offset_cents=body.offset_cents,
        notes=body.notes,
        user_id=current_user.id,
    )
    session.add(loan)
    session.commit()
    session.refresh(loan)
    schedule = _amortise(
        loan.outstanding_cents, loan.interest_rate,
        loan.term_months, loan.monthly_repayment_cents, loan.offset_cents,
    )
    return _loan_summary(loan, schedule)


@router.patch("/api/loans/{loan_id}")
def update_loan(
    loan_id: int,
    body: LoanUpdate,
    session: Session = Depends(get_session),
    current_user: User = Depends(get_current_user),
):
    loan = session.get(Loan, loan_id)
    if not loan or loan.user_id != current_user.id:
        raise HTTPException(404, "Loan not found")
    for field, val in body.model_dump(exclude_none=True).items():
        if field == "start_date":
            setattr(loan, field, date.fromisoformat(val))
        else:
            setattr(loan, field, val)
    session.add(loan)
    session.commit()
    session.refresh(loan)
    schedule = _amortise(
        loan.outstanding_cents, loan.interest_rate,
        loan.term_months, loan.monthly_repayment_cents, loan.offset_cents,
    )
    return _loan_summary(loan, schedule)


@router.delete("/api/loans/{loan_id}")
def delete_loan(
    loan_id: int,
    session: Session = Depends(get_session),
    current_user: User = Depends(get_current_user),
):
    loan = session.get(Loan, loan_id)
    if not loan or loan.user_id != current_user.id:
        raise HTTPException(404, "Loan not found")
    session.delete(loan)
    session.commit()
    return {"ok": True}


@router.get("/api/loans/{loan_id}/amortisation")
def amortisation_schedule(
    loan_id: int,
    session: Session = Depends(get_session),
    current_user: User = Depends(get_current_user),
):
    loan = session.get(Loan, loan_id)
    if not loan or loan.user_id != current_user.id:
        raise HTTPException(404, "Loan not found")
    schedule = _amortise(
        loan.outstanding_cents, loan.interest_rate,
        loan.term_months, loan.monthly_repayment_cents, loan.offset_cents,
    )
    return {"schedule": schedule, "loan_id": loan_id}


@router.get("/api/loans/{loan_id}/extra-repayment")
def extra_repayment(
    loan_id: int,
    extra_cents: int = 0,
    mode: str = "monthly",   # "monthly" | "lumpsum"
    session: Session = Depends(get_session),
    current_user: User = Depends(get_current_user),
):
    loan = session.get(Loan, loan_id)
    if not loan or loan.user_id != current_user.id:
        raise HTTPException(404, "Loan not found")

    base_schedule = _amortise(
        loan.outstanding_cents, loan.interest_rate,
        loan.term_months, loan.monthly_repayment_cents, loan.offset_cents,
    )

    if mode == "lumpsum":
        # Reduce outstanding balance by the lump sum, then amortise normally
        reduced_outstanding = max(0, loan.outstanding_cents - extra_cents)
        extra_schedule = _amortise(
            reduced_outstanding, loan.interest_rate,
            loan.term_months, loan.monthly_repayment_cents, loan.offset_cents,
        )
    else:
        # Add extra to every monthly payment
        extra_schedule = _amortise(
            loan.outstanding_cents, loan.interest_rate,
            loan.term_months, loan.monthly_repayment_cents + extra_cents, loan.offset_cents,
        )

    base_interest = sum(r["interest"] for r in base_schedule)
    extra_interest = sum(r["interest"] for r in extra_schedule)
    months_saved = len(base_schedule) - len(extra_schedule)
    interest_saved = base_interest - extra_interest

    new_months = len(extra_schedule)
    d = date.today()
    m = d.month - 1 + new_months
    new_payoff = date(d.year + m // 12, m % 12 + 1, 1).isoformat()

    return {
        "base_months": len(base_schedule),
        "extra_months": len(extra_schedule),
        "months_saved": months_saved,
        "interest_saved": round(interest_saved, 2),
        "new_payoff_date": new_payoff,
        "extra_per_month": extra_cents / 100,
        "mode": mode,
    }


@router.get("/api/loans/suggest-repayment")
def suggest_repayment(
    principal_cents: int,
    interest_rate: float,
    term_months: int,
    session: Session = Depends(get_session),
    current_user: User = Depends(get_current_user),
):
    """Calculate suggested minimum P&I repayment for given loan parameters."""
    repayment = _calc_repayment(principal_cents / 100, interest_rate, term_months)
    return {"suggested_monthly_repayment_cents": round(repayment * 100)}


# ── Loan Payments ──────────────────────────────────────────────────────────────

@router.get("/api/loans/{loan_id}/payments")
def list_payments(
    loan_id: int,
    session: Session = Depends(get_session),
    current_user: User = Depends(get_current_user),
):
    loan = session.get(Loan, loan_id)
    if not loan or loan.user_id != current_user.id:
        raise HTTPException(404, "Loan not found")
    payments = session.exec(
        select(LoanPayment).where(
            LoanPayment.loan_id == loan_id,
            LoanPayment.user_id == current_user.id,
        ).order_by(LoanPayment.payment_date.desc())
    ).all()
    return [
        {
            "id": p.id,
            "payment_date": str(p.payment_date),
            "amount": p.amount_cents / 100,
            "principal": p.principal_cents / 100,
            "interest": p.interest_cents / 100,
            "notes": p.notes,
        }
        for p in payments
    ]


@router.post("/api/loans/{loan_id}/payments")
def add_payment(
    loan_id: int,
    body: LoanPaymentIn,
    session: Session = Depends(get_session),
    current_user: User = Depends(get_current_user),
):
    loan = session.get(Loan, loan_id)
    if not loan or loan.user_id != current_user.id:
        raise HTTPException(404, "Loan not found")

    amount_cents = round(body.amount * 100)
    principal_cents = round(body.principal * 100) if body.principal else amount_cents
    interest_cents = round(body.interest * 100) if body.interest else 0

    payment = LoanPayment(
        loan_id=loan_id,
        payment_date=date.fromisoformat(body.payment_date),
        amount_cents=amount_cents,
        principal_cents=principal_cents,
        interest_cents=interest_cents,
        notes=body.notes,
        user_id=current_user.id,
    )
    session.add(payment)

    # Reduce outstanding balance by principal portion
    loan.outstanding_cents = max(0, loan.outstanding_cents - principal_cents)
    session.add(loan)
    session.commit()
    session.refresh(loan)

    # Return updated loan summary
    schedule = _amortise(
        loan.outstanding_cents, loan.interest_rate,
        loan.term_months, loan.monthly_repayment_cents, loan.offset_cents,
    )
    return {"ok": True, "payment_id": payment.id, "new_outstanding": loan.outstanding_cents / 100,
            "loan": _loan_summary(loan, schedule)}


@router.delete("/api/loans/{loan_id}/payments/{payment_id}")
def delete_payment(
    loan_id: int,
    payment_id: int,
    session: Session = Depends(get_session),
    current_user: User = Depends(get_current_user),
):
    payment = session.get(LoanPayment, payment_id)
    if not payment or payment.loan_id != loan_id or payment.user_id != current_user.id:
        raise HTTPException(404, "Payment not found")
    loan = session.get(Loan, loan_id)
    if loan:
        # Restore the principal to outstanding balance
        loan.outstanding_cents += payment.principal_cents
        session.add(loan)
    session.delete(payment)
    session.commit()
    return {"ok": True}
