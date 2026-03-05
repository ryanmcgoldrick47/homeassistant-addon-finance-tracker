"""Finance Tracker MCP Server — exposes finance data as tools for Claude Code.

Tools use direct database access (not HTTP calls) to avoid event-loop deadlocks
when mounted in the same single-worker uvicorn process.
"""
from __future__ import annotations

import secrets
from datetime import date, datetime, timedelta
from typing import Optional

from sqlmodel import Session, select, func

from database import (
    engine, Setting, Transaction, Category, Payslip, Bill,
    ShareHolding, NetWorthSnapshot, Dividend, Disposal,
)
from deps import get_setting, set_setting


# ---------------------------------------------------------------------------
# MCP API key management
# ---------------------------------------------------------------------------

def _get_or_create_mcp_key() -> str:
    """Return MCP API key from settings, generating one if not set."""
    with Session(engine) as session:
        key = get_setting(session, "mcp_api_key", "")
        if not key:
            key = secrets.token_hex(24)
            set_setting(session, "mcp_api_key", key)
        return key


# ---------------------------------------------------------------------------
# Build the FastMCP app
# ---------------------------------------------------------------------------

def build_mcp_app():
    """Build and return a FastMCP instance (or None if fastmcp not installed)."""
    try:
        from fastmcp import FastMCP
    except ImportError:
        return None

    mcp = FastMCP(
        "Finance Tracker",
        instructions="Personal finance data — transactions, payslips, tax, investments, net worth.",
    )

    @mcp.tool()
    def get_dashboard(month: int = None, year: int = None) -> dict:
        """Get monthly financial summary: income, spend, net, savings rate, top transactions."""
        today = date.today()
        m = month or today.month
        y = year or today.year
        with Session(engine) as session:
            def _sum(is_credit):
                return float(session.exec(
                    select(func.coalesce(func.sum(Transaction.amount), 0)).where(
                        Transaction.is_credit == is_credit,
                        Transaction.user_id == 1,
                        func.strftime("%m", Transaction.date) == f"{m:02d}",
                        func.strftime("%Y", Transaction.date) == str(y),
                    )
                ).one())

            income = _sum(True)
            spend = _sum(False)
            net = round(income - spend, 2)
            savings_rate = round(net / income * 100, 1) if income > 0 else 0.0

            top = session.exec(
                select(Transaction).where(
                    Transaction.is_credit == False,
                    Transaction.user_id == 1,
                    func.strftime("%m", Transaction.date) == f"{m:02d}",
                    func.strftime("%Y", Transaction.date) == str(y),
                ).order_by(Transaction.amount.desc()).limit(5)
            ).all()

            return {
                "month": f"{m:02d}/{y}",
                "income": round(income, 2),
                "spend": round(spend, 2),
                "net": net,
                "savings_rate_pct": savings_rate,
                "top_expenses": [
                    {"description": t.description, "amount": float(t.amount), "date": str(t.date)}
                    for t in top
                ],
            }

    @mcp.tool()
    def get_transactions(
        search: str = "",
        category_name: str = "",
        month: int = None,
        year: int = None,
        limit: int = 50,
    ) -> dict:
        """Search and filter bank transactions. Returns items list and total count."""
        with Session(engine) as session:
            stmt = select(Transaction).where(Transaction.user_id == 1)
            if search:
                stmt = stmt.where(Transaction.description.ilike(f"%{search}%"))
            if month:
                stmt = stmt.where(func.strftime("%m", Transaction.date) == f"{month:02d}")
            if year:
                stmt = stmt.where(func.strftime("%Y", Transaction.date) == str(year))
            if category_name:
                cat = session.exec(
                    select(Category).where(func.lower(Category.name) == category_name.lower())
                ).first()
                if cat:
                    stmt = stmt.where(Transaction.category_id == cat.id)
            total_stmt = select(func.count()).select_from(stmt.subquery())
            total = session.exec(total_stmt).one()
            rows = session.exec(stmt.order_by(Transaction.date.desc()).limit(min(limit, 200))).all()
            return {
                "total": int(total),
                "items": [
                    {
                        "id": t.id,
                        "date": str(t.date),
                        "description": t.description,
                        "amount": float(t.amount),
                        "is_credit": t.is_credit,
                        "is_overseas": t.is_overseas,
                        "currency_code": t.currency_code,
                        "tax_deductible": t.tax_deductible,
                    }
                    for t in rows
                ],
            }

    @mcp.tool()
    def get_categories() -> list:
        """List all transaction categories with id, name, colour, is_income flag."""
        with Session(engine) as session:
            cats = session.exec(select(Category)).all()
            return [
                {"id": c.id, "name": c.name, "is_income": c.is_income, "colour": c.colour}
                for c in cats
            ]

    @mcp.tool()
    def get_payslip_summary() -> dict:
        """Get YTD payslip summary: gross, tax withheld, super, net pay, leave balances."""
        with Session(engine) as session:
            latest = session.exec(
                select(Payslip).where(Payslip.user_id == 1).order_by(Payslip.pay_date.desc()).limit(1)
            ).first()
            if not latest:
                return {"has_data": False}
            return {
                "has_data": True,
                "latest_pay_date": str(latest.pay_date),
                "employer": latest.employer,
                "latest_gross": latest.gross_pay_cents / 100,
                "latest_net": latest.net_pay_cents / 100,
                "latest_tax_withheld": latest.tax_withheld_cents / 100,
                "latest_super": latest.super_cents / 100,
                "ytd_gross": (latest.ytd_gross_cents or 0) / 100,
                "ytd_tax": (latest.ytd_tax_cents or 0) / 100,
                "ytd_super": (latest.ytd_super_cents or 0) / 100,
                "annual_leave_hours": latest.annual_leave_hours,
                "sick_leave_hours": latest.sick_leave_hours,
                "pay_frequency": latest.pay_frequency,
            }

    @mcp.tool()
    def get_tax_summary(fy: int = None) -> dict:
        """Get ATO tax prefill breakdown for a financial year (salary, dividends, CGT, deductions)."""
        if not fy:
            today = date.today()
            fy = today.year + 1 if today.month >= 7 else today.year
        start = date(fy - 1, 7, 1)
        end = date(fy, 6, 30)
        with Session(engine) as session:
            payslips = session.exec(
                select(Payslip).where(
                    Payslip.user_id == 1,
                    Payslip.pay_date >= start,
                    Payslip.pay_date <= end,
                )
            ).all()
            divs = session.exec(
                select(Dividend).where(
                    Dividend.user_id == 1,
                    Dividend.pay_date >= start,
                    Dividend.pay_date <= end,
                )
            ).all()
            disposals = session.exec(
                select(Disposal).where(
                    Disposal.user_id == 1,
                    Disposal.disposed_date >= start,
                    Disposal.disposed_date <= end,
                )
            ).all()
            deductible = session.exec(
                select(func.coalesce(func.sum(Transaction.amount), 0)).where(
                    Transaction.user_id == 1,
                    Transaction.tax_deductible == True,
                    Transaction.date >= start,
                    Transaction.date <= end,
                )
            ).one()
            wfh_days = int(get_setting(session, "wfh_days", "0") or "0")

        return {
            "fy": fy,
            "item1_salary": {
                "gross": round(sum(p.gross_pay_cents for p in payslips) / 100, 2),
                "tax_withheld": round(sum(p.tax_withheld_cents for p in payslips) / 100, 2),
            },
            "item11_dividends": {
                "cash": round(sum(d.amount_aud for d in divs), 2),
                "franking_credits": round(sum(d.franking_credits_aud for d in divs), 2),
            },
            "item18_cgt": {
                "shares_net_gain": round(sum(d.gain_aud for d in disposals if d.asset_type == "share"), 2),
                "crypto_net_gain": round(sum(d.gain_aud for d in disposals if d.asset_type == "crypto"), 2),
            },
            "deductions_d1_wfh": round(wfh_days * 8 * 0.67, 2),
            "deductions_d5_other": round(float(deductible), 2),
        }

    @mcp.tool()
    def get_net_worth() -> dict:
        """Get the most recent net worth snapshot: assets, liabilities, net worth total."""
        with Session(engine) as session:
            snap = session.exec(
                select(NetWorthSnapshot).where(NetWorthSnapshot.user_id == 1)
                .order_by(NetWorthSnapshot.snapshot_date.desc()).limit(1)
            ).first()
            if not snap:
                return {"has_data": False}
            return {
                "has_data": True,
                "snapshot_date": str(snap.snapshot_date),
                "total_assets": snap.total_assets,
                "total_liabilities": snap.total_liabilities,
                "net_worth": snap.net_worth,
                "cash_savings": snap.cash_savings,
                "super_balance": snap.super_balance,
                "shares_value": snap.shares_value,
                "crypto_value": snap.crypto_value,
                "property_value": snap.property_value,
                "mortgage_balance": snap.mortgage_balance,
                "hecs_debt": snap.hecs_debt,
            }

    @mcp.tool()
    def get_upcoming_bills(days: int = 60) -> list:
        """Get predicted upcoming bill payments within the next N days from active bill schedules."""
        FREQ_DAYS = {"weekly": 7, "fortnightly": 14, "monthly": 30, "quarterly": 91, "annual": 365}
        today = date.today()
        cutoff = today + timedelta(days=min(days, 365))
        results = []
        with Session(engine) as session:
            bills = session.exec(
                select(Bill).where(
                    Bill.user_id == 1,
                    Bill.is_active == True,
                    Bill.next_due != None,
                )
            ).all()
            for bill in bills:
                due = bill.next_due
                interval = timedelta(days=FREQ_DAYS.get(bill.frequency, 30))
                while due <= cutoff:
                    if due >= today:
                        results.append({
                            "bill_name": bill.name,
                            "amount": round(bill.amount_cents / 100, 2),
                            "due_date": str(due),
                            "frequency": bill.frequency,
                        })
                    due += interval
        results.sort(key=lambda x: x["due_date"])
        return results

    @mcp.tool()
    def get_investments_summary() -> dict:
        """Get investment portfolio summary: total value, gain/loss, holdings breakdown."""
        with Session(engine) as session:
            holdings = session.exec(
                select(ShareHolding).where(ShareHolding.user_id == 1)
            ).all()
            if not holdings:
                return {"has_data": False, "total_value": 0, "total_gain": 0, "count": 0}
            return {
                "has_data": True,
                "total_value": round(sum(h.value_aud for h in holdings), 2),
                "total_cost_basis": round(sum(h.cost_basis_aud for h in holdings), 2),
                "total_gain": round(sum(h.gain_aud for h in holdings), 2),
                "count": len(holdings),
                "holdings": [
                    {
                        "ticker": h.ticker,
                        "qty": h.qty,
                        "value_aud": round(h.value_aud, 2),
                        "gain_aud": round(h.gain_aud, 2),
                        "gain_pct": round(h.gain_pct, 1),
                        "broker": h.broker,
                    }
                    for h in sorted(holdings, key=lambda x: x.value_aud, reverse=True)
                ],
            }

    @mcp.tool()
    def categorise_transaction(transaction_id: int, category_name: str) -> dict:
        """Update the category of a transaction. Returns {ok, category_name} or {error}."""
        with Session(engine) as session:
            cat = session.exec(
                select(Category).where(func.lower(Category.name) == category_name.lower())
            ).first()
            if not cat:
                return {"error": f"Category '{category_name}' not found. Use get_categories() to list valid names."}
            txn = session.get(Transaction, transaction_id)
            if not txn or txn.user_id != 1:
                return {"error": f"Transaction {transaction_id} not found"}
            txn.category_id = cat.id
            session.add(txn)
            session.commit()
            return {"ok": True, "transaction_id": transaction_id, "category_name": cat.name}

    return mcp
