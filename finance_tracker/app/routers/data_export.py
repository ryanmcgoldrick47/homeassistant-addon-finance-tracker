"""Full JSON data export and import (backup / restore)."""
from __future__ import annotations

import json
from datetime import datetime

from fastapi import APIRouter, Depends, HTTPException
from fastapi.responses import JSONResponse
from sqlmodel import Session, select

from database import (
    Account, Category, Transaction, Budget, Bill, BillPayment,
    CryptoHolding, CryptoTrade, ShareHolding, NetWorthSnapshot,
    AcquisitionLot, Disposal, Dividend, SuperSnapshot, SuperContribution,
    Goal, GoalContribution, Achievement, Challenge, Payslip,
    get_session, User,
)
from deps import get_current_user

router = APIRouter(prefix="/api/data", tags=["data"])


def _rows(session, model, user_id: int):
    """Return all rows for a user-scoped model as list of dicts."""
    items = session.exec(select(model).where(model.user_id == user_id)).all()
    return [_to_dict(i) for i in items]


def _to_dict(obj) -> dict:
    d = {}
    for col in obj.__class__.__fields__:
        val = getattr(obj, col, None)
        if hasattr(val, 'isoformat'):
            d[col] = val.isoformat()
        else:
            d[col] = val
    return d


@router.get("/export")
def export_data(
    session: Session = Depends(get_session),
    current_user: User = Depends(get_current_user),
):
    uid = current_user.id

    # Categories are shared (no user_id) — export all
    categories = session.exec(select(Category)).all()

    payload = {
        "export_version": 1,
        "exported_at": datetime.now().isoformat(timespec="seconds"),
        "user": {"id": uid, "name": current_user.name},
        "categories": [_to_dict(c) for c in categories],
        "accounts": _rows(session, Account, uid),
        "transactions": _rows(session, Transaction, uid),
        "budgets": _rows(session, Budget, uid),
        "bills": _rows(session, Bill, uid),
        "bill_payments": _rows(session, BillPayment, uid),
        "crypto_holdings": _rows(session, CryptoHolding, uid),
        "crypto_trades": _rows(session, CryptoTrade, uid),
        "share_holdings": _rows(session, ShareHolding, uid),
        "networth_snapshots": _rows(session, NetWorthSnapshot, uid),
        "acquisition_lots": _rows(session, AcquisitionLot, uid),
        "disposals": _rows(session, Disposal, uid),
        "dividends": _rows(session, Dividend, uid),
        "super_snapshots": _rows(session, SuperSnapshot, uid),
        "super_contributions": _rows(session, SuperContribution, uid),
        "goals": _rows(session, Goal, uid),
        "goal_contributions": _rows(session, GoalContribution, uid),
        "achievements": _rows(session, Achievement, uid),
        "challenges": _rows(session, Challenge, uid),
        "payslips": _rows(session, Payslip, uid),
    }

    return JSONResponse(
        content=payload,
        headers={
            "Content-Disposition": f'attachment; filename="finance_backup_{datetime.now().strftime("%Y%m%d")}.json"',
        },
    )


@router.post("/import")
def import_data(
    payload: dict,
    session: Session = Depends(get_session),
    current_user: User = Depends(get_current_user),
):
    """Restore data from a JSON export. Merges into current user — does NOT delete existing data."""
    version = payload.get("export_version", 0)
    if version != 1:
        raise HTTPException(400, f"Unsupported export version: {version}")

    uid = current_user.id
    stats = {}

    def _import_rows(model, rows: list, *, skip_user_id: bool = False):
        count = 0
        for row in rows:
            row = dict(row)
            row.pop("id", None)  # let DB auto-assign new ID
            if not skip_user_id:
                row["user_id"] = uid
            # Convert date strings back to date objects for date fields
            for k, v in list(row.items()):
                if isinstance(v, str) and len(v) == 10 and v[4] == '-' and v[7] == '-':
                    try:
                        from datetime import date
                        row[k] = date.fromisoformat(v)
                    except ValueError:
                        pass
            try:
                session.add(model(**row))
                count += 1
            except Exception:
                pass
        return count

    # Categories — only import ones that don't already exist by name
    existing_cat_names = {c.name.lower() for c in session.exec(select(Category)).all()}
    new_cats = [c for c in payload.get("categories", []) if c.get("name", "").lower() not in existing_cat_names]
    if new_cats:
        stats["categories"] = _import_rows(Category, new_cats, skip_user_id=True)

    # Accounts — skip duplicates by name
    existing_acct_names = {a.name.lower() for a in session.exec(select(Account).where(Account.user_id == uid)).all()}
    new_accts = [a for a in payload.get("accounts", []) if a.get("name", "").lower() not in existing_acct_names]
    stats["accounts"] = _import_rows(Account, new_accts)

    # Transactions — skip by raw_hash
    existing_hashes = {t.raw_hash for t in session.exec(select(Transaction).where(Transaction.user_id == uid)).all()}
    new_txns = [t for t in payload.get("transactions", []) if t.get("raw_hash") not in existing_hashes]
    stats["transactions"] = _import_rows(Transaction, new_txns)

    # Everything else — import all (no dedup for simplicity)
    for key, model in [
        ("budgets", Budget),
        ("bills", Bill),
        ("bill_payments", BillPayment),
        ("crypto_holdings", CryptoHolding),
        ("crypto_trades", CryptoTrade),
        ("share_holdings", ShareHolding),
        ("networth_snapshots", NetWorthSnapshot),
        ("acquisition_lots", AcquisitionLot),
        ("disposals", Disposal),
        ("dividends", Dividend),
        ("super_snapshots", SuperSnapshot),
        ("super_contributions", SuperContribution),
        ("goals", Goal),
        ("goal_contributions", GoalContribution),
        ("challenges", Challenge),
    ]:
        rows = payload.get(key, [])
        if rows:
            stats[key] = _import_rows(model, rows)

    session.commit()
    return {"ok": True, "imported": stats}
