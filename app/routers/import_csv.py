from __future__ import annotations

import csv
import hashlib
import io
import os
import shutil
from datetime import date, datetime
from typing import Optional

from fastapi import APIRouter, Depends, HTTPException, UploadFile, File, Form
from sqlmodel import Session, select

from database import Transaction, Account, Category, get_session, engine

router = APIRouter(prefix="/api/import", tags=["import"])

WATCH_DIR      = "/config/finance_tracker/import_watch"
PROCESSED_DIR  = "/config/finance_tracker/import_watch/processed"

# In-memory log of recent folder-watch import results (last 20)
_watch_log: list[dict] = []


def _ensure_watch_dirs():
    os.makedirs(WATCH_DIR,     exist_ok=True)
    os.makedirs(PROCESSED_DIR, exist_ok=True)


async def folder_watch_tick():
    """Called every 30s by the background task. Imports any CSV found in the watch folder."""
    from deps import get_setting
    _ensure_watch_dirs()
    try:
        files = [f for f in os.listdir(WATCH_DIR)
                 if f.lower().endswith(".csv") and os.path.isfile(os.path.join(WATCH_DIR, f))]
    except Exception:
        return

    if not files:
        return

    with Session(engine) as session:
        enabled = get_setting(session, "folder_watch_enabled", "1")
        if enabled != "1":
            return
        # Use the first account as the import target
        account = session.exec(select(Account)).first()
        if not account:
            return
        account_id = account.id

    for filename in files:
        src = os.path.join(WATCH_DIR, filename)
        try:
            with open(src, "r", encoding="utf-8-sig") as f:
                text = f.read()
            with Session(engine) as session:
                result = import_csv_text(text, account_id, session)
            # Move to processed/
            dst = os.path.join(PROCESSED_DIR, filename)
            # Avoid name collision in processed dir
            if os.path.exists(dst):
                base, ext = os.path.splitext(filename)
                dst = os.path.join(PROCESSED_DIR, f"{base}_{datetime.now().strftime('%H%M%S')}{ext}")
            shutil.move(src, dst)
            entry = {
                "file": filename, "status": "ok",
                "imported": result["imported"], "skipped": result["skipped"],
                "errors": len(result["errors"]),
                "processed_at": datetime.now().strftime("%Y-%m-%d %H:%M"),
            }
        except Exception as e:
            entry = {
                "file": filename, "status": "error", "error": str(e)[:200],
                "processed_at": datetime.now().strftime("%Y-%m-%d %H:%M"),
            }
        _watch_log.insert(0, entry)
        if len(_watch_log) > 20:
            _watch_log.pop()
        # HA notification
        try:
            import requests
            token = os.environ.get("SUPERVISOR_TOKEN", "")
            if token and entry["status"] == "ok":
                msg = f"{filename}: {entry['imported']} imported, {entry['skipped']} skipped"
                requests.post(
                    "http://supervisor/core/api/services/notify/mobile_app_ryans_iphone",
                    headers={"Authorization": f"Bearer {token}"},
                    json={"title": "Finance Tracker — CSV imported", "message": msg},
                    timeout=5,
                )
        except Exception:
            pass


@router.get("/watch-status")
def watch_status():
    _ensure_watch_dirs()
    try:
        pending = [f for f in os.listdir(WATCH_DIR)
                   if f.lower().endswith(".csv") and os.path.isfile(os.path.join(WATCH_DIR, f))]
    except Exception:
        pending = []
    return {
        "watch_dir":  WATCH_DIR,
        "pending":    pending,
        "recent_log": _watch_log[:10],
    }


_DATE_FORMATS = [
    "%d/%m/%Y",   # DD/MM/YYYY (Macquarie)
    "%Y-%m-%d",   # YYYY-MM-DD (ISO)
    "%d %b %Y",   # DD Mon YYYY (e.g. 02 Mar 2026)
    "%d-%m-%Y",   # DD-MM-YYYY
    "%m/%d/%Y",   # MM/DD/YYYY (US)
]


def _parse_macquarie_date(s: str) -> date:
    s = s.strip()
    if not s:
        raise ValueError("Empty date field")
    for fmt in _DATE_FORMATS:
        try:
            return datetime.strptime(s, fmt).date()
        except ValueError:
            continue
    raise ValueError(f"Unrecognised date format: {s!r}")


# Column name aliases — maps our canonical name -> list of CSV header variants (lowercase)
_COL_ALIASES = {
    "date": ["date", "transaction date", "value date", "effective date", "processed date"],
    "description": ["description", "narration", "memo", "details", "transaction details", "particulars", "reference"],
    "credit": ["credit", "credits", "credit amount", "deposit", "deposits", "money in"],
    "debit": ["debit", "debits", "debit amount", "withdrawal", "withdrawals", "money out"],
    "amount": ["amount", "net amount", "transaction amount"],
}


def _map_columns(headers: list[str]) -> dict[str, str]:
    """Return a mapping of canonical name -> actual CSV header, best-effort."""
    lower_map = {h.lower().strip(): h for h in headers}
    result: dict[str, str] = {}
    for canonical, aliases in _COL_ALIASES.items():
        for alias in aliases:
            if alias in lower_map:
                result[canonical] = lower_map[alias]
                break
    return result


def _make_hash(date_val: date, description: str, amount: float) -> str:
    raw = f"{date_val.isoformat()}|{description.strip()}|{amount:.2f}"
    return hashlib.sha256(raw.encode()).hexdigest()


def _find_uncategorised_id(session: Session) -> Optional[int]:
    cat = session.exec(select(Category).where(Category.name == "Uncategorised")).first()
    return cat.id if cat else None


@router.get("/accounts")
def list_accounts(session: Session = Depends(get_session)):
    accounts = session.exec(select(Account)).all()
    return accounts


@router.post("/accounts")
def create_account(name: str, bank: str = "Macquarie", account_number: str = "", session: Session = Depends(get_session)):
    acc = Account(name=name, bank=bank, account_number=account_number)
    session.add(acc)
    session.commit()
    session.refresh(acc)
    return acc


def import_csv_text(text: str, account_id: int, session: Session) -> dict:
    """Core CSV import logic — takes raw text, returns {imported, skipped, errors}.
    Used by both the HTTP endpoint and the folder watcher background task."""
    account = session.get(Account, account_id)
    if not account:
        return {"imported": 0, "skipped": 0, "errors": [{"row": 0, "error": f"Account {account_id} not found"}]}

    uncategorised_id = _find_uncategorised_id(session)
    reader = csv.DictReader(io.StringIO(text))
    imported = skipped = 0
    errors: list[dict] = []

    headers = reader.fieldnames or []
    col = _map_columns(headers)
    if "date" not in col:
        return {
            "imported": 0, "skipped": 0,
            "errors": [{"row": 0, "error": f"No date column found. Headers: {headers}"}],
        }

    for i, row in enumerate(reader):
        try:
            row = {k.strip(): v.strip() for k, v in row.items() if k}
            date_val = _parse_macquarie_date(row.get(col["date"], ""))
            description = row.get(col.get("description", ""), "") if "description" in col else ""
            credit_col = col.get("credit", "")
            debit_col  = col.get("debit", "")
            credit_str = row.get(credit_col, "") if credit_col else ""
            debit_str  = row.get(debit_col,  "") if debit_col  else ""
            if credit_str and credit_str not in ("0", "0.00"):
                amount = float(credit_str.replace(",", ""))
                is_credit = True
            elif debit_str and debit_str not in ("0", "0.00"):
                amount = float(debit_str.replace(",", ""))
                is_credit = False
            else:
                amt_col = col.get("amount", "")
                amt_str = (row.get(amt_col, "0") if amt_col else "0").replace(",", "") or "0"
                amount    = abs(float(amt_str))
                is_credit = float(amt_str) > 0
            raw_hash = _make_hash(date_val, description, amount)
            if session.exec(select(Transaction).where(Transaction.raw_hash == raw_hash)).first():
                skipped += 1
                continue
            session.add(Transaction(
                account_id=account_id, date=date_val, description=description,
                amount=amount, is_credit=is_credit, category_id=uncategorised_id,
                raw_hash=raw_hash,
            ))
            imported += 1
        except Exception as e:
            errors.append({"row": i + 2, "error": str(e)})

    session.commit()
    return {"imported": imported, "skipped": skipped, "errors": errors}


@router.post("/csv")
async def import_csv(
    file: UploadFile = File(...),
    account_id: int = Form(...),
    session: Session = Depends(get_session),
):
    content = await file.read()
    text = content.decode("utf-8-sig")
    return import_csv_text(text, account_id, session)
