from __future__ import annotations

import csv
import hashlib
import io
import os
import shutil
from datetime import date, datetime, timedelta
from typing import Optional

from fastapi import APIRouter, Depends, HTTPException, UploadFile, File, Form, Query
from sqlmodel import Session, select

from database import Transaction, Account, Category, Loan, LoanPayment, get_session, engine, User
from deps import get_current_user

router = APIRouter(prefix="/api/import", tags=["import"])

import os as _os
_DATA_DIR     = _os.environ.get("FINANCE_DATA_DIR", "/data")
WATCH_DIR     = _os.path.join(_DATA_DIR, "import_watch")
PROCESSED_DIR = _os.path.join(_DATA_DIR, "import_watch", "processed")

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
                # folder_watch runs as user_id=1 (default/primary user)
                result = import_csv_text(text, account_id, session, user_id=1)
            # Move to processed/
            dst = os.path.join(PROCESSED_DIR, filename)
            # Avoid name collision in processed dir
            if os.path.exists(dst):
                base, ext = os.path.splitext(filename)
                dst = os.path.join(PROCESSED_DIR, f"{base}_{datetime.now().strftime('%H%M%S')}{ext}")
            shutil.move(src, dst)
            entry = {
                "file": filename, "status": "ok",
                "imported": result["imported"], "skipped": result["skipped"], "reassigned": result.get("reassigned", 0),
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
        # HA notification (uses ha_notify_targets from settings)
        try:
            import requests
            from sqlalchemy import text as _text
            from database import engine as _engine
            token = os.environ.get("SUPERVISOR_TOKEN", "")
            if token and entry["status"] == "ok":
                with _engine.connect() as _conn:
                    row = _conn.execute(_text("SELECT value FROM setting WHERE key='ha_notify_targets'")).fetchone()
                targets_str = row[0] if row else ""
                targets = [t.strip() for t in targets_str.split(",") if t.strip()]
                msg = f"{filename}: {entry['imported']} imported, {entry['skipped']} skipped"
                for target in targets:
                    try:
                        requests.post(
                            f"http://supervisor/core/api/services/notify/{target}",
                            headers={"Authorization": f"Bearer {token}"},
                            json={"title": "Finance Tracker — CSV imported", "message": msg},
                            timeout=5,
                        )
                    except Exception:
                        pass
        except Exception:
            pass


@router.get("/watch-status")
def watch_status(current_user: User = Depends(get_current_user)):
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
    "balance": ["balance", "running balance", "closing balance", "account balance"],
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


_GENERIC_WORDS = {"account", "bank", "the", "my", "of", "and", "transaction",
                  "savings", "everyday", "macquarie", "commbank", "anz", "nab", "westpac"}


@router.get("/detect-account")
def detect_account(
    account_hint: str = Query("", description="Value from the 'Account' column in the CSV"),
    headers: str = Query("", description="Comma-separated CSV column headers"),
    session: Session = Depends(get_session),
    current_user: User = Depends(get_current_user),
):
    """Auto-detect which account a CSV belongs to based on column headers and account name hint."""
    accounts = session.exec(select(Account).where(Account.user_id == current_user.id)).all()
    if not accounts:
        return {"account_id": None, "confidence": "none"}

    hint = account_hint.strip()
    hint_lower = hint.lower()
    best_id: int | None = None
    best_score = 0

    for acc in accounts:
        score = 0
        name_lower = acc.name.lower().strip()

        # Account number exact match — highest priority
        if acc.account_number and acc.account_number.strip() and acc.account_number.strip() in hint:
            score = 100
        # Name is substring of hint or vice versa
        elif name_lower and (name_lower in hint_lower or hint_lower in name_lower):
            score = 85
        else:
            # Token overlap, excluding generic words
            hint_tokens = set(hint_lower.split()) - _GENERIC_WORDS
            name_tokens = set(name_lower.split()) - _GENERIC_WORDS
            overlap = hint_tokens & name_tokens
            score = len(overlap) * 30

        if score > best_score:
            best_score = score
            best_id = acc.id

    if best_id is None or best_score == 0:
        # Return suggested name from the CSV hint so frontend can offer to create it
        suggested = hint.title().strip() if hint else None
        return {"account_id": None, "confidence": "none", "suggested_name": suggested}

    acc = next((a for a in accounts if a.id == best_id), None)
    confidence = "high" if best_score >= 60 else "low"
    return {
        "account_id": best_id,
        "account_name": acc.name if acc else "",
        "confidence": confidence,
        "score": best_score,
    }


@router.get("/accounts")
def list_accounts(
    session: Session = Depends(get_session),
    current_user: User = Depends(get_current_user),
):
    accounts = session.exec(select(Account).where(Account.user_id == current_user.id)).all()
    return accounts


@router.post("/accounts")
def create_account(
    name: str,
    bank: str = "",
    account_number: str = "",
    session: Session = Depends(get_session),
    current_user: User = Depends(get_current_user),
):
    acc = Account(name=name, bank=bank, account_number=account_number, user_id=current_user.id)
    session.add(acc)
    session.commit()
    session.refresh(acc)
    return acc


@router.patch("/accounts/{account_id}")
def update_account(
    account_id: int,
    name: Optional[str] = None,
    linked_loan_id: Optional[int] = None,
    clear_linked_loan: bool = False,
    offset_loan_id: Optional[int] = None,
    clear_offset_loan: bool = False,
    session: Session = Depends(get_session),
    current_user: User = Depends(get_current_user),
):
    acc = session.get(Account, account_id)
    if not acc or acc.user_id != current_user.id:
        raise HTTPException(404, "Account not found")
    if name is not None:
        acc.name = name.strip()
    if linked_loan_id is not None:
        acc.linked_loan_id = linked_loan_id
    if clear_linked_loan:
        acc.linked_loan_id = None
    if offset_loan_id is not None:
        acc.offset_loan_id = offset_loan_id
    if clear_offset_loan:
        acc.offset_loan_id = None
    session.add(acc)
    session.commit()
    session.refresh(acc)
    return acc


import re as _re

# 3-letter ISO country codes that appear at end of Macquarie descriptions (e.g. "Techcombank Vnm")
_COUNTRY_CODES = {
    "VNM": "VND", "USA": "USD", "GBR": "GBP", "JPN": "JPY", "THA": "THB",
    "SGP": "SGD", "HKG": "HKD", "IDN": "IDR", "MYS": "MYR", "PHL": "PHP",
    "KOR": "KRW", "CHN": "CNY", "IND": "INR", "NZL": "NZD", "CAN": "CAD",
    "EUR": "EUR", "FRA": "EUR", "DEU": "EUR", "ITA": "EUR", "ESP": "EUR",
    "AUT": "EUR", "NLD": "EUR", "BEL": "EUR", "PRT": "EUR", "GRC": "EUR",
    "UAE": "AED", "ARE": "AED", "MEX": "MXN", "BRA": "BRL", "ARG": "ARS",
}
# Currency codes that may appear inline in descriptions
_CURRENCY_CODES = {"USD","EUR","GBP","JPY","THB","VND","SGD","HKD","IDR","MYR",
                   "PHP","KRW","CNY","INR","NZD","CAD","AED","MXN","BRL","CHF","SEK","NOK","DKK"}
_OVERSEAS_KEYWORDS = ["INTL ", "FOREIGN TRANSACTION", "VISA INTL", "OVERSEAS", "INTL FEE"]


def _detect_overseas(description: str) -> tuple[bool, str | None]:
    """Return (is_overseas, currency_code) for a transaction description."""
    desc_upper = description.upper()
    # Check trailing 3-letter country code (common in Macquarie CSV)
    m = _re.search(r"\b([A-Z]{3})\s*$", desc_upper)
    if m and m.group(1) in _COUNTRY_CODES:
        return True, _COUNTRY_CODES[m.group(1)]
    # Check inline currency code
    for code in _CURRENCY_CODES:
        if _re.search(r"\b" + code + r"\b", desc_upper):
            return True, code
    # Keywords
    for kw in _OVERSEAS_KEYWORDS:
        if kw in desc_upper:
            return True, None
    return False, None


def _strip_preamble(text: str) -> str:
    """Skip leading rows that don't look like a CSV header.
    Banks like St George include account info rows before the actual column headers.
    We look for the first row that contains a recognised date column alias AND
    at least one amount/description alias (to avoid false matches on preamble text)."""
    date_aliases = _COL_ALIASES["date"]
    other_aliases = [a for k, v in _COL_ALIASES.items() if k != "date" for a in v]
    lines = text.splitlines()
    for i, line in enumerate(lines):
        low = line.lower()
        # Must match a date alias as a whole CSV field (surrounded by commas/start/end)
        has_date = any(_re.search(r'(?:^|,)\s*' + _re.escape(alias) + r'\s*(?:,|$)', low) for alias in date_aliases)
        has_other = any(alias in low for alias in other_aliases)
        if has_date and has_other:
            return "\n".join(lines[i:])
    return text


def import_csv_text(text: str, account_id: int, session: Session, user_id: int = 1) -> dict:
    """Core CSV import logic — takes raw text, returns {imported, skipped, errors}.
    Used by both the HTTP endpoint and the folder watcher background task."""
    account = session.get(Account, account_id)
    if not account:
        return {"imported": 0, "skipped": 0, "reassigned": 0, "errors": [{"row": 0, "error": f"Account {account_id} not found"}]}

    text = _strip_preamble(text)
    uncategorised_id = _find_uncategorised_id(session)
    reader = csv.DictReader(io.StringIO(text))
    imported = skipped = reassigned = 0
    errors: list[dict] = []

    headers = reader.fieldnames or []
    col = _map_columns(headers)
    if "date" not in col:
        return {
            "imported": 0, "skipped": 0, "reassigned": 0,
            "errors": [{"row": 0, "error": f"No date column found. Headers: {headers}"}],
        }

    last_balance: float | None = None
    loan_payment_rows: list[dict] = []   # (date, amount, description) for credit rows

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

            # Track running balance for loan sync
            bal_col = col.get("balance", "")
            if bal_col:
                bal_str = row.get(bal_col, "").replace(",", "").strip()
                try:
                    last_balance = abs(float(bal_str))
                except (ValueError, TypeError):
                    pass
            # Track credit rows for loan payment recording
            if is_credit and account.linked_loan_id:
                loan_payment_rows.append({"date": date_val, "amount": amount, "description": description})

            raw_hash = _make_hash(date_val, description, amount)
            existing = session.exec(select(Transaction).where(Transaction.raw_hash == raw_hash)).first()
            if existing:
                if existing.account_id == account_id:
                    skipped += 1
                    continue
                else:
                    # Same transaction was imported to wrong account — reassign it
                    existing.account_id = account_id
                    session.add(existing)
                    session.commit()
                    reassigned += 1
                    continue
            # Cross-source dedup: skip if Gmail import already has this purchase
            if not is_credit:
                date_lo = date_val - timedelta(days=1)
                date_hi = date_val + timedelta(days=1)
                gmail_match = session.exec(
                    select(Transaction).where(
                        Transaction.user_id == user_id,
                        Transaction.date >= date_lo,
                        Transaction.date <= date_hi,
                        Transaction.is_credit == False,
                        Transaction.amount >= amount * 0.98,
                        Transaction.amount <= amount * 1.02,
                        Transaction.raw_hash.like("gmail:%"),
                    )
                ).first()
                if gmail_match:
                    skipped += 1
                    continue
            is_overseas, currency_code = _detect_overseas(description)
            session.add(Transaction(
                account_id=account_id, date=date_val, description=description,
                amount=amount, is_credit=is_credit, category_id=uncategorised_id,
                raw_hash=raw_hash, user_id=user_id,
                is_overseas=is_overseas, currency_code=currency_code,
            ))
            imported += 1
        except Exception as e:
            errors.append({"row": i + 2, "error": str(e)})

    session.commit()

    # ── Loan sync ──────────────────────────────────────────────────────────────
    loan_payments_synced = 0
    loan_balance_updated = False
    if account.linked_loan_id:
        loan = session.get(Loan, account.linked_loan_id)
        if loan and loan.user_id == user_id:
            # Update outstanding balance from last statement balance
            if last_balance is not None:
                loan.outstanding_cents = round(last_balance * 100)
                session.add(loan)
                loan_balance_updated = True
            # Create LoanPayment records for credit rows (dedup by date+amount)
            for pr in loan_payment_rows:
                existing = session.exec(
                    select(LoanPayment).where(
                        LoanPayment.loan_id == loan.id,
                        LoanPayment.payment_date == pr["date"],
                        LoanPayment.amount_cents == round(pr["amount"] * 100),
                        LoanPayment.user_id == user_id,
                    )
                ).first()
                if not existing:
                    session.add(LoanPayment(
                        loan_id=loan.id,
                        payment_date=pr["date"],
                        amount_cents=round(pr["amount"] * 100),
                        principal_cents=round(pr["amount"] * 100),  # treat full amount as principal
                        notes=f"CSV import: {pr['description'][:80]}",
                        user_id=user_id,
                    ))
                    loan_payments_synced += 1
            session.commit()

    # ── Offset account sync ─────────────────────────────────────────────────────
    offset_loan_updated = False
    if getattr(account, "offset_loan_id", None):
        offset_loan = session.get(Loan, account.offset_loan_id)
        if offset_loan and offset_loan.user_id == user_id and last_balance is not None:
            offset_loan.offset_cents = round(last_balance * 100)
            session.add(offset_loan)
            session.commit()
            offset_loan_updated = True

    return {
        "imported": imported,
        "skipped": skipped,
        "reassigned": reassigned,
        "errors": errors,
        "loan_payments_synced": loan_payments_synced,
        "loan_balance_updated": loan_balance_updated,
        "offset_loan_updated": offset_loan_updated,
    }


@router.post("/csv")
async def import_csv(
    file: UploadFile = File(...),
    account_id: int = Form(...),
    session: Session = Depends(get_session),
    current_user: User = Depends(get_current_user),
):
    content = await file.read()
    text = content.decode("utf-8-sig")
    return import_csv_text(text, account_id, session, user_id=current_user.id)
