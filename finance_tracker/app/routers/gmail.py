"""Gmail IMAP scanner — extracts expense transactions and payslips from labelled emails."""
from __future__ import annotations

import concurrent.futures
import email
import hashlib
import imaplib
import json
import os
import asyncio
import re
import ssl
from datetime import date, datetime, timedelta
from email.header import decode_header
from html.parser import HTMLParser
from typing import Optional

from fastapi import APIRouter, BackgroundTasks, Depends, HTTPException
from pydantic import BaseModel
from sqlmodel import Session, select

from database import Account, Category, Setting, Transaction, Payslip, engine, get_session, User
from deps import get_setting, set_setting, get_current_user

router = APIRouter(prefix="/api/gmail", tags=["gmail"])

_DATA_DIR = os.environ.get("FINANCE_DATA_DIR", "/data")

# ── HTML → plain text ────────────────────────────────────────────────────────

class _HTMLStripper(HTMLParser):
    def __init__(self):
        super().__init__()
        self._parts: list[str] = []

    def handle_data(self, data: str):
        self._parts.append(data)

    def get_text(self) -> str:
        return " ".join(p.strip() for p in self._parts if p.strip())


def _html_to_text(html: str) -> str:
    p = _HTMLStripper()
    try:
        p.feed(html)
        return p.get_text()[:3000]
    except Exception:
        return re.sub(r"<[^>]+>", " ", html)[:3000]


def _decode_header_value(raw) -> str:
    parts = decode_header(raw or "")
    result = []
    for b, charset in parts:
        if isinstance(b, bytes):
            result.append(b.decode(charset or "utf-8", errors="replace"))
        else:
            result.append(b)
    return " ".join(result)


def _get_email_text(msg: email.message.Message) -> str:
    """Extract the best plain-text representation of an email."""
    plain, html = [], []
    if msg.is_multipart():
        for part in msg.walk():
            ct = part.get_content_type()
            if ct == "text/plain":
                try:
                    plain.append(part.get_payload(decode=True).decode(
                        part.get_content_charset("utf-8"), errors="replace"))
                except Exception:
                    pass
            elif ct == "text/html":
                try:
                    html.append(part.get_payload(decode=True).decode(
                        part.get_content_charset("utf-8"), errors="replace"))
                except Exception:
                    pass
    else:
        ct = msg.get_content_type()
        try:
            payload = msg.get_payload(decode=True).decode(
                msg.get_content_charset("utf-8"), errors="replace")
            if ct == "text/html":
                html.append(payload)
            else:
                plain.append(payload)
        except Exception:
            pass

    if plain:
        return "\n".join(plain)[:3000]
    elif html:
        return _html_to_text("\n".join(html))
    return ""


def _get_attachments(msg: email.message.Message) -> list[dict]:
    """Extract binary attachments (PDF, images) from a MIME message."""
    attachments = []
    if not msg.is_multipart():
        return attachments
    for part in msg.walk():
        content_type = part.get_content_type()
        disposition = str(part.get("Content-Disposition") or "")
        filename = part.get_filename()
        if filename:
            filename = _decode_header_value(filename)

        # Accept PDFs and images — either by content-type or file extension
        is_pdf = (content_type == "application/pdf" or
                  (filename or "").lower().endswith(".pdf"))
        is_image = content_type.startswith("image/")
        is_octet = content_type in ("application/octet-stream",) and (
            (filename or "").lower().endswith((".pdf", ".jpg", ".jpeg", ".png", ".gif", ".webp"))
        )

        if not (is_pdf or is_image or is_octet):
            continue
        if "inline" in disposition.lower() and not filename:
            continue  # skip inline images with no name (tracking pixels etc.)

        try:
            data = part.get_payload(decode=True)
            if data and len(data) > 100:  # ignore tiny garbage attachments
                ext = ""
                if filename and "." in filename:
                    ext = filename.rsplit(".", 1)[-1].lower()
                elif is_pdf:
                    ext = "pdf"
                elif is_image:
                    ext = content_type.split("/")[-1]
                else:
                    ext = "bin"
                attachments.append({
                    "filename": filename or f"attachment.{ext}",
                    "content_type": content_type,
                    "ext": ext,
                    "data": data,
                })
        except Exception:
            pass
    return attachments


# ── IMAP helpers ─────────────────────────────────────────────────────────────

RECEIPT_KEYWORDS = [
    "receipt", "invoice", "tax invoice", "order confirmation", "payment confirmation",
    "your order", "purchase", "you paid", "payment received", "billing statement",
    "statement", "subscription", "renewal", "charged", "transaction",
]

PAYSLIP_SUBJECT_KEYWORDS = [
    "payslip", "pay slip", "remittance", "payroll", "salary advice",
    "earnings statement", "wage advice", "pay advice",
]


def _connect_gmail(email_addr: str, app_password: str) -> imaplib.IMAP4_SSL:
    ctx = ssl.create_default_context()
    m = imaplib.IMAP4_SSL("imap.gmail.com", 993, ssl_context=ctx)
    m.sock.settimeout(30)
    m.login(email_addr, app_password)
    return m


def _select_label(m: imaplib.IMAP4_SSL, label: str) -> bool:
    """Try to select a Gmail label as an IMAP folder. Returns True on success."""
    attempts = [
        f'"{label}"',          # quoted (handles spaces and slashes)
        label,                  # unquoted
        f'"[Gmail]/{label}"',  # with Gmail prefix (shouldn't be needed for user labels)
    ]
    for attempt in attempts:
        try:
            status, _ = m.select(attempt, readonly=False)
            if status == "OK":
                return True
        except Exception:
            pass
    return False


def _fetch_emails_from_label(
    email_addr: str, app_password: str, label: str, since_date: str
) -> list[dict]:
    """Fetch emails from a specific Gmail label since a given date, including attachments."""
    m = _connect_gmail(email_addr, app_password)

    if not _select_label(m, label):
        m.logout()
        return []

    try:
        _, data = m.search(None, f'SINCE "{since_date}"')
        ids = data[0].split() if data[0] else []
    except Exception:
        m.logout()
        return []

    results = []
    for uid in ids:
        try:
            _, msg_data = m.fetch(uid, "(RFC822)")
            if not msg_data or not msg_data[0]:
                continue
            raw = msg_data[0][1]
            msg = email.message_from_bytes(raw)

            subject = _decode_header_value(msg.get("Subject", ""))
            sender = _decode_header_value(msg.get("From", ""))
            date_str = msg.get("Date", "")
            message_id = msg.get("Message-ID", uid.decode())
            body = _get_email_text(msg)
            attachments = _get_attachments(msg)

            results.append({
                "uid": uid,
                "message_id": message_id,
                "subject": subject[:200],
                "sender": sender[:200],
                "date_raw": date_str,
                "body": body,
                "attachments": attachments,
            })
        except Exception:
            continue

    m.logout()
    return results


def _fetch_receipt_emails(email_addr: str, app_password: str, days: int = 90) -> list[dict]:
    """Legacy: Fetch receipt emails from inbox (kept for /scan endpoint backward compat)."""
    m = _connect_gmail(email_addr, app_password)

    since_date = (datetime.now() - timedelta(days=days)).strftime("%d-%b-%Y")
    ids = []
    for folder in ('"[Gmail]/All Mail"', 'INBOX'):
        status, _ = m.select(folder)
        if status != 'OK':
            continue
        gm_query = (
            f'after:{(datetime.now() - timedelta(days=days)).strftime("%Y/%m/%d")} '
            f'subject:(receipt OR invoice OR order OR payment OR confirmation OR '
            f'statement OR charged OR billing OR subscription OR renewal)'
        )
        try:
            _, data = m.search(None, f'X-GM-RAW "{gm_query}"')
            ids = data[0].split() if data[0] else []
            break
        except Exception:
            pass
        try:
            _, data = m.search(None, f'SINCE "{since_date}"')
            ids = data[0].split() if data[0] else []
            break
        except Exception:
            continue

    results = []
    for uid in ids[-50:]:
        try:
            _, msg_data = m.fetch(uid, "(RFC822)")
            if not msg_data or not msg_data[0]:
                continue
            raw = msg_data[0][1]
            msg = email.message_from_bytes(raw)

            subject = _decode_header_value(msg.get("Subject", ""))
            sender = _decode_header_value(msg.get("From", ""))
            date_str = msg.get("Date", "")
            message_id = msg.get("Message-ID", uid.decode())
            body = _get_email_text(msg)

            if not any(kw in subject.lower() or kw in body.lower() for kw in RECEIPT_KEYWORDS):
                continue

            results.append({
                "message_id": message_id,
                "subject": subject[:200],
                "sender": sender[:200],
                "date_raw": date_str,
                "body": body,
            })
        except Exception:
            continue

    m.logout()
    return results


# ── Receipt file saving ───────────────────────────────────────────────────────

def _save_receipt_binary(
    data: bytes, filename: str, ext: str, label_type: str, txn_date: str, description: str
) -> str:
    """Save a binary receipt file (PDF or image) and return path relative to RECEIPTS_DIR."""
    receipts_base = os.path.join(_DATA_DIR, "receipts")
    receipt_dir = os.path.join(receipts_base, "gmail")
    os.makedirs(receipt_dir, exist_ok=True)
    safe_desc = re.sub(r"[^\w\-]", "_", description)[:40]
    safe_date = re.sub(r"[^\w\-]", "_", txn_date)[:10]
    h = hashlib.md5(data).hexdigest()[:6]
    dest_filename = f"{safe_date}_{safe_desc}_{h}.{ext}"
    dest = os.path.join(receipt_dir, dest_filename)
    with open(dest, "wb") as f:
        f.write(data)
    # Return relative path so /api/receipts/{path} can serve it
    return os.path.relpath(dest, receipts_base)


# ── Payslip import from PDF bytes ────────────────────────────────────────────

async def _import_payslip_from_bytes(
    pdf_bytes: bytes, filename: str, sender: str, session: Session, user_id: int
) -> dict:
    """Extract and save a payslip from raw PDF bytes. Returns result dict."""
    from routers.payslips import _extract_pdf_text, _call_ai_extraction, _check_variations, _save_pdf

    try:
        text = _extract_pdf_text(pdf_bytes)
    except Exception as e:
        return {"ok": False, "error": f"PDF text extraction failed: {e}"}

    if len(text.strip()) < 50:
        return {"ok": False, "error": "PDF appears to be scanned/image-only — no readable text"}

    try:
        data = await _call_ai_extraction(text, session)
    except Exception as e:
        return {"ok": False, "error": f"AI extraction failed: {e}"}

    pay_date_str = data.get("pay_date")
    if not pay_date_str:
        return {"ok": False, "error": "Could not determine pay date from PDF"}

    try:
        pay_date = date.fromisoformat(pay_date_str)
    except ValueError:
        return {"ok": False, "error": f"Invalid pay date: {pay_date_str}"}

    employer = data.get("employer") or _extract_sender_name(sender)

    # Dedup check
    existing = session.exec(
        select(Payslip).where(
            Payslip.user_id == user_id,
            Payslip.pay_date == pay_date,
            Payslip.employer == employer,
        )
    ).first()
    if existing:
        return {"ok": False, "skipped": True, "reason": f"Duplicate: {pay_date} from {employer}"}

    prev = session.exec(
        select(Payslip).where(
            Payslip.user_id == user_id,
            Payslip.pay_date < pay_date,
        ).order_by(Payslip.pay_date.desc()).limit(1)
    ).first()

    flags = _check_variations(data, prev)

    def _cents(val): return round((val or 0) * 100)

    payslip = Payslip(
        pay_date=pay_date,
        period_start=date.fromisoformat(data["period_start"]) if data.get("period_start") else None,
        period_end=date.fromisoformat(data["period_end"]) if data.get("period_end") else None,
        employer=employer,
        pay_frequency=data.get("pay_frequency"),
        gross_pay_cents=_cents(data.get("gross_pay")),
        net_pay_cents=_cents(data.get("net_pay")),
        tax_withheld_cents=_cents(data.get("tax_withheld")),
        super_cents=_cents(data.get("super_amount")),
        annual_leave_hours=data.get("annual_leave_hours"),
        sick_leave_hours=data.get("sick_leave_hours"),
        long_service_hours=data.get("long_service_leave_hours"),
        ytd_gross_cents=_cents(data.get("ytd_gross")) if data.get("ytd_gross") else None,
        ytd_tax_cents=_cents(data.get("ytd_tax")) if data.get("ytd_tax") else None,
        ytd_super_cents=_cents(data.get("ytd_super")) if data.get("ytd_super") else None,
        hours_worked=data.get("hours_worked"),
        allowances_json=json.dumps(data.get("allowances") or []),
        deductions_json=json.dumps(data.get("deductions") or []),
        flags_json=json.dumps(flags),
        raw_extracted=json.dumps(data),
        source="gmail",
        filename=filename,
        is_reviewed=False,
        user_id=user_id,
    )
    session.add(payslip)
    session.commit()
    session.refresh(payslip)
    _save_pdf(payslip.id, pdf_bytes)

    return {
        "ok": True,
        "payslip_id": payslip.id,
        "pay_date": str(pay_date),
        "employer": employer,
        "flags": flags,
    }


def _extract_sender_name(sender: str) -> str:
    """Pull a clean name from a 'Display Name <email>' string."""
    m = re.match(r'^"?([^"<]+)"?\s*<', sender)
    if m:
        return m.group(1).strip()
    return sender.split("@")[0] if "@" in sender else sender


# ── Australian tax rules ──────────────────────────────────────────────────────

_ALWAYS_DEDUCTIBLE = {"donations", "investment fees"}
_EMPLOYEE_DEDUCTIBLE = {
    "work equipment", "self-education", "internet & phone",
    "work-related car expenses", "work-related travel expenses",
    "work-related clothing expenses", "work-related self-education",
    "other work-related deductions",
}
_SOLE_TRADER_DEDUCTIBLE = {
    "sole trader expenses", "work equipment", "self-education",
    "internet & phone", "utilities", "subscriptions",
    "advertising", "accounting", "insurance",
}
_INVESTOR_DEDUCTIBLE = {"investment fees", "subscriptions"}
_REVIEW_CATEGORIES = {
    "internet & phone", "subscriptions", "transport",
    "home & garden", "utilities",
}


def _build_tax_rules(occupation: str, income_types: str) -> dict:
    types_set = {t.strip().lower() for t in income_types.split(",")} if income_types else set()
    deductible = set(_ALWAYS_DEDUCTIBLE)
    if "employee" in types_set or not types_set:
        deductible |= _EMPLOYEE_DEDUCTIBLE
    if "sole_trader" in types_set:
        deductible |= _SOLE_TRADER_DEDUCTIBLE
    if "investor" in types_set:
        deductible |= _INVESTOR_DEDUCTIBLE
    return {"deductible_categories": deductible, "income_types": types_set, "occupation": occupation}


def _apply_tax_rules(category: str, description: str, ai_deductible: bool, ai_tax_cat: Optional[str], rules: dict) -> dict:
    cat_lower = category.lower()
    deductible_cats = rules["deductible_categories"]
    rule_deductible = cat_lower in deductible_cats
    deductible = ai_deductible or rule_deductible
    tax_category = ai_tax_cat
    if deductible and not tax_category:
        if cat_lower in ("work equipment",): tax_category = "Other work-related deductions"
        elif cat_lower in ("self-education",): tax_category = "Work-related self-education"
        elif cat_lower in ("internet & phone",): tax_category = "Other work-related deductions"
        elif cat_lower in ("donations",): tax_category = "Donations"
        elif cat_lower in ("investment fees",): tax_category = "Investment income deductions"
        elif cat_lower in ("sole trader expenses",): tax_category = "Sole trader business expenses"
    needs_review = (cat_lower in _REVIEW_CATEGORIES and not ai_deductible) or (
        cat_lower == "subscriptions" and not deductible
    )
    return {"deductible": deductible, "tax_category": tax_category, "needs_review": needs_review}


# ── AI extraction ─────────────────────────────────────────────────────────────

def _build_batch_prompt(batch: list[dict]) -> str:
    email_text = ""
    for j, e in enumerate(batch):
        email_text += f"\n--- EMAIL {j+1} ---\n"
        email_text += f"From: {e['sender']}\n"
        email_text += f"Subject: {e['subject']}\n"
        email_text += f"Date: {e['date_raw']}\n"
        email_text += f"Body:\n{e['body'][:400]}\n"
    return f"""Extract expense/payment transactions from these receipt emails for an Australian user.
Return a JSON array, one entry per email. If no transaction found, set found=false.
[{{"email_index":1,"found":true,"date":"YYYY-MM-DD","description":"Merchant","amount":29.99,"is_credit":false,"suggested_category":"Subscriptions","is_tax_deductible":false,"tax_category":null,"notes":null}},{{"email_index":2,"found":false}}]
Today={date.today().isoformat()}. Amounts positive, AUD GST-inclusive. Return ONLY the JSON array.
{email_text}"""


def _call_ai_batch(prompt: str, provider: str, api_key: str) -> list:
    try:
        if provider == "gemini":
            from google import genai
            from google.genai import types
            client = genai.Client(api_key=api_key)
            response = client.models.generate_content(
                model="gemini-2.5-flash-lite",
                contents=prompt,
                config=types.GenerateContentConfig(response_mime_type="application/json"),
            )
            raw = response.text.strip()
        else:
            import anthropic
            client = anthropic.Anthropic(api_key=api_key)
            msg = client.messages.create(
                model="claude-haiku-4-5-20251001",
                max_tokens=2000,
                messages=[{"role": "user", "content": prompt}],
            )
            raw = msg.content[0].text.strip()
        if raw.startswith("```"):
            raw = raw.split("```")[1]
            if raw.startswith("json"):
                raw = raw[4:]
        return json.loads(raw.strip())
    except Exception:
        return []


def _extract_transactions_from_emails(emails: list[dict], provider: str, api_key: str) -> list[dict]:
    """Use configured AI to extract transaction data — batches run in parallel."""
    batches = [emails[i:i+10] for i in range(0, len(emails), 10)]
    prompts = [(_build_batch_prompt(b), b) for b in batches]

    extracted = []
    with concurrent.futures.ThreadPoolExecutor(max_workers=3) as ex:
        futures = {ex.submit(_call_ai_batch, p, provider, api_key): b for p, b in prompts}
        for future, batch in futures.items():
            try:
                results = future.result(timeout=30)
            except Exception:
                continue
            for item in results:
                if not item.get("found"):
                    continue
                idx = item.get("email_index", 1) - 1
                if idx < 0 or idx >= len(batch):
                    continue
                src = batch[idx]
                item["message_id"] = src["message_id"]
                item["sender"] = src["sender"]
                item["subject"] = src["subject"]
                extracted.append(item)

    return extracted


# ── Core scan logic ───────────────────────────────────────────────────────────

async def _run_gmail_scan(session: Session, user_id: int, override_since: str | None = None) -> dict:
    """
    Core scan logic: process payslip label and expense label.
    override_since: ISO date string (YYYY-MM-DD) to scan from a specific date regardless of last_scan.
    Returns a summary dict. Called by both the HTTP endpoint and the background scheduler.
    """
    if get_setting(session, "ai_gmail_enabled") == "0":
        return {"ok": False, "reason": "Gmail AI import is disabled. Enable it in Settings → AI Features."}

    email_addr = get_setting(session, "gmail_address") or ""
    app_password = get_setting(session, "gmail_app_password") or ""

    if not email_addr or not app_password:
        return {"ok": False, "reason": "Gmail credentials not configured"}

    payslip_label = get_setting(session, "gmail_payslip_label") or ""
    expense_label = get_setting(session, "gmail_expense_label") or ""

    if not payslip_label and not expense_label:
        return {"ok": False, "reason": "No Gmail labels configured. Set gmail_payslip_label and/or gmail_expense_label in Settings."}

    # Determine since_date: explicit override > last_scan - 1d > 30d fallback
    if override_since:
        try:
            since_date = datetime.fromisoformat(override_since).strftime("%d-%b-%Y")
        except Exception:
            since_date = (datetime.now() - timedelta(days=30)).strftime("%d-%b-%Y")
    else:
        last_scan = get_setting(session, "gmail_last_scan") or ""
        if last_scan:
            try:
                since_dt = datetime.fromisoformat(last_scan) - timedelta(days=1)
                since_date = since_dt.strftime("%d-%b-%Y")
            except Exception:
                since_date = (datetime.now() - timedelta(days=30)).strftime("%d-%b-%Y")
        else:
            since_date = (datetime.now() - timedelta(days=30)).strftime("%d-%b-%Y")

    results: dict = {
        "payslips": {"processed": 0, "skipped": 0, "errors": []},
        "expenses": {"processed": 0, "skipped": 0, "matched": 0, "errors": []},
        "since_date": since_date,
    }

    loop = asyncio.get_event_loop()
    executor = concurrent.futures.ThreadPoolExecutor(max_workers=2)

    # ── 1. Payslip labels (comma-separated) ──────────────────────────────────
    payslip_labels = [l.strip() for l in payslip_label.split(",") if l.strip()]
    if payslip_labels:
        try:
            emails = []
            for lbl in payslip_labels:
                fetched = await loop.run_in_executor(
                    executor, _fetch_emails_from_label, email_addr, app_password, lbl, since_date
                )
                emails += fetched
            # Deduplicate by message_id across labels
            seen = set()
            emails = [e for e in emails if e["message_id"] not in seen and not seen.add(e["message_id"])]
            for em in emails:
                for att in em["attachments"]:
                    if att["ext"] == "pdf":
                        result = await _import_payslip_from_bytes(
                            att["data"], att["filename"], em["sender"], session, user_id
                        )
                        if result.get("ok"):
                            results["payslips"]["processed"] += 1
                        elif result.get("skipped"):
                            results["payslips"]["skipped"] += 1
                        else:
                            results["payslips"]["errors"].append(
                                f"{em['subject'][:60]}: {result.get('error', 'unknown')}"
                            )
        except Exception as e:
            results["payslips"]["errors"].append(str(e))

    # ── 2. Expense/receipt labels (comma-separated) ───────────────────────────
    expense_labels = [l.strip() for l in expense_label.split(",") if l.strip()]
    if expense_labels:
        try:
            emails = []
            for lbl in expense_labels:
                fetched = await loop.run_in_executor(
                    executor, _fetch_emails_from_label, email_addr, app_password, lbl, since_date
                )
                emails += fetched
            seen2 = set()
            emails = [e for e in emails if e["message_id"] not in seen2 and not seen2.add(e["message_id"])]

            provider = get_setting(session, "ai_provider") or "gemini"
            if provider == "gemini":
                api_key = get_setting(session, "gemini_api_key") or os.environ.get("GEMINI_API_KEY", "")
            else:
                api_key = get_setting(session, "anthropic_api_key") or os.environ.get("ANTHROPIC_API_KEY", "")

            categories = session.exec(select(Category)).all()
            cat_map = {c.name.lower(): c for c in categories}
            uncat = next((c for c in categories if c.name == "Uncategorised"), None)

            first_account = session.exec(
                select(Account).where(Account.user_id == user_id)
            ).first()
            account_id = first_account.id if first_account else None

            occupation = get_setting(session, "occupation") or ""
            income_types = get_setting(session, "income_types") or ""
            tax_rules = _build_tax_rules(occupation, income_types)

            # Save binary attachments first, keyed by message_id
            receipt_files: dict[str, list[str]] = {}
            for em in emails:
                saved = []
                for att in em["attachments"]:
                    if att["ext"] in ("pdf", "jpg", "jpeg", "png", "gif", "webp") or att["content_type"].startswith("image/"):
                        try:
                            path = _save_receipt_binary(
                                att["data"], att["filename"], att["ext"],
                                "expense", date.today().isoformat(), em["subject"][:40]
                            )
                            saved.append(path)
                        except Exception as save_err:
                            results["expenses"]["errors"].append(f"Save error: {save_err}")
                if saved:
                    receipt_files[em["message_id"]] = saved

            # AI extraction for transaction amounts from email bodies
            if api_key:
                extracted = _extract_transactions_from_emails(emails, provider, api_key)

                for item in extracted:
                    raw_hash = "gmail:" + hashlib.sha256(
                        (item.get("message_id", "") + str(item.get("amount", "")) + item.get("date", "")).encode()
                    ).hexdigest()

                    exists = session.exec(
                        select(Transaction).where(
                            Transaction.raw_hash == raw_hash,
                            Transaction.user_id == user_id,
                        )
                    ).first()
                    if exists:
                        results["expenses"]["skipped"] += 1
                        continue

                    amount = float(item.get("amount", 0))
                    if amount <= 0:
                        results["expenses"]["skipped"] += 1
                        continue

                    try:
                        txn_date = date.fromisoformat(item["date"])
                    except Exception:
                        txn_date = date.today()

                    # Cross-source dedup: skip if a bank CSV import already has this purchase
                    is_credit_item = bool(item.get("is_credit", False))
                    if not is_credit_item:
                        lo = txn_date - timedelta(days=2)
                        hi = txn_date + timedelta(days=2)
                        csv_candidates = session.exec(
                            select(Transaction).where(
                                Transaction.user_id == user_id,
                                Transaction.date >= lo,
                                Transaction.date <= hi,
                                Transaction.is_credit == False,
                                Transaction.amount >= amount * 0.98,
                                Transaction.amount <= amount * 1.02,
                                Transaction.raw_hash.notlike("gmail:%"),
                            )
                        ).all()
                        cross_match = None
                        if csv_candidates:
                            desc = item.get("description", "")
                            for c in csv_candidates:
                                if _fuzzy_merchant_match(desc, c.description):
                                    cross_match = c
                                    break
                            if not cross_match and len(csv_candidates) == 1:
                                cross_match = csv_candidates[0]
                        if cross_match:
                            # Attach receipt to existing CSV transaction if it doesn't have one yet
                            msg_receipts = receipt_files.get(item.get("message_id", ""), [])
                            if msg_receipts and not cross_match.receipt_path:
                                cross_match.receipt_path = msg_receipts[0]
                                session.add(cross_match)
                            results["expenses"]["matched"] += 1
                            continue

                    cat_name = item.get("suggested_category", "")
                    cat = cat_map.get(cat_name.lower()) or uncat
                    ai_deductible = bool(item.get("is_tax_deductible", False))
                    ai_tax_cat = item.get("tax_category")
                    tax_result = _apply_tax_rules(cat_name, item.get("description", ""), ai_deductible, ai_tax_cat, tax_rules)

                    # Attach saved receipt file if any
                    msg_receipts = receipt_files.get(item.get("message_id", ""), [])
                    receipt_path = msg_receipts[0] if msg_receipts else None

                    txn = Transaction(
                        account_id=account_id,
                        date=txn_date,
                        description=item.get("description", item.get("subject", "Gmail import"))[:200],
                        amount=amount,
                        is_credit=bool(item.get("is_credit", False)),
                        category_id=cat.id if cat else None,
                        tax_deductible=tax_result["deductible"],
                        tax_category=tax_result["tax_category"],
                        is_flagged=tax_result["needs_review"],
                        notes=item.get("notes"),
                        receipt_path=receipt_path,
                        raw_hash=raw_hash,
                        user_id=user_id,
                    )
                    session.add(txn)
                    results["expenses"]["processed"] += 1

                session.commit()

        except Exception as e:
            results["expenses"]["errors"].append(str(e))

    # Append to import history (keep last 20 scans)
    history_raw = get_setting(session, "gmail_import_history") or "[]"
    try:
        history = json.loads(history_raw)
    except Exception:
        history = []
    history.insert(0, {
        "ts": datetime.now().isoformat(timespec="seconds"),
        "since_date": since_date,
        "payslips_imported": results["payslips"]["processed"],
        "payslips_skipped": results["payslips"]["skipped"],
        "expenses_imported": results["expenses"]["processed"],
        "expenses_matched": results["expenses"]["matched"],
        "expenses_skipped": results["expenses"]["skipped"],
        "errors": len(results["payslips"]["errors"]) + len(results["expenses"]["errors"]),
    })
    if len(history) > 20:
        history = history[:20]
    set_setting(session, "gmail_import_history", json.dumps(history))

    set_setting(session, "gmail_last_scan", datetime.now().isoformat())
    return {"ok": True, "results": results}


# ── Endpoints ─────────────────────────────────────────────────────────────────

import os as _os_gmail
_DATA_DIR_GMAIL = _os_gmail.environ.get("FINANCE_DATA_DIR", "/data")
RECEIPTS_DIR = _os_gmail.path.join(_DATA_DIR_GMAIL, "receipts")

_STOP_WORDS = {
    "the", "and", "for", "with", "from", "your", "you", "our", "via",
    "pty", "ltd", "inc", "com", "net", "org", "aud", "gst", "tax",
    "payment", "paid", "receipt", "invoice", "order", "purchase",
    "australia", "australian", "sydney", "melbourne", "brisbane",
}


def _merchant_words(text: str) -> set[str]:
    words = re.findall(r"[a-zA-Z]{4,}", text.lower())
    return {w for w in words if w not in _STOP_WORDS}


def _fuzzy_merchant_match(desc_a: str, desc_b: str) -> bool:
    return bool(_merchant_words(desc_a) & _merchant_words(desc_b))


def _find_matching_txn(
    session: Session, amount: float, txn_date: date, description: str, user_id: int
) -> Optional[Transaction]:
    lo = txn_date - timedelta(days=3)
    hi = txn_date + timedelta(days=3)
    amt_lo = amount * 0.98
    amt_hi = amount * 1.02

    candidates = session.exec(
        select(Transaction).where(
            Transaction.user_id == user_id,
            Transaction.date >= lo,
            Transaction.date <= hi,
            Transaction.is_credit == False,
            Transaction.amount >= amt_lo,
            Transaction.amount <= amt_hi,
        )
    ).all()

    if not candidates:
        return None
    for txn in candidates:
        if _fuzzy_merchant_match(description, txn.description):
            return txn
    if len(candidates) == 1:
        return candidates[0]
    return None


class ScanRequest(BaseModel):
    days: int = 90
    account_id: Optional[int] = None
    commit: bool = False


class CorrelateRequest(BaseModel):
    days: int = 30


@router.get("/test")
def test_gmail_connection(
    session: Session = Depends(get_session),
    current_user: User = Depends(get_current_user),
):
    """Test Gmail IMAP connection with saved credentials."""
    email_addr = get_setting(session, "gmail_address") or ""
    app_password = get_setting(session, "gmail_app_password") or ""
    if not email_addr or not app_password:
        raise HTTPException(400, "Gmail address and app password not configured in Settings")
    try:
        m = _connect_gmail(email_addr, app_password)
        _, counts = m.select("INBOX")
        m.logout()
        return {"ok": True, "inbox_messages": int(counts[0]) if counts else 0}
    except imaplib.IMAP4.error as e:
        raise HTTPException(400, f"IMAP login failed: {e}. Check your app password is correct.")
    except Exception as e:
        raise HTTPException(500, f"Connection error: {e}")


@router.get("/labels")
def list_gmail_labels(
    session: Session = Depends(get_session),
    current_user: User = Depends(get_current_user),
):
    """List all IMAP folder names from the connected Gmail account."""
    email_addr = get_setting(session, "gmail_address") or ""
    app_password = get_setting(session, "gmail_app_password") or ""
    if not email_addr or not app_password:
        raise HTTPException(400, "Gmail credentials not configured in Settings")
    try:
        m = _connect_gmail(email_addr, app_password)
        _, folders = m.list()
        m.logout()
        labels = []
        for f in (folders or []):
            if isinstance(f, bytes):
                f = f.decode("utf-8", errors="replace")
            # IMAP list response: (\Flags) "/" "Folder Name"  or  (\Flags) "/" Folder
            match = re.search(r'\(.*?\)\s+"[^"]+"\s+(.+)$', f)
            if match:
                name = match.group(1).strip().strip('"')
                labels.append(name)
        labels.sort()
        return {"labels": labels}
    except imaplib.IMAP4.error as e:
        raise HTTPException(400, f"IMAP error: {e}")
    except Exception as e:
        raise HTTPException(500, f"Error listing labels: {e}")


@router.get("/status")
def gmail_scan_status(
    session: Session = Depends(get_session),
    current_user: User = Depends(get_current_user),
):
    """Return last scan time, configured labels, running state, and last result."""
    import json as _json
    last_scan = get_setting(session, "gmail_last_scan") or None
    payslip_label = get_setting(session, "gmail_payslip_label") or ""
    expense_label = get_setting(session, "gmail_expense_label") or ""
    running = get_setting(session, "gmail_scan_running") == "1"
    last_result_raw = get_setting(session, "gmail_last_result") or None
    last_result = None
    if last_result_raw:
        try:
            last_result = _json.loads(last_result_raw)
        except Exception:
            last_result = {"raw": last_result_raw}
    return {
        "last_scan": last_scan,
        "payslip_label": payslip_label,
        "expense_label": expense_label,
        "configured": bool(payslip_label or expense_label),
        "running": running,
        "last_result": last_result,
    }


@router.get("/import-history")
def get_import_history(
    session: Session = Depends(get_session),
    current_user: User = Depends(get_current_user),
):
    """Return the last 20 Gmail scan results."""
    raw = get_setting(session, "gmail_import_history") or "[]"
    try:
        history = json.loads(raw)
    except Exception:
        history = []
    return {"history": history}


class AutoScanRequest(BaseModel):
    since_date: Optional[str] = None  # ISO date YYYY-MM-DD; if omitted, uses last_scan logic


async def _run_scan_background(user_id: int, override_since: str | None):
    """Run scan in background and store result in settings."""
    from database import engine
    with Session(engine) as bg_session:
        set_setting(bg_session, "gmail_scan_running", "1")
        try:
            result = await _run_gmail_scan(bg_session, user_id, override_since=override_since)
            import json as _json
            set_setting(bg_session, "gmail_last_result", _json.dumps(result))
        except Exception as e:
            set_setting(bg_session, "gmail_last_result", f'{{"ok":false,"reason":"{e}"}}')
        finally:
            set_setting(bg_session, "gmail_scan_running", "0")


@router.post("/auto-scan")
async def auto_scan_gmail(
    background_tasks: BackgroundTasks,
    body: AutoScanRequest = AutoScanRequest(),
    session: Session = Depends(get_session),
    current_user: User = Depends(get_current_user),
):
    """
    Start a Gmail label scan in the background — returns immediately.
    Poll GET /api/gmail/status to check running state and last result.
    """
    running = get_setting(session, "gmail_scan_running") or "0"
    if running == "1":
        return {"ok": False, "reason": "Scan already in progress"}
    background_tasks.add_task(_run_scan_background, current_user.id, body.since_date)
    return {"ok": True, "status": "running", "message": "Scan started — check back in a minute"}


@router.post("/scan")
def scan_gmail(
    body: ScanRequest,
    session: Session = Depends(get_session),
    current_user: User = Depends(get_current_user),
):
    """Legacy: scan Gmail inbox for receipt emails and extract transactions."""
    email_addr = get_setting(session, "gmail_address") or ""
    app_password = get_setting(session, "gmail_app_password") or ""
    provider = get_setting(session, "ai_provider") or "gemini"

    if provider == "gemini":
        api_key = get_setting(session, "gemini_api_key") or os.environ.get("GEMINI_API_KEY", "")
        key_name = "Gemini"
    else:
        api_key = get_setting(session, "anthropic_api_key") or os.environ.get("ANTHROPIC_API_KEY", "")
        key_name = "Anthropic"

    if not email_addr or not app_password:
        raise HTTPException(400, "Gmail credentials not configured. Add them in Settings.")
    if not api_key:
        raise HTTPException(400, f"{key_name} API key not configured. Add it in Settings.")

    account_id = body.account_id
    if not account_id:
        first_account = session.exec(
            select(Account).where(Account.user_id == current_user.id)
        ).first()
        if first_account:
            account_id = first_account.id

    ex = concurrent.futures.ThreadPoolExecutor(max_workers=1)
    future = ex.submit(_fetch_receipt_emails, email_addr, app_password, body.days)
    try:
        emails = future.result(timeout=40)
    except concurrent.futures.TimeoutError:
        ex.shutdown(wait=False)
        raise HTTPException(504, "Gmail scan timed out. Try a shorter date range.")
    except imaplib.IMAP4.error as e:
        ex.shutdown(wait=False)
        raise HTTPException(400, f"IMAP error: {e}")
    except Exception as e:
        ex.shutdown(wait=False)
        raise HTTPException(500, f"Failed to fetch emails: {e}")
    finally:
        ex.shutdown(wait=False)

    if not emails:
        return {"scanned": 0, "found": 0, "transactions": [], "imported": 0, "skipped": 0}

    extracted = _extract_transactions_from_emails(emails, provider, api_key)

    if not body.commit:
        return {
            "scanned": len(emails),
            "found": len(extracted),
            "transactions": extracted,
            "imported": 0,
            "skipped": 0,
        }

    occupation = get_setting(session, "occupation") or ""
    income_types = get_setting(session, "income_types") or ""
    tax_rules = _build_tax_rules(occupation, income_types)

    categories = session.exec(select(Category)).all()
    cat_map = {c.name.lower(): c for c in categories}
    uncat = next((c for c in categories if c.name == "Uncategorised"), None)

    receipt_dir = _os_gmail.path.join(_DATA_DIR_GMAIL, "receipts", "gmail")
    os.makedirs(receipt_dir, exist_ok=True)
    email_body_map = {e["message_id"]: e for e in emails}

    imported, skipped = 0, 0
    saved = []
    for item in extracted:
        raw_hash = "gmail:" + hashlib.sha256(
            (item.get("message_id", "") + str(item.get("amount", "")) + item.get("date", "")).encode()
        ).hexdigest()

        exists = session.exec(
            select(Transaction).where(
                Transaction.raw_hash == raw_hash,
                Transaction.user_id == current_user.id,
            )
        ).first()
        if exists:
            skipped += 1
            continue

        try:
            txn_date = date.fromisoformat(item["date"])
        except Exception:
            txn_date = date.today()

        cat_name = item.get("suggested_category", "")
        cat = cat_map.get(cat_name.lower()) or uncat
        ai_deductible = bool(item.get("is_tax_deductible", False))
        ai_tax_cat = item.get("tax_category")
        tax_result = _apply_tax_rules(cat_name, item.get("description", ""), ai_deductible, ai_tax_cat, tax_rules)

        receipt_path = None
        msg_id = item.get("message_id", "")
        src_email = email_body_map.get(msg_id)
        if src_email and body.commit:
            safe_name = re.sub(r"[^\w\-]", "_", f"{txn_date}_{item.get('description','receipt')}")[:60]
            receipt_filename = f"{safe_name}_{raw_hash[:8]}.html"
            receipt_file = os.path.join(receipt_dir, receipt_filename)
            try:
                html_content = (
                    f"<html><head><meta charset='utf-8'><title>{src_email['subject']}</title></head>"
                    f"<body><h2>{src_email['subject']}</h2>"
                    f"<p><strong>From:</strong> {src_email['sender']}<br>"
                    f"<strong>Date:</strong> {src_email['date_raw']}</p><hr>"
                    f"<pre style='white-space:pre-wrap'>{src_email['body']}</pre></body></html>"
                )
                with open(receipt_file, "w", encoding="utf-8") as f:
                    f.write(html_content)
                # Store relative path so /api/receipts/{path} can serve it
                receipts_base = os.path.join(_DATA_DIR_GMAIL, "receipts")
                receipt_path = os.path.relpath(receipt_file, receipts_base)
            except Exception:
                pass

        txn = Transaction(
            account_id=account_id,
            date=txn_date,
            description=item.get("description", item.get("subject", "Gmail receipt"))[:200],
            amount=float(item.get("amount", 0)),
            is_credit=bool(item.get("is_credit", False)),
            category_id=cat.id if cat else None,
            tax_deductible=tax_result["deductible"],
            tax_category=tax_result["tax_category"],
            is_flagged=tax_result["needs_review"],
            notes=item.get("notes"),
            receipt_path=receipt_path,
            raw_hash=raw_hash,
            user_id=current_user.id,
        )
        session.add(txn)
        imported += 1
        saved.append({
            "description": txn.description,
            "amount": txn.amount,
            "date": str(txn.date),
            "category": cat.name if cat else "Uncategorised",
            "tax_deductible": txn.tax_deductible,
            "needs_review": tax_result["needs_review"],
            "receipt_saved": receipt_path is not None,
        })

    session.commit()
    return {
        "scanned": len(emails),
        "found": len(extracted),
        "transactions": saved,
        "imported": imported,
        "skipped": skipped,
    }


@router.post("/correlate")
def correlate_receipts(
    body: CorrelateRequest,
    session: Session = Depends(get_session),
    current_user: User = Depends(get_current_user),
):
    """Scan Gmail for receipt emails and match them to existing transactions."""
    email_addr = get_setting(session, "gmail_address") or ""
    app_password = get_setting(session, "gmail_app_password") or ""
    provider = get_setting(session, "ai_provider") or "gemini"

    if provider == "gemini":
        api_key = get_setting(session, "gemini_api_key") or os.environ.get("GEMINI_API_KEY", "")
        key_name = "Gemini"
    else:
        api_key = get_setting(session, "anthropic_api_key") or os.environ.get("ANTHROPIC_API_KEY", "")
        key_name = "Anthropic"

    if not email_addr or not app_password:
        raise HTTPException(400, "Gmail credentials not configured in Settings.")
    if not api_key:
        raise HTTPException(400, f"{key_name} API key not configured in Settings.")

    ex = concurrent.futures.ThreadPoolExecutor(max_workers=1)
    future = ex.submit(_fetch_receipt_emails, email_addr, app_password, body.days)
    try:
        emails = future.result(timeout=40)
    except concurrent.futures.TimeoutError:
        ex.shutdown(wait=False)
        raise HTTPException(504, "Gmail scan timed out. Try fewer days.")
    except Exception as e:
        ex.shutdown(wait=False)
        raise HTTPException(500, f"Failed to fetch emails: {e}")
    finally:
        ex.shutdown(wait=False)

    if not emails:
        return {"scanned": 0, "matched": [], "unmatched": [], "already_receipted": 0}

    extracted = _extract_transactions_from_emails(emails, provider, api_key)
    email_body_map = {e["message_id"]: e for e in emails}

    os.makedirs(RECEIPTS_DIR, exist_ok=True)

    matched = []
    unmatched = []
    already_receipted = 0

    for item in extracted:
        try:
            txn_date = date.fromisoformat(item["date"])
        except Exception:
            unmatched.append({"reason": "invalid date", **_item_summary(item)})
            continue

        amount = float(item.get("amount", 0))
        if amount <= 0:
            unmatched.append({"reason": "no amount", **_item_summary(item)})
            continue

        txn = _find_matching_txn(
            session, amount, txn_date, item.get("description", ""), current_user.id
        )

        if not txn:
            unmatched.append({"reason": "no transaction match", **_item_summary(item)})
            continue

        if txn.receipt_path:
            already_receipted += 1
            continue

        msg_id = item.get("message_id", "")
        src = email_body_map.get(msg_id)
        receipt_filename = None
        if src:
            safe = re.sub(r"[^\w\-]", "_", f"gmail_{txn.id}_{txn_date}")[:50]
            receipt_filename = f"{safe}_{msg_id.__hash__() & 0xFFFFFF:06x}.html"
            dest = os.path.join(RECEIPTS_DIR, receipt_filename)
            try:
                html_content = (
                    f"<html><head><meta charset='utf-8'>"
                    f"<title>{src['subject']}</title></head>"
                    f"<body><h2>{src['subject']}</h2>"
                    f"<p><strong>From:</strong> {src['sender']}<br>"
                    f"<strong>Date:</strong> {src['date_raw']}</p><hr>"
                    f"<pre style='white-space:pre-wrap'>{src['body']}</pre>"
                    f"</body></html>"
                )
                with open(dest, "w", encoding="utf-8") as f:
                    f.write(html_content)
                txn.receipt_path = receipt_filename
                session.add(txn)
            except Exception:
                receipt_filename = None

        matched.append({
            **_item_summary(item),
            "txn_id": txn.id,
            "txn_description": txn.description,
            "txn_date": str(txn.date),
            "txn_amount": txn.amount,
            "receipt_saved": receipt_filename is not None,
        })

    session.commit()
    return {
        "scanned": len(emails),
        "matched": matched,
        "unmatched": unmatched,
        "already_receipted": already_receipted,
    }


@router.get("/dedup-scan")
def dedup_scan(
    session: Session = Depends(get_session),
    current_user: User = Depends(get_current_user),
):
    """Find potential cross-source duplicates: same purchase imported from both Gmail and a bank CSV."""
    txns = session.exec(
        select(Transaction).where(
            Transaction.user_id == current_user.id,
            Transaction.is_credit == False,
        ).order_by(Transaction.date)
    ).all()

    duplicates = []
    seen_ids: set[int] = set()
    for i, t1 in enumerate(txns):
        if t1.id in seen_ids:
            continue
        t1_gmail = bool(t1.raw_hash and t1.raw_hash.startswith("gmail:"))
        for t2 in txns[i + 1:]:
            if t2.id in seen_ids:
                continue
            days_diff = (t2.date - t1.date).days
            if days_diff > 3:
                break  # txns are date-ordered; no point checking further
            t2_gmail = bool(t2.raw_hash and t2.raw_hash.startswith("gmail:"))
            if t1_gmail == t2_gmail:
                continue  # both same source — not a cross-source dupe
            if max(t1.amount, t2.amount) == 0:
                continue
            amt_diff_pct = abs(t1.amount - t2.amount) / max(t1.amount, t2.amount)
            if amt_diff_pct > 0.02:
                continue
            gmail_txn = t1 if t1_gmail else t2
            csv_txn   = t2 if t1_gmail else t1
            duplicates.append({
                "gmail_txn": {
                    "id": gmail_txn.id,
                    "date": str(gmail_txn.date),
                    "description": gmail_txn.description,
                    "amount": gmail_txn.amount,
                    "receipt_path": gmail_txn.receipt_path,
                },
                "csv_txn": {
                    "id": csv_txn.id,
                    "date": str(csv_txn.date),
                    "description": csv_txn.description,
                    "amount": csv_txn.amount,
                    "receipt_path": csv_txn.receipt_path,
                    "account_id": csv_txn.account_id,
                },
            })
            seen_ids.add(t1.id)
            seen_ids.add(t2.id)
            break

    return {"duplicates": duplicates, "count": len(duplicates)}


class DedupMergeRequest(BaseModel):
    keep_id: int
    delete_id: int


@router.post("/dedup-merge")
def dedup_merge(
    body: DedupMergeRequest,
    session: Session = Depends(get_session),
    current_user: User = Depends(get_current_user),
):
    """Merge two duplicate transactions: keep one, delete the other.
    Transfers receipt and account_id to the kept record if needed."""
    keep   = session.get(Transaction, body.keep_id)
    delete = session.get(Transaction, body.delete_id)
    if not keep or not delete:
        raise HTTPException(404, "Transaction not found")
    if keep.user_id != current_user.id or delete.user_id != current_user.id:
        raise HTTPException(403, "Not your transaction")
    # Transfer receipt from the deleted record if the kept one has none
    if not keep.receipt_path and delete.receipt_path:
        keep.receipt_path = delete.receipt_path
    # Transfer account info if the kept one is missing it
    if not keep.account_id and delete.account_id:
        keep.account_id = delete.account_id
    session.add(keep)
    session.delete(delete)
    session.commit()
    return {"ok": True, "kept_id": keep.id}


def _item_summary(item: dict) -> dict:
    return {
        "email_description": item.get("description", ""),
        "email_amount": item.get("amount"),
        "email_date": item.get("date"),
        "subject": item.get("subject", ""),
    }
