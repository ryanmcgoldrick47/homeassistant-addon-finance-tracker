"""Gmail IMAP scanner — extracts expense transactions from receipt emails using Claude."""
from __future__ import annotations

import concurrent.futures
import email
import hashlib
import imaplib
import json
import os
import re
import ssl
from datetime import date, datetime, timedelta
from email.header import decode_header
from html.parser import HTMLParser
from typing import Optional

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel
from sqlmodel import Session, select

from database import Account, Category, Setting, Transaction, engine, get_session, User
from deps import get_setting, get_current_user

router = APIRouter(prefix="/api/gmail", tags=["gmail"])


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


# ── IMAP helpers ─────────────────────────────────────────────────────────────

RECEIPT_KEYWORDS = [
    "receipt", "invoice", "tax invoice", "order confirmation", "payment confirmation",
    "your order", "purchase", "you paid", "payment received", "billing statement",
    "statement", "subscription", "renewal", "charged", "transaction",
]

# ── Australian tax rules ──────────────────────────────────────────────────────

# Categories that are ALWAYS deductible for any income type
_ALWAYS_DEDUCTIBLE = {"donations", "investment fees"}

# Categories deductible for PAYG employees (ATO work-related expense rules)
_EMPLOYEE_DEDUCTIBLE = {
    "work equipment", "self-education", "internet & phone",
    "work-related car expenses", "work-related travel expenses",
    "work-related clothing expenses", "work-related self-education",
    "other work-related deductions",
}

# Categories deductible for sole traders / ABN holders
_SOLE_TRADER_DEDUCTIBLE = {
    "sole trader expenses", "work equipment", "self-education",
    "internet & phone", "utilities", "subscriptions",
    "advertising", "accounting", "insurance",
}

# Categories deductible for investors
_INVESTOR_DEDUCTIBLE = {"investment fees", "subscriptions"}

# Categories that MIGHT be deductible — flag for review
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
    """Return deductible flag, tax_category, and needs_review flag."""
    cat_lower = category.lower()
    desc_lower = description.lower()
    deductible_cats = rules["deductible_categories"]

    # Rule-based deductibility
    rule_deductible = cat_lower in deductible_cats
    # Combine: deductible if either AI or rules say so
    deductible = ai_deductible or rule_deductible

    # Assign ATO tax category if not already set
    tax_category = ai_tax_cat
    if deductible and not tax_category:
        if cat_lower in ("work equipment",):
            tax_category = "Other work-related deductions"
        elif cat_lower in ("self-education",):
            tax_category = "Work-related self-education"
        elif cat_lower in ("internet & phone",):
            tax_category = "Other work-related deductions"
        elif cat_lower in ("donations",):
            tax_category = "Donations"
        elif cat_lower in ("investment fees",):
            tax_category = "Investment income deductions"
        elif cat_lower in ("sole trader expenses",):
            tax_category = "Sole trader business expenses"

    # Flag for review: borderline categories where user should confirm
    needs_review = (
        cat_lower in _REVIEW_CATEGORIES and not ai_deductible
    ) or (
        # Subscription > $50 flagged regardless — could be deductible software
        cat_lower == "subscriptions" and not deductible
    )

    return {"deductible": deductible, "tax_category": tax_category, "needs_review": needs_review}


def _connect_gmail(email_addr: str, app_password: str) -> imaplib.IMAP4_SSL:
    ctx = ssl.create_default_context()
    m = imaplib.IMAP4_SSL("imap.gmail.com", 993, ssl_context=ctx)
    m.sock.settimeout(20)  # 20s per IMAP read — won't affect other sockets (Gemini etc.)
    m.login(email_addr, app_password)
    return m


def _fetch_receipt_emails(email_addr: str, app_password: str, days: int = 90) -> list[dict]:
    """Fetch emails that look like receipts from the last N days."""
    m = _connect_gmail(email_addr, app_password)

    since_date = (datetime.now() - timedelta(days=days)).strftime("%d-%b-%Y")

    # Try Gmail X-GM-RAW search on All Mail — filters on Gmail's side so we only
    # download matching emails, much faster than fetching everything locally.
    # Falls back to per-folder INBOX search if X-GM-RAW unsupported.
    ids = []
    for folder in ('"[Gmail]/All Mail"', 'INBOX'):
        status, _ = m.select(folder)
        if status != 'OK':
            continue

        # Build a Gmail search query: date + receipt-like subjects
        gm_query = (
            f'after:{(datetime.now() - timedelta(days=days)).strftime("%Y/%m/%d")} '
            f'subject:(receipt OR invoice OR order OR payment OR confirmation OR '
            f'statement OR charged OR billing OR subscription OR renewal)'
        )
        try:
            _, data = m.search(None, f'X-GM-RAW "{gm_query}"')
            ids = data[0].split() if data[0] else []
            break  # X-GM-RAW worked
        except Exception:
            pass

        # X-GM-RAW not supported — fall back to plain SINCE search
        try:
            _, data = m.search(None, f'SINCE "{since_date}"')
            ids = data[0].split() if data[0] else []
            break
        except Exception:
            continue

    results = []
    for uid in ids[-50:]:  # X-GM-RAW already filtered by subject; 50 is plenty
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
    # Run all batches concurrently (3 threads max to avoid rate limits)
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
    """Extract meaningful words from a merchant/description string."""
    words = re.findall(r"[a-zA-Z]{4,}", text.lower())
    return {w for w in words if w not in _STOP_WORDS}


def _fuzzy_merchant_match(desc_a: str, desc_b: str) -> bool:
    """True if the two strings share at least one significant word."""
    return bool(_merchant_words(desc_a) & _merchant_words(desc_b))


def _find_matching_txn(
    session: Session,
    amount: float,
    txn_date: date,
    description: str,
    user_id: int,
) -> Optional[Transaction]:
    """
    Find an existing transaction matching amount (±2%), date (±3 days),
    and merchant (fuzzy word overlap). Returns the best match or None.
    """
    from datetime import timedelta
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

    # Prefer fuzzy merchant match; fall back to first candidate by date proximity
    for txn in candidates:
        if _fuzzy_merchant_match(description, txn.description):
            return txn

    # No merchant overlap — still return closest by date if amount is exact-ish
    if len(candidates) == 1:
        return candidates[0]

    # Multiple candidates, no merchant match — skip to avoid false positives
    return None


class ScanRequest(BaseModel):
    days: int = 90
    account_id: Optional[int] = None
    commit: bool = False  # if True, save directly; if False, return preview


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


@router.post("/scan")
def scan_gmail(
    body: ScanRequest,
    session: Session = Depends(get_session),
    current_user: User = Depends(get_current_user),
):
    """Scan Gmail for receipt emails and extract transactions."""
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

    # Determine account to import to (only consider accounts owned by this user)
    account_id = body.account_id
    if not account_id:
        first_account = session.exec(
            select(Account).where(Account.user_id == current_user.id)
        ).first()
        if first_account:
            account_id = first_account.id

    # Fetch emails in a thread so we can enforce a hard timeout.
    # IMPORTANT: do NOT use `with executor` — its __exit__ waits for threads to finish.
    ex = concurrent.futures.ThreadPoolExecutor(max_workers=1)
    future = ex.submit(_fetch_receipt_emails, email_addr, app_password, body.days)
    try:
        emails = future.result(timeout=40)
    except concurrent.futures.TimeoutError:
        ex.shutdown(wait=False)
        raise HTTPException(504, "Gmail scan timed out. Try a shorter date range (7 or 14 days).")
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

    # Extract via AI
    extracted = _extract_transactions_from_emails(emails, provider, api_key)

    if not body.commit:
        # Preview mode — return extracted without saving
        return {
            "scanned": len(emails),
            "found": len(extracted),
            "transactions": extracted,
            "imported": 0,
            "skipped": 0,
        }

    # Load income profile for tax auto-flagging
    occupation = get_setting(session, "occupation") or ""
    income_types = get_setting(session, "income_types") or ""  # comma-separated: employee,investor,sole_trader,rental
    tax_rules = _build_tax_rules(occupation, income_types)

    # Commit mode — save to DB
    categories = session.exec(select(Category)).all()
    cat_map = {c.name.lower(): c for c in categories}
    uncat = next((c for c in categories if c.name == "Uncategorised"), None)

    # Ensure receipt directory exists
    receipt_dir = _os_gmail.path.join(_DATA_DIR_GMAIL, "receipts", "gmail")
    os.makedirs(receipt_dir, exist_ok=True)

    # Build email body lookup by message_id
    email_body_map = {e["message_id"]: e for e in emails}

    imported, skipped = 0, 0
    saved = []
    for item in extracted:
        raw_hash = "gmail:" + hashlib.sha256(
            (item.get("message_id", "") + str(item.get("amount", "")) + item.get("date", "")).encode()
        ).hexdigest()

        # Dedup (scoped to this user)
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

        # Tax deductibility: start with AI suggestion, then apply income-profile rules
        ai_deductible = bool(item.get("is_tax_deductible", False))
        ai_tax_cat = item.get("tax_category")
        tax_result = _apply_tax_rules(cat_name, item.get("description", ""), ai_deductible, ai_tax_cat, tax_rules)

        # Save receipt email as HTML file for audit trail
        receipt_path = None
        msg_id = item.get("message_id", "")
        src_email = email_body_map.get(msg_id)
        if src_email and body.commit:
            safe_name = re.sub(r"[^\w\-]", "_", f"{txn_date}_{item.get('description','receipt')}")[:60]
            receipt_file = os.path.join(receipt_dir, f"{safe_name}_{raw_hash[:8]}.html")
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
                receipt_path = receipt_file
            except Exception:
                pass  # receipt save failure shouldn't block import

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
    """
    Scan Gmail for receipt emails and match them to existing transactions.
    For each match, saves the email as an HTML receipt file and attaches it.
    Returns matched, unmatched, and already-receipted counts.
    """
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

        # Save email as HTML receipt
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


def _item_summary(item: dict) -> dict:
    return {
        "email_description": item.get("description", ""),
        "email_amount": item.get("amount"),
        "email_date": item.get("date"),
        "subject": item.get("subject", ""),
    }
