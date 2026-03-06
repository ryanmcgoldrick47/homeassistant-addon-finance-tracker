from __future__ import annotations

"""Receipt upload / serve / delete for transactions."""

import base64
import hashlib
import json
import os
import uuid
from datetime import date, timedelta
from typing import Optional

from fastapi import APIRouter, Depends, Form, HTTPException, UploadFile, File
from fastapi.responses import FileResponse
from pydantic import BaseModel
from sqlmodel import Session, select

from database import Transaction, Category, get_session, User
from deps import get_current_user, get_setting

router = APIRouter(tags=["receipts"])

_DATA_DIR = os.environ.get("FINANCE_DATA_DIR", "/data")
RECEIPTS_DIR = os.path.join(_DATA_DIR, "receipts")
ALLOWED_TYPES = {
    "image/jpeg", "image/png", "image/gif", "image/webp",
    "application/pdf",
}
MAX_SIZE_MB = 20


def _detect_mime(data: bytes) -> str | None:
    """Detect MIME type from magic bytes."""
    if data[:3] == b"\xff\xd8\xff":
        return "image/jpeg"
    if data[:8] == b"\x89PNG\r\n\x1a\n":
        return "image/png"
    if data[:6] in (b"GIF87a", b"GIF89a"):
        return "image/gif"
    if data[:4] == b"RIFF" and data[8:12] == b"WEBP":
        return "image/webp"
    if data[:4] == b"%PDF":
        return "application/pdf"
    # HEIC/HEIF — ftyp box at offset 4
    if len(data) >= 12 and data[4:8] == b"ftyp":
        brand = data[8:12]
        if brand in (b"heic", b"heix", b"hevc", b"hevx", b"mif1", b"msf1"):
            return "image/heic"
    return None


def _ensure_dir():
    os.makedirs(RECEIPTS_DIR, exist_ok=True)


@router.post("/api/transactions/{txn_id}/receipt")
async def upload_receipt(
    txn_id: int,
    file: UploadFile = File(...),
    session: Session = Depends(get_session),
    current_user: User = Depends(get_current_user),
):
    txn = session.get(Transaction, txn_id)
    if not txn:
        raise HTTPException(404, "Transaction not found")
    if txn.user_id != current_user.id:
        raise HTTPException(403, "Access denied")

    if file.content_type not in ALLOWED_TYPES:
        raise HTTPException(400, f"Unsupported file type: {file.content_type}. Allowed: JPEG, PNG, GIF, WebP, PDF")

    content = await file.read()
    if len(content) > MAX_SIZE_MB * 1024 * 1024:
        raise HTTPException(400, f"File too large (max {MAX_SIZE_MB} MB)")

    _ensure_dir()

    # Remove old receipt if one exists
    if txn.receipt_path:
        old = os.path.join(RECEIPTS_DIR, os.path.basename(txn.receipt_path))
        if os.path.exists(old):
            os.remove(old)

    ext = os.path.splitext(file.filename or "")[1].lower() or ".bin"
    filename = f"txn_{txn_id}_{uuid.uuid4().hex[:8]}{ext}"
    dest = os.path.join(RECEIPTS_DIR, filename)

    with open(dest, "wb") as f:
        f.write(content)

    txn.receipt_path = filename
    session.add(txn)
    session.commit()

    return {"ok": True, "filename": filename}


@router.get("/api/receipts/{filepath:path}")
def serve_receipt(filepath: str):
    # Prevent path traversal — resolve and ensure it stays within RECEIPTS_DIR
    full = os.path.realpath(os.path.join(RECEIPTS_DIR, filepath))
    receipts_root = os.path.realpath(RECEIPTS_DIR)
    if not full.startswith(receipts_root + os.sep) and full != receipts_root:
        raise HTTPException(400, "Invalid path")
    if not os.path.exists(full):
        raise HTTPException(404, "Receipt not found")
    return FileResponse(full, headers={
        "Content-Disposition": f'inline; filename="{os.path.basename(full)}"',
    })


@router.delete("/api/transactions/{txn_id}/receipt")
def delete_receipt(
    txn_id: int,
    session: Session = Depends(get_session),
    current_user: User = Depends(get_current_user),
):
    txn = session.get(Transaction, txn_id)
    if not txn:
        raise HTTPException(404, "Transaction not found")
    if txn.user_id != current_user.id:
        raise HTTPException(403, "Access denied")
    if not txn.receipt_path:
        raise HTTPException(404, "No receipt attached")

    path = os.path.join(RECEIPTS_DIR, os.path.basename(txn.receipt_path))
    if os.path.exists(path):
        os.remove(path)

    txn.receipt_path = None
    session.add(txn)
    session.commit()
    return {"ok": True}


# ── OCR Receipt Scanner ────────────────────────────────────────────────────────

class OcrAttachRequest(BaseModel):
    txn_id: Optional[int] = None          # attach to existing transaction
    merchant: str = ""
    amount: float = 0.0
    date: Optional[str] = None            # ISO YYYY-MM-DD
    category_name: Optional[str] = None
    create_if_no_match: bool = False       # create new transaction if no match
    account_id: Optional[int] = None      # required if create_if_no_match


class OcrScanRequest(BaseModel):
    image: str          # base64-encoded bytes
    content_type: str = "image/jpeg"
    filename: str = "receipt.jpg"


@router.post("/api/receipts/ocr")
async def ocr_receipt(
    body: OcrScanRequest,
    session: Session = Depends(get_session),
    current_user: User = Depends(get_current_user),
):
    """Accept a base64-encoded image/PDF, run AI OCR, return extracted receipt data + best-match transaction."""
    if get_setting(session, "ai_ocr_enabled") == "0":
        raise HTTPException(403, "Receipt OCR is disabled. Enable it in Settings → AI Features.")
    try:
        content = base64.b64decode(body.image)
    except Exception:
        raise HTTPException(400, "Invalid base64 image data")

    if len(content) > MAX_SIZE_MB * 1024 * 1024:
        raise HTTPException(400, f"File too large (max {MAX_SIZE_MB} MB)")

    # Detect actual MIME type from magic bytes — don't trust browser-reported type
    actual_mime = _detect_mime(content) or body.content_type or "image/jpeg"
    if actual_mime not in ALLOWED_TYPES:
        if actual_mime in ("image/heic", "image/heif"):
            raise HTTPException(400,
                "iPhone HEIC photos are not supported. On your iPhone go to "
                "Settings → Camera → Formats → Most Compatible to shoot in JPEG instead.")
        raise HTTPException(400, f"Unsupported file type: {actual_mime}. Allowed: JPEG, PNG, GIF, WebP, PDF")

    # Determine AI provider
    provider = get_setting(session, "ai_provider") or "gemini"
    if provider == "gemini":
        api_key = get_setting(session, "gemini_api_key") or os.environ.get("GEMINI_API_KEY", "")
    else:
        api_key = get_setting(session, "anthropic_api_key") or os.environ.get("ANTHROPIC_API_KEY", "")

    if not api_key:
        raise HTTPException(400, "No AI API key configured — add one in Settings")

    # Extract receipt data with AI
    extracted = _ocr_with_ai(content, actual_mime, provider, api_key)
    if not extracted.get("ok"):
        raise HTTPException(422, extracted.get("error", "Could not extract receipt data"))

    merchant = extracted.get("merchant", "") or ""
    try:
        amount = float(extracted.get("amount") or 0)
    except (ValueError, TypeError):
        amount = 0.0
    currency = (extracted.get("currency") or "AUD").upper().strip()
    try:
        amount_aud = float(extracted.get("amount_aud") or 0) or (amount if currency == "AUD" else None)
    except (ValueError, TypeError):
        amount_aud = amount if currency == "AUD" else None
    receipt_date_str = extracted.get("date") or date.today().isoformat()
    category_name = extracted.get("category", "")
    notes = extracted.get("notes", "")

    # Parse date
    try:
        receipt_date = date.fromisoformat(receipt_date_str)
    except Exception:
        receipt_date = date.today()

    # Find best-matching transaction
    match = None
    match_score = 0.0
    if amount > 0:
        lo = receipt_date - timedelta(days=3)
        hi = receipt_date + timedelta(days=3)
        candidates = session.exec(
            select(Transaction).where(
                Transaction.user_id == current_user.id,
                Transaction.is_credit == False,
                Transaction.date >= lo,
                Transaction.date <= hi,
                Transaction.amount >= amount * 0.95,
                Transaction.amount <= amount * 1.05,
            )
        ).all()
        if candidates:
            # Score: exact date + amount gets highest score
            best = None
            best_score = 0.0
            for c in candidates:
                s = 0.5  # base: amount match
                if c.date == receipt_date:
                    s += 0.4
                elif abs((c.date - receipt_date).days) <= 1:
                    s += 0.2
                if merchant and any(w.lower() in c.description.lower() for w in merchant.split()[:3] if len(w) > 3):
                    s += 0.1
                if s > best_score:
                    best_score = s
                    best = c
            if best:
                match = best
                match_score = round(best_score, 2)

    match_data = None
    if match:
        cat = session.get(Category, match.category_id) if match.category_id else None
        match_data = {
            "id": match.id,
            "date": str(match.date),
            "description": match.description,
            "amount": match.amount,
            "category": cat.name if cat else None,
            "has_receipt": bool(match.receipt_path),
            "match_score": match_score,
        }

    return {
        "ok": True,
        "extracted": {
            "merchant": merchant,
            "amount": amount,
            "currency": currency,
            "amount_aud": amount_aud,
            "date": str(receipt_date),
            "category": category_name,
            "notes": notes,
        },
        "match": match_data,
        # Save image temporarily so we can attach it in the confirm step
        "_temp_content": base64.b64encode(content).decode(),
        "_temp_mime": actual_mime,
        "_temp_filename": body.filename or f"receipt_{uuid.uuid4().hex[:8]}.jpg",
    }


class OcrAttachJsonRequest(BaseModel):
    image: str                          # base64 from OCR response _temp_content
    content_type: str = "image/jpeg"
    filename: str = "receipt.jpg"
    txn_id: Optional[int] = None
    merchant: str = ""
    amount: float = 0.0
    currency: str = "AUD"
    amount_aud: Optional[float] = None  # AUD equivalent (if foreign)
    receipt_date: Optional[str] = None
    category_name: Optional[str] = None
    create_if_no_match: bool = False
    account_id: Optional[int] = None
    tax_deductible: bool = False
    is_reimbursable: bool = False


@router.post("/api/receipts/ocr/attach")
async def ocr_attach(
    body: OcrAttachJsonRequest,
    session: Session = Depends(get_session),
    current_user: User = Depends(get_current_user),
):
    """Attach the scanned receipt to a transaction (existing or new)."""
    try:
        content = base64.b64decode(body.image)
    except Exception:
        raise HTTPException(400, "Invalid base64 image data")

    txn_id = body.txn_id
    merchant = body.merchant
    amount = body.amount_aud if (body.amount_aud and body.currency != "AUD") else body.amount
    receipt_date = body.receipt_date
    category_name = body.category_name
    create_if_no_match = body.create_if_no_match
    account_id = body.account_id
    is_foreign = body.currency not in ("AUD", "")
    notes_extra = ""
    if is_foreign and body.amount:
        notes_extra = f"Original: {body.currency} {body.amount:.2f}"

    _ensure_dir()
    ext = os.path.splitext(body.filename or "")[1].lower() or ".jpg"

    if txn_id:
        txn = session.get(Transaction, txn_id)
        if not txn or txn.user_id != current_user.id:
            raise HTTPException(404, "Transaction not found")
        # Apply flags to existing transaction
        txn.tax_deductible = body.tax_deductible
        txn.is_reimbursable = body.is_reimbursable
        if is_foreign:
            txn.is_overseas = True
            txn.currency_code = body.currency
        if notes_extra and not (txn.notes or "").startswith("Original:"):
            txn.notes = (notes_extra + (" · " + txn.notes if txn.notes else "")).strip()
    elif create_if_no_match:
        # Resolve category
        cat_id = None
        if category_name:
            cat = session.exec(
                select(Category).where(Category.name == category_name)
            ).first()
            cat_id = cat.id if cat else None

        try:
            txn_date = date.fromisoformat(receipt_date) if receipt_date else date.today()
        except Exception:
            txn_date = date.today()

        raw = f"{txn_date}|{merchant or 'OCR Receipt Import'}|{amount:.2f}|ocr"
        raw_hash = hashlib.sha256(raw.encode()).hexdigest()

        txn = Transaction(
            account_id=account_id,
            date=txn_date,
            description=merchant or "OCR Receipt Import",
            amount=amount,
            is_credit=False,
            category_id=cat_id,
            raw_hash=raw_hash,
            user_id=current_user.id,
            tax_deductible=body.tax_deductible,
            is_reimbursable=body.is_reimbursable,
            is_overseas=is_foreign,
            currency_code=body.currency if is_foreign else None,
            notes=notes_extra or None,
        )
        session.add(txn)
        session.flush()  # get txn.id
    else:
        raise HTTPException(400, "Provide txn_id or set create_if_no_match=true")

    # Remove old receipt
    if txn.receipt_path:
        old = os.path.join(RECEIPTS_DIR, os.path.basename(txn.receipt_path))
        if os.path.exists(old):
            os.remove(old)

    filename = f"txn_{txn.id}_{uuid.uuid4().hex[:8]}{ext}"
    dest = os.path.join(RECEIPTS_DIR, filename)
    with open(dest, "wb") as f:
        f.write(content)

    txn.receipt_path = filename
    session.add(txn)
    session.commit()

    return {"ok": True, "txn_id": txn.id, "filename": filename, "created": not bool(txn_id)}


def _ocr_with_ai(content: bytes, mime_type: str, provider: str, api_key: str) -> dict:
    """Send image to AI and extract receipt fields as JSON."""
    prompt = """You are reading a receipt image. Extract the following and return ONLY valid JSON with no markdown:
{
  "merchant": "store/merchant name",
  "amount": 12.34,
  "currency": "AUD",
  "amount_aud": 12.34,
  "date": "YYYY-MM-DD",
  "category": "one of: Groceries, Dining & Takeaway, Coffee & Snacks, Transport, Fuel, Health & Medical, Pharmacy, Personal Care, Entertainment, Shopping & Clothing, Home & Garden, Utilities, Internet & Phone, Insurance, Subscriptions, Work Equipment, ATM / Cash, Other",
  "notes": "any useful extra info (optional)"
}
Rules: amount is the total paid in the receipt's original currency (a positive number). currency is the 3-letter ISO code (e.g. AUD, USD, EUR, GBP, JPY). amount_aud is the AUD equivalent — if the receipt is already AUD set amount_aud equal to amount; if foreign currency use the approximate exchange rate if visible on the receipt, otherwise set amount_aud to null. date in YYYY-MM-DD (use today if not visible). If you cannot read the receipt, return {"ok": false, "error": "reason"}."""

    try:
        if provider == "gemini":
            from google import genai
            from google.genai import types as gtypes
            client = genai.Client(api_key=api_key)
            # PDF: use inline_data directly; images: same
            part_image = gtypes.Part.from_bytes(data=content, mime_type=mime_type)
            response = client.models.generate_content(
                model="gemini-2.5-flash",
                contents=[prompt, part_image],
                config=gtypes.GenerateContentConfig(response_mime_type="application/json"),
            )
            raw = response.text.strip()
        else:
            import anthropic
            b64 = base64.standard_b64encode(content).decode()
            media = mime_type if mime_type in ("image/jpeg","image/png","image/gif","image/webp") else "image/jpeg"
            client = anthropic.Anthropic(api_key=api_key)
            msg = client.messages.create(
                model="claude-haiku-4-5-20251001",
                max_tokens=512,
                messages=[{
                    "role": "user",
                    "content": [
                        {"type": "image", "source": {"type": "base64", "media_type": media, "data": b64}},
                        {"type": "text", "text": prompt},
                    ],
                }],
            )
            raw = msg.content[0].text.strip()

        # Strip markdown fences if present
        if raw.startswith("```"):
            raw = raw.split("```")[1]
            if raw.startswith("json"):
                raw = raw[4:]
        data = json.loads(raw)
        if data.get("ok") is False:
            return data
        data["ok"] = True
        return data

    except Exception as e:
        return {"ok": False, "error": str(e)}
