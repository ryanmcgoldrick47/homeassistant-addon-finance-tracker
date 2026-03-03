from __future__ import annotations

"""Receipt upload / serve / delete for transactions."""

import os
import shutil
import uuid

from fastapi import APIRouter, Depends, HTTPException, UploadFile, File
from fastapi.responses import FileResponse
from sqlmodel import Session

from database import Transaction, get_session

router = APIRouter(tags=["receipts"])

RECEIPTS_DIR = "/config/finance_tracker/receipts"
ALLOWED_TYPES = {
    "image/jpeg", "image/png", "image/gif", "image/webp",
    "application/pdf",
}
MAX_SIZE_MB = 20


def _ensure_dir():
    os.makedirs(RECEIPTS_DIR, exist_ok=True)


@router.post("/api/transactions/{txn_id}/receipt")
async def upload_receipt(
    txn_id: int,
    file: UploadFile = File(...),
    session: Session = Depends(get_session),
):
    txn = session.get(Transaction, txn_id)
    if not txn:
        raise HTTPException(404, "Transaction not found")

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


@router.get("/api/receipts/{filename}")
def serve_receipt(filename: str):
    # Prevent path traversal
    if "/" in filename or "\\" in filename or ".." in filename:
        raise HTTPException(400, "Invalid filename")
    path = os.path.join(RECEIPTS_DIR, filename)
    if not os.path.exists(path):
        raise HTTPException(404, "Receipt not found")
    return FileResponse(path)


@router.delete("/api/transactions/{txn_id}/receipt")
def delete_receipt(
    txn_id: int,
    session: Session = Depends(get_session),
):
    txn = session.get(Transaction, txn_id)
    if not txn:
        raise HTTPException(404, "Transaction not found")
    if not txn.receipt_path:
        raise HTTPException(404, "No receipt attached")

    path = os.path.join(RECEIPTS_DIR, os.path.basename(txn.receipt_path))
    if os.path.exists(path):
        os.remove(path)

    txn.receipt_path = None
    session.add(txn)
    session.commit()
    return {"ok": True}
