from __future__ import annotations

"""Receipt upload / serve / delete for transactions."""

import os
import shutil
import uuid

from fastapi import APIRouter, Depends, HTTPException, UploadFile, File
from fastapi.responses import FileResponse
from sqlmodel import Session

from database import Transaction, get_session, User
from deps import get_current_user

router = APIRouter(tags=["receipts"])

_DATA_DIR = os.environ.get("FINANCE_DATA_DIR", "/data")
RECEIPTS_DIR = os.path.join(_DATA_DIR, "receipts")
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
