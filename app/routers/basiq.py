"""Basiq Open Banking integration — CDR bank feed for Australian accounts."""
from __future__ import annotations

import base64
import hashlib
import os
from datetime import date, datetime
from typing import Optional

import httpx
from fastapi import APIRouter, Depends, HTTPException
from sqlmodel import Session, select

from database import Account, Category, Transaction, get_session, User
from deps import get_setting, set_setting as save_setting, get_current_user

router = APIRouter(prefix="/api/basiq", tags=["basiq"])

BASIQ_BASE = "https://au-api.basiq.io"


# ---------------------------------------------------------------------------
# Basiq API helpers
# ---------------------------------------------------------------------------

def _b64(s: str) -> str:
    return base64.b64encode(s.encode()).decode()


def _server_token(api_key: str) -> str:
    """Get a SERVER_ACCESS bearer token."""
    r = httpx.post(
        f"{BASIQ_BASE}/token",
        headers={
            "Authorization": f"Basic {_b64(api_key + ':')}",
            "Content-Type": "application/x-www-form-urlencoded",
            "basiq-version": "3.0",
        },
        content="scope=SERVER_ACCESS",
        timeout=15,
    )
    if r.status_code != 200:
        raise HTTPException(502, f"Basiq auth failed — check your API key. ({r.status_code}: {r.text[:200]})")
    return r.json()["access_token"]


def _client_token(api_key: str, basiq_user_id: str) -> str:
    """Get a CLIENT_ACCESS token scoped to a specific user (required for auth links)."""
    r = httpx.post(
        f"{BASIQ_BASE}/token",
        headers={
            "Authorization": f"Basic {_b64(api_key + ':')}",
            "Content-Type": "application/x-www-form-urlencoded",
            "basiq-version": "3.0",
        },
        content=f"scope=CLIENT_ACCESS&userId={basiq_user_id}",
        timeout=15,
    )
    if r.status_code != 200:
        raise HTTPException(502, f"Basiq client token error: {r.text[:200]}")
    return r.json()["access_token"]


def _make_hash(d: date, description: str, amount: float) -> str:
    """Same hashing logic as CSV import for cross-source deduplication."""
    raw = f"{d}|{description.strip()}|{round(amount, 2)}"
    return hashlib.sha256(raw.encode()).hexdigest()


# ---------------------------------------------------------------------------
# Endpoints
# ---------------------------------------------------------------------------

@router.get("/status")
def basiq_status(
    session: Session = Depends(get_session),
    current_user: User = Depends(get_current_user),
):
    api_key    = get_setting(session, "basiq_api_key", "")
    basiq_uid  = get_setting(session, "basiq_user_id", "")
    last_sync  = get_setting(session, "basiq_last_sync", "")
    last_count = get_setting(session, "basiq_last_sync_count", "")
    return {
        "configured": bool(api_key),
        "connected":  bool(basiq_uid),
        "user_id":    basiq_uid or None,
        "last_sync":  last_sync or None,
        "last_sync_count": int(last_count) if last_count else None,
    }


@router.post("/connect")
def basiq_connect(
    session: Session = Depends(get_session),
    current_user: User = Depends(get_current_user),
):
    """Create (or reuse) a Basiq user and return an auth link for the user to connect their bank."""
    api_key = get_setting(session, "basiq_api_key", "")
    if not api_key:
        raise HTTPException(400, "Basiq API key not configured. Add it in Settings → Open Banking.")

    server_tok = _server_token(api_key)

    # Create user once; store basiq_user_id for all future calls
    basiq_uid = get_setting(session, "basiq_user_id", "")
    if not basiq_uid:
        r = httpx.post(
            f"{BASIQ_BASE}/users",
            headers={
                "Authorization": f"Bearer {server_tok}",
                "Content-Type": "application/json",
                "basiq-version": "3.0",
            },
            json={"email": "user@finance-tracker.local"},
            timeout=15,
        )
        if r.status_code not in (200, 201):
            raise HTTPException(502, f"Could not create Basiq user: {r.text[:200]}")
        basiq_uid = r.json()["id"]
        save_setting(session, "basiq_user_id", basiq_uid)

    # Client token required for auth link
    client_tok = _client_token(api_key, basiq_uid)

    r = httpx.post(
        f"{BASIQ_BASE}/users/{basiq_uid}/auth_link",
        headers={
            "Authorization": f"Bearer {client_tok}",
            "Content-Type": "application/json",
            "basiq-version": "3.0",
        },
        json={},
        timeout=15,
    )
    if r.status_code not in (200, 201):
        raise HTTPException(502, f"Could not create auth link: {r.text[:200]}")

    link = r.json().get("links", {}).get("public", "")
    if not link:
        raise HTTPException(502, "Basiq did not return an auth link URL")

    return {"auth_link": link, "user_id": basiq_uid}


@router.post("/sync")
def basiq_sync(
    days: int = 90,
    session: Session = Depends(get_session),
    current_user: User = Depends(get_current_user),
):
    """Pull the last N days of posted transactions from Basiq and import new ones."""
    api_key   = get_setting(session, "basiq_api_key", "")
    basiq_uid = get_setting(session, "basiq_user_id", "")
    if not api_key or not basiq_uid:
        raise HTTPException(400, "Not connected to Basiq. Configure your API key and connect a bank first.")

    server_tok = _server_token(api_key)
    acct_map   = _sync_accounts(server_tok, basiq_uid, session, current_user.id)
    uncat_id   = _uncategorised_id(session)
    from_date  = date.fromordinal(date.today().toordinal() - days)

    imported = skipped = 0
    url    = f"{BASIQ_BASE}/users/{basiq_uid}/transactions"
    params = {
        "filter": f"transaction.status.eq('posted'),transaction.postDate.gteq('{from_date}')",
        "limit": "500",
    }

    while url:
        r = httpx.get(
            url,
            headers={"Authorization": f"Bearer {server_tok}", "basiq-version": "3.0"},
            params=params,
            timeout=30,
        )
        params = None  # only first request uses query params; subsequent use next link
        if r.status_code != 200:
            raise HTTPException(502, f"Basiq transactions error: {r.text[:200]}")

        body = r.json()
        for txn in body.get("data", []):
            status = _import_transaction(txn, acct_map, uncat_id, session, current_user.id)
            if status == "imported":
                imported += 1
            else:
                skipped += 1

        next_link = body.get("links", {}).get("next")
        url = next_link if next_link and next_link != url else None

    session.commit()
    save_setting(session, "basiq_last_sync", datetime.now().isoformat(timespec="seconds"))
    save_setting(session, "basiq_last_sync_count", str(imported))

    return {"imported": imported, "skipped": skipped, "days": days}


@router.delete("/disconnect")
def basiq_disconnect(
    session: Session = Depends(get_session),
    current_user: User = Depends(get_current_user),
):
    """Clear the Basiq connection (keeps the API key, removes user binding)."""
    save_setting(session, "basiq_user_id", "")
    save_setting(session, "basiq_last_sync", "")
    save_setting(session, "basiq_last_sync_count", "")
    return {"ok": True}


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

def _sync_accounts(server_tok: str, basiq_uid: str, session: Session, db_user_id: int) -> dict[str, int]:
    """Fetch Basiq accounts and create matching Account rows in our DB.
    Returns mapping of basiq_account_id → our Account.id.
    """
    r = httpx.get(
        f"{BASIQ_BASE}/users/{basiq_uid}/accounts",
        headers={"Authorization": f"Bearer {server_tok}", "basiq-version": "3.0"},
        timeout=15,
    )
    if r.status_code != 200:
        return {}  # non-fatal; transactions will have no account attached

    mapping: dict[str, int] = {}
    for ba in r.json().get("data", []):
        basiq_id = ba.get("id", "")
        if not basiq_id:
            continue

        marker = f"basiq:{basiq_id}"
        existing = session.exec(
            select(Account).where(
                Account.account_number == marker,
                Account.user_id == db_user_id,
            )
        ).first()
        if existing:
            mapping[basiq_id] = existing.id
            continue

        # Build a friendly display name from institution + account type
        institution = ba.get("institution") or {}
        inst_name   = institution.get("shortName") or (institution if isinstance(institution, str) else "Bank")
        acct_class  = ba.get("class") or {}
        acct_type   = acct_class.get("product") or acct_class.get("type") or ba.get("type") or "Account"
        name        = f"{inst_name} {acct_type.replace('-', ' ').title()}"

        acct = Account(name=name, bank=str(inst_name), account_number=marker, user_id=db_user_id)
        session.add(acct)
        session.flush()
        mapping[basiq_id] = acct.id

    return mapping


def _uncategorised_id(session: Session) -> Optional[int]:
    cat = session.exec(select(Category).where(Category.name == "Uncategorised")).first()
    return cat.id if cat else None


def _import_transaction(
    txn: dict,
    acct_map: dict[str, int],
    uncat_id: Optional[int],
    session: Session,
    user_id: int,
) -> str:
    """Import one Basiq transaction. Returns 'imported' or 'skipped'."""
    try:
        post_date_str = txn.get("postDate") or txn.get("transactionDate")
        if not post_date_str:
            return "skipped"

        txn_date = date.fromisoformat(post_date_str[:10])
        raw_desc = (txn.get("description") or "").strip()
        if not raw_desc:
            return "skipped"

        amt_str   = str(txn.get("amount", "0")).replace(",", "")
        amt_float = float(amt_str)
        amount    = abs(amt_float)
        is_credit = amt_float > 0

        # Use same hash as CSV import — prevents duplicates across both sources
        raw_hash = _make_hash(txn_date, raw_desc, amount)
        if session.exec(
            select(Transaction).where(
                Transaction.raw_hash == raw_hash,
                Transaction.user_id == user_id,
            )
        ).first():
            return "skipped"

        # Prefer enriched merchant name as the display description
        enrich        = txn.get("enrich") or {}
        merchant_name = ((enrich.get("merchant") or {}).get("businessName") or "").strip()
        description   = merchant_name if merchant_name else raw_desc

        basiq_acct_id = (txn.get("account") or {}).get("id", "")
        account_id    = acct_map.get(basiq_acct_id)

        session.add(Transaction(
            account_id=account_id,
            date=txn_date,
            description=description,
            amount=amount,
            is_credit=is_credit,
            category_id=uncat_id,
            raw_hash=raw_hash,
            user_id=user_id,
        ))
        return "imported"
    except Exception:
        return "skipped"
