from __future__ import annotations

import hashlib
import os
import secrets
from datetime import datetime, timedelta

from fastapi import Depends, Header, HTTPException
from sqlmodel import Session, select

from database import Setting, User, UserSession, get_session


# ---------------------------------------------------------------------------
# Settings helpers
# ---------------------------------------------------------------------------

def get_setting(session: Session, key: str, default: str = "") -> str:
    s = session.get(Setting, key)
    return s.value if s else default


def set_setting(session: Session, key: str, value: str):
    s = session.get(Setting, key)
    if s:
        s.value = value
    else:
        s = Setting(key=key, value=value)
        session.add(s)
    session.commit()


# ---------------------------------------------------------------------------
# Password hashing (pbkdf2_hmac — built-in, no extra deps)
# ---------------------------------------------------------------------------

def hash_password(password: str) -> str:
    salt = os.urandom(16)
    dk = hashlib.pbkdf2_hmac("sha256", password.encode(), salt, 100_000)
    return salt.hex() + ":" + dk.hex()


def verify_password(password: str, stored: str) -> bool:
    try:
        salt_hex, dk_hex = stored.split(":", 1)
        salt = bytes.fromhex(salt_hex)
        dk = hashlib.pbkdf2_hmac("sha256", password.encode(), salt, 100_000)
        return secrets.compare_digest(dk.hex(), dk_hex)
    except Exception:
        return False


# ---------------------------------------------------------------------------
# Session management
# ---------------------------------------------------------------------------

SESSION_TTL_DAYS = 30


def create_session(user_id: int, session: Session) -> str:
    token = secrets.token_hex(32)
    expires = (datetime.now() + timedelta(days=SESSION_TTL_DAYS)).isoformat(timespec="seconds")
    sess = UserSession(token=token, user_id=user_id, expires_at=expires)
    session.add(sess)
    session.commit()
    return token


def get_current_user(
    authorization: str = Header(default=None),
    session: Session = Depends(get_session),
) -> User:
    if not authorization or not authorization.startswith("Bearer "):
        raise HTTPException(401, "Not authenticated")
    token = authorization[7:]
    sess = session.get(UserSession, token)
    if not sess:
        raise HTTPException(401, "Invalid or expired session")
    if datetime.fromisoformat(sess.expires_at) < datetime.now():
        session.delete(sess)
        session.commit()
        raise HTTPException(401, "Session expired — please log in again")
    user = session.get(User, sess.user_id)
    if not user:
        raise HTTPException(401, "User not found")
    return user
