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

_DEFAULT_SESSION_TTL_DAYS      = 7     # absolute TTL
_DEFAULT_INACTIVITY_HOURS      = 24    # auto-expire if unused for this long
_LAST_USED_UPDATE_INTERVAL_MIN = 5     # only write last_used_at every N minutes


def _get_session_ttl_days(session: Session) -> int:
    try:
        v = get_setting(session, "session_ttl_days", str(_DEFAULT_SESSION_TTL_DAYS))
        return max(1, min(int(v), 90))
    except Exception:
        return _DEFAULT_SESSION_TTL_DAYS


def _get_inactivity_hours(session: Session) -> int:
    try:
        v = get_setting(session, "session_inactivity_hours", str(_DEFAULT_INACTIVITY_HOURS))
        return max(1, min(int(v), 720))  # 1h – 30 days
    except Exception:
        return _DEFAULT_INACTIVITY_HOURS


def create_session(user_id: int, session: Session) -> str:
    ttl_days = _get_session_ttl_days(session)
    now = datetime.now()
    token = secrets.token_hex(32)
    expires = (now + timedelta(days=ttl_days)).isoformat(timespec="seconds")
    sess = UserSession(token=token, user_id=user_id, expires_at=expires,
                       last_used_at=now.isoformat(timespec="seconds"))
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

    now = datetime.now()

    # Check absolute TTL
    if datetime.fromisoformat(sess.expires_at) < now:
        session.delete(sess)
        session.commit()
        raise HTTPException(401, "Session expired — please log in again")

    # Check inactivity timeout
    inactivity_hours = _get_inactivity_hours(session)
    last_used = sess.last_used_at or sess.created_at
    if last_used:
        try:
            idle_since = datetime.fromisoformat(last_used)
            if (now - idle_since).total_seconds() > inactivity_hours * 3600:
                session.delete(sess)
                session.commit()
                raise HTTPException(401, "Session expired due to inactivity — please log in again")
        except HTTPException:
            raise
        except Exception:
            pass

    # Update last_used_at (throttled — only every N minutes)
    try:
        last_used_dt = datetime.fromisoformat(last_used) if last_used else now
        if (now - last_used_dt).total_seconds() > _LAST_USED_UPDATE_INTERVAL_MIN * 60:
            sess.last_used_at = now.isoformat(timespec="seconds")
            session.add(sess)
            session.commit()
    except Exception:
        pass

    user = session.get(User, sess.user_id)
    if not user:
        raise HTTPException(401, "User not found")
    return user


def purge_expired_sessions(session: Session) -> int:
    """Delete sessions past their absolute TTL. Call from background task."""
    now_str = datetime.now().isoformat(timespec="seconds")
    from sqlmodel import delete
    from database import UserSession as _US
    expired = session.exec(select(_US).where(_US.expires_at < now_str)).all()
    for s in expired:
        session.delete(s)
    if expired:
        session.commit()
    return len(expired)
