"""Authentication — register, login, logout, session management."""
from __future__ import annotations

import asyncio
import time
from collections import defaultdict

from fastapi import APIRouter, Depends, HTTPException, Request
from pydantic import BaseModel
from sqlmodel import Session, select, func

from database import LoginAuditLog, User, UserSession, get_session
from deps import create_session, get_current_user, hash_password, verify_password

router = APIRouter(prefix="/api/auth", tags=["auth"])

# ---------------------------------------------------------------------------
# Brute-force tracking (in-memory, resets on server restart)
# ---------------------------------------------------------------------------
_fail_times: dict[str, list[float]] = defaultdict(list)
_notified: set[str] = set()

_FAIL_WINDOW    = 600.0   # 10 minutes
_FAIL_THRESHOLD = 5       # alert after this many failures
_LOCKOUT_LIMIT  = 10      # hard lockout after this many
_LOCKOUT_TTL    = 900.0   # lockout duration (15 min)


def _get_ip(request: Request) -> str:
    fwd = request.headers.get("X-Forwarded-For", "")
    if fwd:
        return fwd.split(",")[0].strip()
    return request.client.host if request.client else "unknown"


def _prune(key: str) -> list[float]:
    now = time.monotonic()
    times = _fail_times[key]
    times[:] = [t for t in times if now - t < _FAIL_WINDOW]
    return times


def _is_locked_out(key: str) -> bool:
    return len(_prune(key)) >= _LOCKOUT_LIMIT


def _record_fail(key: str) -> int:
    times = _prune(key)
    times.append(time.monotonic())
    return len(times)


def _log_attempt(session: Session, user_id, username: str, ip: str, success: bool, reason: str):
    try:
        log = LoginAuditLog(
            user_id=user_id, username=username, ip_address=ip,
            success=success, reason=reason,
        )
        session.add(log)
        session.commit()
    except Exception:
        pass


async def _fire_security_alert(title: str, body: str):
    """Send HA notification — silently fails if not configured."""
    try:
        from routers.notify import _send_notification, _get_notify_config
        from sqlmodel import Session as _Session
        from database import engine
        with _Session(engine) as s:
            ha_url, token, targets = _get_notify_config(s)
        if token and targets:
            await _send_notification(ha_url, token, targets, title, body)
    except Exception:
        pass


async def _check_brute_and_notify(ip: str, ip_key: str, user_key: str, username: str, fail_count: int):
    """Fire HA notification if brute-force threshold reached (once per window)."""
    if fail_count >= _FAIL_THRESHOLD and ip_key not in _notified:
        _notified.add(ip_key)
        title = "🚨 Finance Tracker — Suspicious Login Activity"
        msg = (
            f"{fail_count} failed login attempt(s) detected from {ip} "
            f"(targeting '{username}'). Review your login history."
        )
        await _fire_security_alert(title, msg)


# ---------------------------------------------------------------------------
# Request/response schemas
# ---------------------------------------------------------------------------

class AuthRequest(BaseModel):
    name: str
    password: str


class UpdateMeRequest(BaseModel):
    name: str | None = None
    password: str | None = None
    color_hex: str | None = None


def _user_dict(user: User) -> dict:
    return {"id": user.id, "name": user.name, "color_hex": user.color_hex}


# ---------------------------------------------------------------------------
# Endpoints
# ---------------------------------------------------------------------------

@router.post("/register")
def register(body: AuthRequest, session: Session = Depends(get_session)):
    """Create a new account. Name must be unique (case-insensitive)."""
    name = body.name.strip()
    if not name:
        raise HTTPException(400, "Name is required")
    if len(body.password) < 8:
        raise HTTPException(400, "Password must be at least 8 characters")

    existing = session.exec(
        select(User).where(func.lower(User.name) == name.lower())
    ).first()
    if existing:
        raise HTTPException(409, f"An account named '{name}' already exists")

    user = User(name=name, password_hash=hash_password(body.password))
    session.add(user)
    session.commit()
    session.refresh(user)

    token = create_session(user.id, session)
    return {"token": token, "user": _user_dict(user)}


@router.post("/login")
async def login(body: AuthRequest, request: Request, session: Session = Depends(get_session)):
    """Log in with name and password."""
    ip = _get_ip(request)
    ip_key   = f"ip:{ip}"
    user_key = f"user:{body.name.strip().lower()}"

    # Hard lockout check
    if _is_locked_out(ip_key) or _is_locked_out(user_key):
        _log_attempt(session, None, body.name.strip(), ip, False, "locked_out")
        raise HTTPException(429, "Too many failed attempts — please wait 15 minutes before trying again")

    user = session.exec(
        select(User).where(func.lower(User.name) == body.name.strip().lower())
    ).first()

    if not user:
        count = _record_fail(ip_key)
        _record_fail(user_key)
        _log_attempt(session, None, body.name.strip(), ip, False, "user_not_found")
        await _check_brute_and_notify(ip, ip_key, user_key, body.name.strip(), count)
        raise HTTPException(401, "Incorrect name or password")

    # Allow login without password if no password has been set yet (first-time setup)
    if user.password_hash and not verify_password(body.password, user.password_hash):
        count = _record_fail(ip_key)
        _record_fail(user_key)
        _log_attempt(session, user.id, body.name.strip(), ip, False, "wrong_password")
        await _check_brute_and_notify(ip, ip_key, user_key, body.name.strip(), count)
        raise HTTPException(401, "Incorrect name or password")

    # Success — clear fail tracking
    _fail_times.pop(ip_key, None)
    _fail_times.pop(user_key, None)
    _notified.discard(ip_key)
    _notified.discard(user_key)

    _log_attempt(session, user.id, body.name.strip(), ip, True, "ok")
    token = create_session(user.id, session)
    return {"token": token, "user": _user_dict(user)}


@router.get("/me")
def get_me(current_user: User = Depends(get_current_user)):
    return _user_dict(current_user)


@router.patch("/me")
async def update_me(
    body: UpdateMeRequest,
    current_user: User = Depends(get_current_user),
    session: Session = Depends(get_session),
):
    password_changed = False

    if body.name is not None:
        name = body.name.strip()
        if not name:
            raise HTTPException(400, "Name cannot be empty")
        conflict = session.exec(
            select(User).where(func.lower(User.name) == name.lower(), User.id != current_user.id)
        ).first()
        if conflict:
            raise HTTPException(409, f"Name '{name}' is already taken")
        current_user.name = name

    if body.password is not None:
        if len(body.password) < 8:
            raise HTTPException(400, "Password must be at least 8 characters")
        current_user.password_hash = hash_password(body.password)
        password_changed = True

    if body.color_hex is not None:
        current_user.color_hex = body.color_hex

    session.add(current_user)
    session.commit()

    # Notify on password change if enabled
    if password_changed:
        try:
            from deps import get_setting
            notify_pw = get_setting(session, "security_notify_password_change", "1") == "1"
            if notify_pw:
                await _fire_security_alert(
                    "🔑 Finance Tracker — Password Changed",
                    f"The password for '{current_user.name}' was just changed. If this wasn't you, revoke all sessions immediately.",
                )
        except Exception:
            pass

    return _user_dict(current_user)


@router.post("/logout")
def logout(
    authorization: str = __import__("fastapi").Header(default=None, alias="authorization"),
    session: Session = Depends(get_session),
):
    """Invalidate the current session token."""
    if authorization and authorization.startswith("Bearer "):
        token = authorization[7:]
        sess = session.get(UserSession, token)
        if sess:
            session.delete(sess)
            session.commit()
    return {"ok": True}


@router.get("/users")
def list_users(
    current_user: User = Depends(get_current_user),
    session: Session = Depends(get_session),
):
    """List all user accounts (so the logged-in user can see who else exists)."""
    users = session.exec(select(User)).all()
    return [_user_dict(u) for u in users]
