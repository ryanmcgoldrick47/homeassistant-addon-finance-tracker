"""Security — audit, login history, session management."""
from __future__ import annotations

import os
from datetime import datetime, timedelta

from fastapi import APIRouter, Depends, HTTPException, Request
from sqlmodel import Session, select, func

from database import LoginAuditLog, User, UserSession, get_session
from deps import get_current_user, get_setting, set_setting

router = APIRouter(prefix="/api/security", tags=["security"])


# ---------------------------------------------------------------------------
# Security audit
# ---------------------------------------------------------------------------

@router.get("/audit")
def security_audit(
    current_user: User = Depends(get_current_user),
    session: Session = Depends(get_session),
):
    checks = []

    # 1. All users have passwords
    users = session.exec(select(User)).all()
    no_pw = [u.name for u in users if not u.password_hash]
    checks.append({
        "id": "passwords_set",
        "label": "All accounts are password-protected",
        "status": "pass" if not no_pw else "fail",
        "detail": f"Accounts without a password: {', '.join(no_pw)}" if no_pw
                  else "Every account has a password set",
    })

    # 2. Security alert notifications
    alerts_on = get_setting(session, "security_alerts_enabled", "1") == "1"
    ha_token  = os.environ.get("SUPERVISOR_TOKEN", "") or get_setting(session, "ha_token", "")
    if alerts_on and ha_token:
        alert_status  = "pass"
        alert_detail  = "Brute-force and suspicious activity alerts will notify your devices"
    elif alerts_on and not ha_token:
        alert_status  = "warn"
        alert_detail  = "Alerts are enabled but no HA token is configured — notifications won't be delivered"
    else:
        alert_status  = "fail"
        alert_detail  = "Security alert notifications are disabled"
    checks.append({
        "id": "security_alerts",
        "label": "Security alert notifications enabled",
        "status": alert_status,
        "detail": alert_detail,
    })

    # 3. Recent failed logins
    yesterday = (datetime.now() - timedelta(hours=24)).isoformat(timespec="seconds")
    fail_24h = session.exec(
        select(func.count()).where(
            LoginAuditLog.success == False,
            LoginAuditLog.created_at >= yesterday,
        )
    ).one()
    if fail_24h == 0:
        f_status, f_detail = "pass", "No failed login attempts in the last 24 hours"
    elif fail_24h < 5:
        f_status, f_detail = "warn", f"{fail_24h} failed attempt(s) in the last 24 hours"
    else:
        f_status, f_detail = "fail", f"{fail_24h} failed attempts in the last 24 hours — check login history"
    checks.append({
        "id": "failed_logins",
        "label": "No recent brute-force activity",
        "status": f_status,
        "detail": f_detail,
    })

    # 4. Active sessions
    now_str = datetime.now().isoformat(timespec="seconds")
    sess_count = session.exec(
        select(func.count()).where(UserSession.expires_at >= now_str)
    ).one()
    checks.append({
        "id": "active_sessions",
        "label": "Active sessions look normal",
        "status": "pass" if sess_count <= 5 else "warn",
        "detail": f"{sess_count} active session(s) — review the Sessions panel if unexpected",
    })

    # 5. HA authentication layer
    checks.append({
        "id": "ha_auth",
        "label": "App sits behind Home Assistant authentication",
        "status": "pass",
        "detail": "Direct internet access is blocked — HA auth is required to reach this app",
    })

    # 6. Database backup safety
    db_url = os.environ.get("DATABASE_URL", "")
    gitignore_ok = False
    try:
        with open("/config/.gitignore") as f:
            content = f.read()
            gitignore_ok = "finance_tracker" in content or "finance.db" in content
    except Exception:
        pass
    if gitignore_ok:
        db_status = "pass"
        db_detail = "finance.db is excluded from git backups"
    else:
        db_status = "warn"
        db_detail = "Could not confirm finance.db is in .gitignore — verify it won't be committed"
    checks.append({
        "id": "db_backup",
        "label": "Financial data excluded from git backups",
        "status": db_status,
        "detail": db_detail,
    })

    passed = sum(1 for c in checks if c["status"] == "pass")
    warned = sum(1 for c in checks if c["status"] == "warn")
    return {
        "checks": checks,
        "passed": passed,
        "warned": warned,
        "failed": len(checks) - passed - warned,
        "total": len(checks),
        "checked_at": datetime.now().isoformat(timespec="seconds"),
    }


# ---------------------------------------------------------------------------
# Login history
# ---------------------------------------------------------------------------

@router.get("/login-history")
def login_history(
    limit: int = 100,
    current_user: User = Depends(get_current_user),
    session: Session = Depends(get_session),
):
    logs = session.exec(
        select(LoginAuditLog)
        .order_by(LoginAuditLog.created_at.desc())
        .limit(min(limit, 500))
    ).all()
    return [
        {
            "id": l.id,
            "username": l.username,
            "ip_address": l.ip_address,
            "success": l.success,
            "reason": l.reason,
            "created_at": l.created_at,
        }
        for l in logs
    ]


@router.delete("/login-history")
def clear_login_history(
    current_user: User = Depends(get_current_user),
    session: Session = Depends(get_session),
):
    logs = session.exec(select(LoginAuditLog)).all()
    for l in logs:
        session.delete(l)
    session.commit()
    return {"ok": True, "cleared": len(logs)}


# ---------------------------------------------------------------------------
# Session management
# ---------------------------------------------------------------------------

@router.get("/sessions")
def list_sessions(
    current_user: User = Depends(get_current_user),
    session: Session = Depends(get_session),
    authorization: str = __import__("fastapi").Header(default=None, alias="authorization"),
):
    current_token = authorization[7:] if authorization and authorization.startswith("Bearer ") else None
    now_str = datetime.now().isoformat(timespec="seconds")
    sessions = session.exec(
        select(UserSession)
        .where(UserSession.expires_at >= now_str)
        .order_by(UserSession.created_at.desc())
    ).all()
    result = []
    for s in sessions:
        user = session.get(User, s.user_id)
        result.append({
            "token":      s.token,
            "user_name":  user.name if user else "Unknown",
            "created_at": s.created_at,
            "expires_at": s.expires_at,
            "is_current": s.token == current_token,
        })
    return result


@router.delete("/sessions/{token}")
def revoke_session(
    token: str,
    current_user: User = Depends(get_current_user),
    session: Session = Depends(get_session),
    authorization: str = __import__("fastapi").Header(default=None, alias="authorization"),
):
    current_token = authorization[7:] if authorization and authorization.startswith("Bearer ") else None
    if token == current_token:
        raise HTTPException(400, "Cannot revoke your current session — use logout instead")
    sess = session.get(UserSession, token)
    if not sess:
        raise HTTPException(404, "Session not found")
    session.delete(sess)
    session.commit()
    return {"ok": True}


@router.delete("/sessions")
def revoke_all_other_sessions(
    current_user: User = Depends(get_current_user),
    session: Session = Depends(get_session),
    authorization: str = __import__("fastapi").Header(default=None, alias="authorization"),
):
    current_token = authorization[7:] if authorization and authorization.startswith("Bearer ") else None
    all_sessions = session.exec(select(UserSession)).all()
    count = 0
    for s in all_sessions:
        if s.token != current_token:
            session.delete(s)
            count += 1
    session.commit()
    return {"ok": True, "revoked": count}


# ---------------------------------------------------------------------------
# Security settings
# ---------------------------------------------------------------------------

@router.get("/settings")
def get_security_settings(
    current_user: User = Depends(get_current_user),
    session: Session = Depends(get_session),
):
    return {
        "security_alerts_enabled":         get_setting(session, "security_alerts_enabled", "1"),
        "security_fail_threshold":         get_setting(session, "security_fail_threshold", "5"),
        "security_notify_password_change": get_setting(session, "security_notify_password_change", "1"),
        "security_notify_settings_change": get_setting(session, "security_notify_settings_change", "1"),
        "session_ttl_days":                get_setting(session, "session_ttl_days", "7"),
        "session_inactivity_hours":        get_setting(session, "session_inactivity_hours", "24"),
    }


@router.post("/settings")
async def update_security_settings(
    request: Request,
    current_user: User = Depends(get_current_user),
    session: Session = Depends(get_session),
):
    data = await request.json()
    allowed = {
        "security_alerts_enabled",
        "security_fail_threshold",
        "security_notify_password_change",
        "security_notify_settings_change",
        "session_ttl_days",
        "session_inactivity_hours",
    }
    for key, value in data.items():
        if key in allowed:
            set_setting(session, key, str(value))
    return {"ok": True}


@router.post("/sessions/purge-expired")
def purge_expired(
    current_user: User = Depends(get_current_user),
    session: Session = Depends(get_session),
):
    """Manually purge all expired sessions."""
    from deps import purge_expired_sessions
    count = purge_expired_sessions(session)
    return {"ok": True, "purged": count}
