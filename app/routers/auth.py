"""Authentication — register, login, logout, session management."""
from __future__ import annotations

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel
from sqlmodel import Session, select, func

from database import User, UserSession, get_session
from deps import create_session, get_current_user, hash_password, verify_password

router = APIRouter(prefix="/api/auth", tags=["auth"])


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
    if len(body.password) < 4:
        raise HTTPException(400, "Password must be at least 4 characters")

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
def login(body: AuthRequest, session: Session = Depends(get_session)):
    """Log in with name and password."""
    user = session.exec(
        select(User).where(func.lower(User.name) == body.name.strip().lower())
    ).first()
    if not user:
        raise HTTPException(401, "Incorrect name or password")

    # Allow login without password if no password has been set yet (first-time setup)
    if user.password_hash and not verify_password(body.password, user.password_hash):
        raise HTTPException(401, "Incorrect name or password")

    token = create_session(user.id, session)
    return {"token": token, "user": _user_dict(user)}


@router.get("/me")
def get_me(current_user: User = Depends(get_current_user)):
    return _user_dict(current_user)


@router.patch("/me")
def update_me(
    body: UpdateMeRequest,
    current_user: User = Depends(get_current_user),
    session: Session = Depends(get_session),
):
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
        if len(body.password) < 4:
            raise HTTPException(400, "Password must be at least 4 characters")
        current_user.password_hash = hash_password(body.password)
    if body.color_hex is not None:
        current_user.color_hex = body.color_hex
    session.add(current_user)
    session.commit()
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
