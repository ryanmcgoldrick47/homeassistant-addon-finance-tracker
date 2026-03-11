from __future__ import annotations

from typing import Optional
from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel
from sqlmodel import Session, select

from database import Category, get_session

router = APIRouter(prefix="/api/categories", tags=["categories"])


class CategoryCreate(BaseModel):
    name: str
    colour: str = "#6366f1"
    icon: str = "mdi:tag"
    is_income: bool = False
    is_tax_relevant: bool = False
    exclude_from_spend: bool = False
    parent_id: Optional[int] = None


class CategoryUpdate(BaseModel):
    name: Optional[str] = None
    colour: Optional[str] = None
    icon: Optional[str] = None
    is_income: Optional[bool] = None
    is_tax_relevant: Optional[bool] = None
    exclude_from_spend: Optional[bool] = None


@router.get("")
def list_categories(session: Session = Depends(get_session)):
    cats = session.exec(select(Category).order_by(Category.is_income.desc(), Category.name)).all()
    return cats


@router.post("")
def create_category(body: CategoryCreate, session: Session = Depends(get_session)):
    cat = Category(**body.model_dump())
    session.add(cat)
    session.commit()
    session.refresh(cat)
    return cat


@router.patch("/{cat_id}")
def update_category(cat_id: int, body: CategoryUpdate, session: Session = Depends(get_session)):
    cat = session.get(Category, cat_id)
    if not cat:
        raise HTTPException(status_code=404, detail="Category not found")
    for k, v in body.model_dump(exclude_none=True).items():
        setattr(cat, k, v)
    session.add(cat)
    session.commit()
    session.refresh(cat)
    return cat


@router.delete("/{cat_id}")
def delete_category(cat_id: int, session: Session = Depends(get_session)):
    cat = session.get(Category, cat_id)
    if not cat:
        raise HTTPException(status_code=404, detail="Not found")
    session.delete(cat)
    session.commit()
    return {"ok": True}
