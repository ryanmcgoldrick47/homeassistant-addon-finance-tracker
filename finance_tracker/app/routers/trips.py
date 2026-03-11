"""Car & Travel logbook — work-related trip tracking for ATO tax purposes."""
from __future__ import annotations

from datetime import date
from typing import Optional

import httpx
from fastapi import APIRouter, Depends, HTTPException, Query
from pydantic import BaseModel
from sqlmodel import Session, select

from database import Trip, get_session, User
from deps import get_current_user, get_setting

router = APIRouter(prefix="/api/trips", tags=["trips"])

ATO_RATE_DEFAULT = 0.88   # FY 2024-25 rate (88c/km)
MAX_KM_CAP = 5000         # cents-per-km method annual cap


def _fy_dates(fy_year: int) -> tuple[date, date]:
    """Return start/end dates for an Australian FY.
    fy_year=2025 means FY 2024-25 → 1 Jul 2024 to 30 Jun 2025."""
    return date(fy_year - 1, 7, 1), date(fy_year, 6, 30)


class TripCreate(BaseModel):
    date: date
    purpose: str = "work"
    description: Optional[str] = None
    start_location: Optional[str] = None
    end_location: Optional[str] = None
    km: float = 0.0
    toll_cents: int = 0
    notes: Optional[str] = None


class TripUpdate(BaseModel):
    date: Optional[date] = None
    purpose: Optional[str] = None
    description: Optional[str] = None
    start_location: Optional[str] = None
    end_location: Optional[str] = None
    km: Optional[float] = None
    toll_cents: Optional[int] = None
    notes: Optional[str] = None


@router.get("")
def list_trips(
    fy: Optional[int] = None,
    purpose: Optional[str] = None,
    limit: int = 500,
    session: Session = Depends(get_session),
    current_user: User = Depends(get_current_user),
):
    """List trips, optionally filtered by FY year (e.g. fy=2025 = FY 2024-25)."""
    q = select(Trip).where(Trip.user_id == current_user.id)
    if fy:
        start, end = _fy_dates(fy)
        q = q.where(Trip.date >= start, Trip.date <= end)
    if purpose:
        q = q.where(Trip.purpose == purpose)
    return session.exec(q.order_by(Trip.date.desc()).limit(limit)).all()


@router.post("")
def create_trip(
    body: TripCreate,
    session: Session = Depends(get_session),
    current_user: User = Depends(get_current_user),
):
    trip = Trip(**body.model_dump(), user_id=current_user.id)
    session.add(trip)
    session.commit()
    session.refresh(trip)
    return trip


@router.patch("/{trip_id}")
def update_trip(
    trip_id: int,
    body: TripUpdate,
    session: Session = Depends(get_session),
    current_user: User = Depends(get_current_user),
):
    trip = session.get(Trip, trip_id)
    if not trip or trip.user_id != current_user.id:
        raise HTTPException(404, "Trip not found")
    for k, v in body.model_dump(exclude_none=True).items():
        setattr(trip, k, v)
    session.add(trip)
    session.commit()
    session.refresh(trip)
    return trip


@router.delete("/{trip_id}")
def delete_trip(
    trip_id: int,
    session: Session = Depends(get_session),
    current_user: User = Depends(get_current_user),
):
    trip = session.get(Trip, trip_id)
    if not trip or trip.user_id != current_user.id:
        raise HTTPException(404, "Trip not found")
    session.delete(trip)
    session.commit()
    return {"ok": True}


@router.get("/distance")
async def calculate_distance(
    origin: str = Query(..., description="Start address"),
    destination: str = Query(..., description="End address"),
    current_user: User = Depends(get_current_user),
):
    """Calculate driving distance (km) between two addresses using Nominatim + OSRM."""
    NOM_URL = "https://nominatim.openstreetmap.org/search"
    OSRM_URL = "https://router.project-osrm.org/route/v1/driving"
    HEADERS = {"User-Agent": "FinanceTracker/1.0 (home assistant app)"}

    async with httpx.AsyncClient(timeout=10) as client:
        async def geocode(addr: str) -> tuple[float, float]:
            r = await client.get(NOM_URL, params={"q": addr, "format": "json", "limit": 1}, headers=HEADERS)
            r.raise_for_status()
            results = r.json()
            if not results:
                raise HTTPException(400, f"Could not geocode address: {addr!r}")
            return float(results[0]["lon"]), float(results[0]["lat"])

        try:
            orig_lon, orig_lat = await geocode(origin)
            dest_lon, dest_lat = await geocode(destination)
        except HTTPException:
            raise
        except Exception as e:
            raise HTTPException(400, f"Geocoding failed: {e}") from e

        try:
            coords = f"{orig_lon},{orig_lat};{dest_lon},{dest_lat}"
            r = await client.get(f"{OSRM_URL}/{coords}", params={"overview": "false"}, headers=HEADERS)
            r.raise_for_status()
            data = r.json()
            if data.get("code") != "Ok" or not data.get("routes"):
                raise HTTPException(400, "OSRM routing failed")
            distance_m = data["routes"][0]["distance"]
            duration_s = data["routes"][0]["duration"]
            km = round(distance_m / 1000, 1)
            return {
                "km": km,
                "duration_minutes": round(duration_s / 60, 1),
                "origin": origin,
                "destination": destination,
            }
        except HTTPException:
            raise
        except Exception as e:
            raise HTTPException(400, f"Routing failed: {e}") from e


@router.get("/summary")
def trip_summary(
    fy: Optional[int] = None,
    session: Session = Depends(get_session),
    current_user: User = Depends(get_current_user),
):
    """Return FY summary: km by purpose, deduction estimates, tolls."""
    if not fy:
        today = date.today()
        fy = today.year + 1 if today.month >= 7 else today.year
    start, end = _fy_dates(fy)
    trips = session.exec(
        select(Trip).where(
            Trip.user_id == current_user.id,
            Trip.date >= start,
            Trip.date <= end,
        )
    ).all()

    ato_rate = float(get_setting(session, "ato_km_rate") or ATO_RATE_DEFAULT)

    km_by_purpose: dict[str, float] = {}
    toll_by_purpose: dict[str, int] = {}
    for t in trips:
        km_by_purpose[t.purpose] = km_by_purpose.get(t.purpose, 0) + t.km
        toll_by_purpose[t.purpose] = toll_by_purpose.get(t.purpose, 0) + t.toll_cents

    work_km = km_by_purpose.get("work", 0)
    work_km_capped = min(work_km, MAX_KM_CAP)
    deduction = round(work_km_capped * ato_rate, 2)
    work_toll = toll_by_purpose.get("work", 0) / 100

    return {
        "fy": f"{fy-1}–{str(fy)[2:]}",
        "fy_year": fy,
        "total_trips": len(trips),
        "km_by_purpose": km_by_purpose,
        "toll_by_purpose": {k: v / 100 for k, v in toll_by_purpose.items()},
        "work_km": work_km,
        "work_km_capped": work_km_capped,
        "capped_at_5000": work_km > MAX_KM_CAP,
        "ato_km_rate": ato_rate,
        "cents_per_km_deduction": deduction,
        "work_toll_total": work_toll,
        "total_work_deduction": round(deduction + work_toll, 2),
    }
