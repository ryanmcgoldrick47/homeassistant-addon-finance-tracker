from __future__ import annotations

import io
import json
import os
from datetime import date
from typing import Optional

from fastapi import APIRouter, Depends, HTTPException, UploadFile, File
from fastapi.responses import FileResponse
from sqlmodel import Session, select, func

from database import Payslip, Transaction, Setting, get_session, User, engine
from deps import get_setting, get_current_user

router = APIRouter(prefix="/api/payslips", tags=["payslips"])

_DATA_DIR = os.environ.get("FINANCE_DATA_DIR", "/data")
PAYSLIPS_DIR = os.path.join(_DATA_DIR, "payslips")

# In-memory short-lived view tokens: {uuid: (payslip_id, user_id, expires_datetime)}
import uuid as _uuid_mod
from datetime import datetime as _dt
_pdf_view_tokens: dict[str, tuple[int, int, object]] = {}
PAYSLIP_WATCH_DIR = os.path.join(_DATA_DIR, "payslip_watch")

# In-memory log of recent watch-folder import results (last 20)
_payslip_watch_log: list[dict] = []


def _save_pdf(payslip_id: int, file_bytes: bytes) -> None:
    os.makedirs(PAYSLIPS_DIR, exist_ok=True)
    path = os.path.join(PAYSLIPS_DIR, f"payslip_{payslip_id}.pdf")
    with open(path, "wb") as fh:
        fh.write(file_bytes)


def _pdf_path(payslip_id: int) -> str:
    return os.path.join(PAYSLIPS_DIR, f"payslip_{payslip_id}.pdf")

# Australian super guarantee rate by FY (key = FY ending year)
SUPER_RATES = {2024: 11.0, 2025: 11.5, 2026: 11.5, 2027: 12.0}


# ---------------------------------------------------------------------------
# PDF text extraction
# ---------------------------------------------------------------------------

def _extract_pdf_text(file_bytes: bytes) -> str:
    try:
        import pypdf
        reader = pypdf.PdfReader(io.BytesIO(file_bytes))
        pages = [page.extract_text() or "" for page in reader.pages]
        return "\n".join(pages)
    except Exception as e:
        raise HTTPException(400, f"Could not read PDF: {e}")


# ---------------------------------------------------------------------------
# AI extraction
# ---------------------------------------------------------------------------

def _build_extraction_prompt(text: str) -> str:
    return f"""You are extracting structured data from an Australian payslip.

Extract all available fields and return ONLY a valid JSON object with this structure (use null for missing fields):
{{
  "employer": "Company name",
  "employee_name": "Employee name",
  "pay_date": "YYYY-MM-DD",
  "period_start": "YYYY-MM-DD",
  "period_end": "YYYY-MM-DD",
  "pay_frequency": "fortnightly|weekly|monthly|quarterly",
  "gross_pay": 3500.00,
  "net_pay": 2734.56,
  "tax_withheld": 612.00,
  "super_amount": 402.50,
  "hours_worked": 76.0,
  "annual_leave_hours": 87.5,
  "sick_leave_hours": 36.25,
  "long_service_leave_hours": null,
  "ytd_gross": 21000.00,
  "ytd_tax": 3672.00,
  "ytd_super": 2415.00,
  "allowances": [{{"name": "Car allowance", "amount": 150.00}}],
  "deductions": [{{"name": "Union fees", "amount": 12.50}}]
}}

All dollar amounts as floats (not strings). Dates as YYYY-MM-DD strings.

Payslip text:
{text[:6000]}

Return ONLY the JSON object, no markdown, no explanation."""


async def _call_ai_extraction(text: str, session: Session) -> dict:
    provider = get_setting(session, "ai_provider") or "gemini"
    prompt = _build_extraction_prompt(text)

    try:
        if provider == "gemini":
            api_key = get_setting(session, "gemini_api_key") or os.environ.get("GEMINI_API_KEY", "")
            if not api_key:
                raise HTTPException(400, "Gemini API key not configured.")
            from google import genai
            from google.genai import types
            client = genai.Client(api_key=api_key)
            response = client.models.generate_content(
                model="gemini-2.5-flash",
                contents=prompt,
                config=types.GenerateContentConfig(response_mime_type="application/json"),
            )
            raw = response.text
        else:
            api_key = get_setting(session, "anthropic_api_key") or os.environ.get("ANTHROPIC_API_KEY", "")
            if not api_key:
                raise HTTPException(400, "Anthropic API key not configured.")
            import anthropic
            client = anthropic.Anthropic(api_key=api_key)
            msg = client.messages.create(
                model="claude-sonnet-4-6",
                max_tokens=2000,
                messages=[{"role": "user", "content": prompt}],
            )
            raw = msg.content[0].text
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(500, f"AI error during extraction: {e}")

    raw = raw.strip()
    if raw.startswith("```"):
        raw = raw.split("```")[1]
        if raw.startswith("json"):
            raw = raw[4:]
    try:
        return json.loads(raw.strip())
    except Exception:
        raise HTTPException(500, f"AI returned invalid JSON: {raw[:300]}")


# ---------------------------------------------------------------------------
# Variation checking
# ---------------------------------------------------------------------------

def _check_variations(data: dict, prev: Optional[Payslip]) -> list[str]:
    flags = []

    gross = data.get("gross_pay") or 0
    net = data.get("net_pay") or 0
    tax = data.get("tax_withheld") or 0
    super_amt = data.get("super_amount") or 0

    # Super check — should be ~11.5% of gross (FY2026)
    if gross > 0 and super_amt > 0:
        super_rate = super_amt / gross * 100
        expected = 11.5
        if abs(super_rate - expected) > 1.5:
            flags.append(
                f"Super appears to be {super_rate:.1f}% of gross (expected ~{expected}%). "
                f"Check if salary sacrifice or other arrangement applies."
            )

    # Tax sanity — should generally be 15–40% of gross
    if gross > 0 and tax > 0:
        tax_rate = tax / gross * 100
        if tax_rate < 5:
            flags.append(f"Tax withheld is very low ({tax_rate:.1f}% of gross). Verify tax file number is on file.")
        elif tax_rate > 50:
            flags.append(f"Tax withheld is unusually high ({tax_rate:.1f}% of gross). May indicate a HELP/HECS catch-up or error.")

    if prev:
        prev_gross = prev.gross_pay_cents / 100
        prev_net = prev.net_pay_cents / 100
        prev_tax = prev.tax_withheld_cents / 100
        prev_super = prev.super_cents / 100
        prev_al = prev.annual_leave_hours
        prev_sl = prev.sick_leave_hours

        # Gross pay change
        if prev_gross > 0:
            gross_chg = (gross - prev_gross) / prev_gross * 100
            if gross_chg > 10:
                flags.append(f"Gross pay increased {gross_chg:.1f}% vs previous payslip (${prev_gross:,.2f} → ${gross:,.2f}). Pay rise or additional hours?")
            elif gross_chg < -5:
                flags.append(f"Gross pay decreased {abs(gross_chg):.1f}% vs previous payslip (${prev_gross:,.2f} → ${gross:,.2f}). Fewer hours, unpaid leave, or error?")

        # Net pay change where gross unchanged
        if abs(gross - prev_gross) < 10 and prev_net > 0:
            net_chg = (net - prev_net) / prev_net * 100
            if abs(net_chg) > 8:
                flags.append(f"Net pay changed {net_chg:+.1f}% while gross pay stayed similar. Check deductions or tax adjustments.")

        # Leave balance changes
        al_hours = data.get("annual_leave_hours")
        sl_hours = data.get("sick_leave_hours")
        if al_hours is not None and prev_al is not None:
            al_chg = al_hours - prev_al
            # If balance went down more than expected accrual - expected accrual is ~1.538hr/week for FY
            if al_chg < -40:
                flags.append(f"Annual leave balance dropped significantly ({al_chg:+.1f} hrs). Large leave period taken.")
            elif al_hours > 200:
                flags.append(f"Annual leave balance is {al_hours:.1f} hours — consider taking some leave before it affects entitlements.")

        if sl_hours is not None and prev_sl is not None:
            sl_chg = sl_hours - prev_sl
            if sl_chg < -16:
                flags.append(f"Sick/personal leave balance dropped {abs(sl_chg):.1f} hours since last payslip.")

        # New deductions
        prev_deds = json.loads(prev.deductions_json or "[]")
        curr_deds = data.get("deductions") or []
        prev_ded_names = {d.get("name", "").lower() for d in prev_deds}
        for ded in curr_deds:
            if ded.get("name", "").lower() not in prev_ded_names:
                flags.append(f"New deduction appeared: '{ded['name']}' (${ded.get('amount', 0):,.2f}). Verify this is expected.")

    return flags


# ---------------------------------------------------------------------------
# Endpoints
# ---------------------------------------------------------------------------

@router.get("")
def list_payslips(
    session: Session = Depends(get_session),
    current_user: User = Depends(get_current_user),
):
    rows = session.exec(
        select(Payslip).where(Payslip.user_id == current_user.id).order_by(Payslip.pay_date.desc())
    ).all()
    now = _dt.now()
    from datetime import timedelta
    result = []
    for p in rows:
        d = {
            **_payslip_dict(p),
            "flags": json.loads(p.flags_json or "[]"),
            "allowances": json.loads(p.allowances_json or "[]"),
            "deductions": json.loads(p.deductions_json or "[]"),
            "pdf_view_path": None,
        }
        # Pre-generate a 10-min view token so the frontend can use a plain <a href>
        if d["has_pdf"]:
            view_id = _uuid_mod.uuid4().hex
            _pdf_view_tokens[view_id] = (p.id, current_user.id, now + timedelta(minutes=10))
            d["pdf_view_path"] = f"/api/payslips/open/{view_id}"
        result.append(d)
    return result


@router.get("/summary")
def payslip_summary(
    session: Session = Depends(get_session),
    current_user: User = Depends(get_current_user),
):
    """Latest leave balances, YTD totals, pay stats — combined across all employers."""
    all_payslips = session.exec(
        select(Payslip).where(Payslip.user_id == current_user.id).order_by(Payslip.pay_date.desc())
    ).all()
    if not all_payslips:
        return {"has_data": False}

    # Get the most recent payslip per employer
    seen_employers: set = set()
    latest_per_employer: list = []
    for p in all_payslips:
        key = (p.employer or "").strip().lower()
        if key not in seen_employers:
            seen_employers.add(key)
            latest_per_employer.append(p)

    # Sum YTD figures across all employers
    ytd_gross = sum((p.ytd_gross_cents or 0) for p in latest_per_employer) / 100
    ytd_tax   = sum((p.ytd_tax_cents or 0) for p in latest_per_employer) / 100
    ytd_super = sum((p.ytd_super_cents or 0) for p in latest_per_employer) / 100

    # Sum leave balances across employers (each employer tracks leave independently)
    annual_leave = sum((p.annual_leave_hours or 0) for p in latest_per_employer)
    sick_leave   = sum((p.sick_leave_hours or 0) for p in latest_per_employer)
    long_service = sum((p.long_service_hours or 0) for p in latest_per_employer)

    latest = latest_per_employer[0]  # most recent overall for display fields

    multi_employer = len(latest_per_employer) > 1

    unreviewed_flags = sum(
        1 for p in all_payslips
        if not p.is_reviewed and json.loads(p.flags_json or "[]")
    )

    return {
        "has_data": True,
        "latest_pay_date": str(latest.pay_date),
        "employer": latest.employer if not multi_employer else f"{len(latest_per_employer)} employers",
        "multi_employer": multi_employer,
        "employers": [p.employer for p in latest_per_employer],
        "annual_leave_hours": annual_leave,
        "sick_leave_hours": sick_leave,
        "long_service_hours": long_service,
        "ytd_gross": ytd_gross,
        "ytd_tax": ytd_tax,
        "ytd_super": ytd_super,
        "ytd_net": ytd_gross - ytd_tax,
        "latest_gross": latest.gross_pay_cents / 100,
        "latest_net": latest.net_pay_cents / 100,
        "latest_super": latest.super_cents / 100,
        "pay_frequency": latest.pay_frequency,
        "unreviewed_flags": unreviewed_flags,
    }


@router.post("/upload")
async def upload_payslip(
    file: UploadFile = File(...),
    session: Session = Depends(get_session),
    current_user: User = Depends(get_current_user),
):
    if not file.filename.lower().endswith(".pdf"):
        raise HTTPException(400, "Only PDF files are supported.")

    file_bytes = await file.read()
    text = _extract_pdf_text(file_bytes)
    if len(text.strip()) < 50:
        raise HTTPException(400, "Could not extract readable text from this PDF. It may be a scanned/image PDF.")

    data = await _call_ai_extraction(text, session)

    # Parse pay_date
    pay_date_str = data.get("pay_date")
    if not pay_date_str:
        raise HTTPException(400, "Could not determine pay date from payslip.")
    try:
        pay_date = date.fromisoformat(pay_date_str)
    except ValueError:
        raise HTTPException(400, f"Invalid pay date format: {pay_date_str}")

    # Check for duplicate (same pay_date + employer + user)
    employer = data.get("employer") or ""
    existing = session.exec(
        select(Payslip).where(
            Payslip.user_id == current_user.id,
            Payslip.pay_date == pay_date,
            Payslip.employer == employer,
        )
    ).first()
    if existing:
        raise HTTPException(409, f"A payslip for {pay_date} from {employer} already exists (ID {existing.id}).")

    # Get previous payslip for same employer for comparison
    prev = session.exec(
        select(Payslip).where(
            Payslip.user_id == current_user.id,
            Payslip.employer == employer,
            Payslip.pay_date < pay_date,
        ).order_by(Payslip.pay_date.desc()).limit(1)
    ).first()

    flags = _check_variations(data, prev)

    def _cents(val) -> int:
        return round((val or 0) * 100)

    payslip = Payslip(
        pay_date=pay_date,
        period_start=date.fromisoformat(data["period_start"]) if data.get("period_start") else None,
        period_end=date.fromisoformat(data["period_end"]) if data.get("period_end") else None,
        employer=employer,
        pay_frequency=data.get("pay_frequency"),
        gross_pay_cents=_cents(data.get("gross_pay")),
        net_pay_cents=_cents(data.get("net_pay")),
        tax_withheld_cents=_cents(data.get("tax_withheld")),
        super_cents=_cents(data.get("super_amount")),
        annual_leave_hours=data.get("annual_leave_hours"),
        sick_leave_hours=data.get("sick_leave_hours"),
        long_service_hours=data.get("long_service_leave_hours"),
        ytd_gross_cents=_cents(data.get("ytd_gross")) if data.get("ytd_gross") else None,
        ytd_tax_cents=_cents(data.get("ytd_tax")) if data.get("ytd_tax") else None,
        ytd_super_cents=_cents(data.get("ytd_super")) if data.get("ytd_super") else None,
        hours_worked=data.get("hours_worked"),
        allowances_json=json.dumps(data.get("allowances") or []),
        deductions_json=json.dumps(data.get("deductions") or []),
        flags_json=json.dumps(flags),
        raw_extracted=json.dumps(data),
        source="upload",
        filename=file.filename,
        is_reviewed=False,
        user_id=current_user.id,
    )
    session.add(payslip)
    session.commit()
    session.refresh(payslip)

    _save_pdf(payslip.id, file_bytes)

    return {
        **_payslip_dict(payslip),
        "flags": flags,
        "allowances": data.get("allowances") or [],
        "deductions": data.get("deductions") or [],
    }


@router.post("/bulk")
async def bulk_upload_payslips(
    files: list[UploadFile] = File(...),
    session: Session = Depends(get_session),
    current_user: User = Depends(get_current_user),
):
    """Upload multiple PDF payslips at once. Returns a result per file."""
    results = []
    for file in files:
        result = {"filename": file.filename, "status": None, "detail": None}
        try:
            if not file.filename.lower().endswith(".pdf"):
                result["status"] = "error"
                result["detail"] = "Not a PDF file"
                results.append(result)
                continue

            file_bytes = await file.read()
            try:
                text = _extract_pdf_text(file_bytes)
            except HTTPException as e:
                result["status"] = "error"
                result["detail"] = e.detail
                results.append(result)
                continue

            if len(text.strip()) < 50:
                result["status"] = "error"
                result["detail"] = "Could not extract text — may be a scanned PDF"
                results.append(result)
                continue

            data = await _call_ai_extraction(text, session)

            pay_date_str = data.get("pay_date")
            if not pay_date_str:
                result["status"] = "error"
                result["detail"] = "Could not determine pay date"
                results.append(result)
                continue

            try:
                pay_date = date.fromisoformat(pay_date_str)
            except ValueError:
                result["status"] = "error"
                result["detail"] = f"Invalid pay date: {pay_date_str}"
                results.append(result)
                continue

            employer = data.get("employer") or ""
            existing = session.exec(
                select(Payslip).where(
                    Payslip.user_id == current_user.id,
                    Payslip.pay_date == pay_date,
                    Payslip.employer == employer,
                )
            ).first()
            if existing:
                result["status"] = "duplicate"
                result["detail"] = f"Already exists (ID {existing.id}) for {pay_date}"
                result["pay_date"] = str(pay_date)
                results.append(result)
                continue

            prev = session.exec(
                select(Payslip).where(
                    Payslip.user_id == current_user.id,
                    Payslip.employer == employer,
                    Payslip.pay_date < pay_date,
                ).order_by(Payslip.pay_date.desc()).limit(1)
            ).first()
            flags = _check_variations(data, prev)

            def _cents(val) -> int:
                return round((val or 0) * 100)

            payslip = Payslip(
                pay_date=pay_date,
                period_start=date.fromisoformat(data["period_start"]) if data.get("period_start") else None,
                period_end=date.fromisoformat(data["period_end"]) if data.get("period_end") else None,
                employer=employer,
                pay_frequency=data.get("pay_frequency"),
                gross_pay_cents=_cents(data.get("gross_pay")),
                net_pay_cents=_cents(data.get("net_pay")),
                tax_withheld_cents=_cents(data.get("tax_withheld")),
                super_cents=_cents(data.get("super_amount")),
                annual_leave_hours=data.get("annual_leave_hours"),
                sick_leave_hours=data.get("sick_leave_hours"),
                long_service_hours=data.get("long_service_leave_hours"),
                ytd_gross_cents=_cents(data.get("ytd_gross")) if data.get("ytd_gross") else None,
                ytd_tax_cents=_cents(data.get("ytd_tax")) if data.get("ytd_tax") else None,
                ytd_super_cents=_cents(data.get("ytd_super")) if data.get("ytd_super") else None,
                hours_worked=data.get("hours_worked"),
                allowances_json=json.dumps(data.get("allowances") or []),
                deductions_json=json.dumps(data.get("deductions") or []),
                flags_json=json.dumps(flags),
                raw_extracted=json.dumps(data),
                source="upload",
                filename=file.filename,
                is_reviewed=False,
                user_id=current_user.id,
            )
            session.add(payslip)
            session.commit()
            session.refresh(payslip)

            _save_pdf(payslip.id, file_bytes)

            result["status"] = "ok"
            result["pay_date"] = str(pay_date)
            result["gross_pay"] = payslip.gross_pay_cents / 100
            result["flag_count"] = len(flags)
            result["id"] = payslip.id
        except Exception as e:
            result["status"] = "error"
            result["detail"] = str(e)[:200]
        results.append(result)

    return results


@router.get("/dedup")
def find_duplicates(
    session: Session = Depends(get_session),
    current_user: User = Depends(get_current_user),
):
    """Find duplicate payslips grouped by pay date."""
    from collections import defaultdict

    all_payslips = session.exec(
        select(Payslip).where(Payslip.user_id == current_user.id).order_by(Payslip.pay_date.asc(), Payslip.id.asc())
    ).all()

    by_date: dict[str, list] = defaultdict(list)
    for p in all_payslips:
        by_date[str(p.pay_date)].append(p)

    def _score(p: Payslip) -> int:
        """Count populated fields — higher = more complete."""
        return sum(1 for f in (
            "ytd_gross_cents", "ytd_tax_cents", "ytd_super_cents",
            "annual_leave_hours", "sick_leave_hours", "period_start",
            "period_end", "pay_frequency", "hours_worked",
        ) if getattr(p, f) is not None)

    groups = []
    for pay_date, records in by_date.items():
        if len(records) <= 1:
            continue

        used = set()
        clusters = []
        for i, a in enumerate(records):
            if i in used:
                continue
            cluster = [a]
            used.add(i)
            for j, b in enumerate(records):
                if j in used:
                    continue
                gross_match = abs(a.gross_pay_cents - b.gross_pay_cents) <= 500
                emp_a = (a.employer or "").lower().strip()
                emp_b = (b.employer or "").lower().strip()
                emp_match = emp_a and emp_b and emp_a == emp_b
                if gross_match or emp_match:
                    cluster.append(b)
                    used.add(j)
            if len(cluster) > 1:
                clusters.append(cluster)

        for cluster in clusters:
            ranked = sorted(cluster, key=lambda p: (_score(p), p.id), reverse=True)
            keep = ranked[0]
            remove = ranked[1:]
            groups.append({
                "pay_date": pay_date,
                "keep": _payslip_dict(keep),
                "remove": [_payslip_dict(r) for r in remove],
            })

    return {
        "groups": groups,
        "total_duplicates": sum(len(g["remove"]) for g in groups),
    }


@router.post("/dedup")
def remove_duplicates(
    session: Session = Depends(get_session),
    current_user: User = Depends(get_current_user),
):
    """Delete the less-complete duplicates identified by /dedup."""
    data = find_duplicates(session=session, current_user=current_user)
    removed_ids = []
    for group in data["groups"]:
        for r in group["remove"]:
            p = session.get(Payslip, r["id"])
            if p:
                session.delete(p)
                removed_ids.append(r["id"])
    session.commit()
    return {"removed": len(removed_ids), "ids": removed_ids}


@router.patch("/{payslip_id}/review")
def mark_reviewed(
    payslip_id: int,
    session: Session = Depends(get_session),
    current_user: User = Depends(get_current_user),
):
    p = session.get(Payslip, payslip_id)
    if not p:
        raise HTTPException(404, "Not found")
    if p.user_id != current_user.id:
        raise HTTPException(403, "Access denied")
    p.is_reviewed = True
    session.add(p)
    session.commit()
    return {"ok": True}


@router.delete("/{payslip_id}")
def delete_payslip(
    payslip_id: int,
    session: Session = Depends(get_session),
    current_user: User = Depends(get_current_user),
):
    p = session.get(Payslip, payslip_id)
    if not p:
        raise HTTPException(404, "Not found")
    if p.user_id != current_user.id:
        raise HTTPException(403, "Access denied")
    # Remove PDF from disk if present
    pdf = _pdf_path(payslip_id)
    if os.path.exists(pdf):
        os.remove(pdf)
    session.delete(p)
    session.commit()
    return {"ok": True}


@router.get("/{payslip_id}/pdf")
def serve_payslip_pdf(
    payslip_id: int,
    session: Session = Depends(get_session),
    current_user: User = Depends(get_current_user),
):
    p = session.get(Payslip, payslip_id)
    if not p:
        raise HTTPException(404, "Payslip not found")
    if p.user_id != current_user.id:
        raise HTTPException(403, "Access denied")
    path = _pdf_path(payslip_id)
    if not os.path.exists(path):
        raise HTTPException(404, "PDF not available — this payslip was uploaded before file storage was added. Re-upload the PDF to attach it.")
    display_name = p.filename or f"payslip_{p.pay_date}.pdf"
    return FileResponse(path, media_type="application/pdf", headers={
        "Content-Disposition": f'inline; filename="{display_name}"',
    })


@router.post("/{payslip_id}/pdf-view")
def create_pdf_view_link(
    payslip_id: int,
    session: Session = Depends(get_session),
    current_user: User = Depends(get_current_user),
):
    """Generate a short-lived (5 min) one-time URL to view the PDF without query params."""
    p = session.get(Payslip, payslip_id)
    if not p or p.user_id != current_user.id:
        raise HTTPException(404, "Not found")
    if not os.path.exists(_pdf_path(payslip_id)):
        raise HTTPException(404, "PDF file not found")
    # Clean up expired tokens
    now = _dt.now()
    expired = [k for k, (_, _, exp) in _pdf_view_tokens.items() if exp < now]
    for k in expired:
        _pdf_view_tokens.pop(k, None)
    # Issue new token
    view_id = _uuid_mod.uuid4().hex
    from datetime import timedelta
    _pdf_view_tokens[view_id] = (payslip_id, current_user.id, now + timedelta(minutes=5))
    return {"view_path": f"/api/payslips/open/{view_id}"}


@router.get("/open/{view_id}")
def open_pdf_by_view_token(view_id: str):
    """Serve PDF using a short-lived path-based view token (no auth header needed)."""
    entry = _pdf_view_tokens.pop(view_id, None)
    if not entry:
        raise HTTPException(404, "Link expired or not found — click View PDF again")
    payslip_id, _user_id, expires = entry
    if _dt.now() > expires:
        raise HTTPException(404, "Link expired — click View PDF again")
    path = _pdf_path(payslip_id)
    if not os.path.exists(path):
        raise HTTPException(404, "PDF file not available")
    return FileResponse(path, media_type="application/pdf", headers={
        "Content-Disposition": f'inline; filename="payslip_{payslip_id}.pdf"',
    })


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _payslip_dict(p: Payslip) -> dict:
    return {
        "id": p.id,
        "pay_date": str(p.pay_date),
        "period_start": str(p.period_start) if p.period_start else None,
        "period_end": str(p.period_end) if p.period_end else None,
        "employer": p.employer,
        "pay_frequency": p.pay_frequency,
        "gross_pay": p.gross_pay_cents / 100,
        "net_pay": p.net_pay_cents / 100,
        "tax_withheld": p.tax_withheld_cents / 100,
        "super_amount": p.super_cents / 100,
        "annual_leave_hours": p.annual_leave_hours,
        "sick_leave_hours": p.sick_leave_hours,
        "long_service_hours": p.long_service_hours,
        "ytd_gross": (p.ytd_gross_cents or 0) / 100,
        "ytd_tax": (p.ytd_tax_cents or 0) / 100,
        "ytd_super": (p.ytd_super_cents or 0) / 100,
        "hours_worked": p.hours_worked,
        "source": p.source,
        "filename": p.filename,
        "is_reviewed": p.is_reviewed,
        "matched_txn_id": p.matched_txn_id,
        "flag_count": len(json.loads(p.flags_json or "[]")),
        "has_pdf": os.path.exists(_pdf_path(p.id)),
    }


# ---------------------------------------------------------------------------
# Payslip-transaction matching
# ---------------------------------------------------------------------------

@router.get("/suggest-matches")
def suggest_matches(
    session: Session = Depends(get_session),
    current_user: User = Depends(get_current_user),
):
    """For each unmatched payslip, find the most likely matching credit transaction."""
    from datetime import timedelta
    payslips = session.exec(
        select(Payslip).where(
            Payslip.user_id == current_user.id,
            Payslip.matched_txn_id == None,
        ).order_by(Payslip.pay_date.desc())
    ).all()

    results = []
    for p in payslips:
        net = p.net_pay_cents / 100
        window_start = p.pay_date - timedelta(days=5)
        window_end = p.pay_date + timedelta(days=5)
        tolerance = net * 0.05

        candidates = session.exec(
            select(Transaction).where(
                Transaction.user_id == current_user.id,
                Transaction.is_credit == True,
                Transaction.date >= window_start,
                Transaction.date <= window_end,
                Transaction.amount >= net - tolerance,
                Transaction.amount <= net + tolerance,
            ).order_by(
                func.abs(Transaction.amount - net)
            ).limit(1)
        ).all()

        suggestion = None
        if candidates:
            t = candidates[0]
            suggestion = {
                "txn_id": t.id,
                "date": str(t.date),
                "amount": float(t.amount),
                "description": t.description,
            }

        results.append({
            "payslip_id": p.id,
            "pay_date": str(p.pay_date),
            "employer": p.employer,
            "net_pay": net,
            "suggestion": suggestion,
        })

    return results


@router.patch("/{payslip_id}/match")
def set_match(
    payslip_id: int,
    data: dict,
    session: Session = Depends(get_session),
    current_user: User = Depends(get_current_user),
):
    """Set or clear the matched transaction for a payslip. Body: {txn_id: int | null}"""
    p = session.get(Payslip, payslip_id)
    if not p:
        raise HTTPException(404, "Payslip not found")
    if p.user_id != current_user.id:
        raise HTTPException(403, "Access denied")
    p.matched_txn_id = data.get("txn_id")  # None clears the match
    session.add(p)
    session.commit()
    return {"ok": True, "matched_txn_id": p.matched_txn_id}


# ---------------------------------------------------------------------------
# Payslip watch-folder status
# ---------------------------------------------------------------------------

@router.get("/watch-status")
def payslip_watch_status(current_user: User = Depends(get_current_user)):
    _ensure_watch_dirs()
    try:
        pending = [f for f in os.listdir(PAYSLIP_WATCH_DIR)
                   if f.lower().endswith(".pdf") and os.path.isfile(os.path.join(PAYSLIP_WATCH_DIR, f))]
    except Exception:
        pending = []
    return {
        "watch_dir": PAYSLIP_WATCH_DIR,
        "pending": pending,
        "recent_log": _payslip_watch_log[:10],
    }


def _ensure_watch_dirs():
    os.makedirs(PAYSLIP_WATCH_DIR, exist_ok=True)
    os.makedirs(os.path.join(PAYSLIP_WATCH_DIR, "processed"), exist_ok=True)
    os.makedirs(os.path.join(PAYSLIP_WATCH_DIR, "failed"), exist_ok=True)


async def payslip_watch_tick():
    """Called every 60s by background task. Imports any PDFs found in the payslip watch folder."""
    from datetime import datetime
    _ensure_watch_dirs()
    try:
        files = [f for f in os.listdir(PAYSLIP_WATCH_DIR)
                 if f.lower().endswith(".pdf") and os.path.isfile(os.path.join(PAYSLIP_WATCH_DIR, f))]
    except Exception:
        return

    if not files:
        return

    with Session(engine) as session:
        enabled = get_setting(session, "payslip_watch_enabled", "1")
        if enabled != "1":
            return

    import shutil
    for filename in files:
        src = os.path.join(PAYSLIP_WATCH_DIR, filename)
        entry = {"file": filename, "processed_at": datetime.now().strftime("%Y-%m-%d %H:%M")}
        try:
            with open(src, "rb") as fh:
                file_bytes = fh.read()

            text = _extract_pdf_text(file_bytes)
            if len(text.strip()) < 50:
                raise ValueError("Could not extract readable text — may be scanned PDF")

            with Session(engine) as session:
                data = await _call_ai_extraction(text, session)

                pay_date_str = data.get("pay_date")
                if not pay_date_str:
                    raise ValueError("Could not determine pay date")
                pay_date = date.fromisoformat(pay_date_str)
                employer = data.get("employer") or ""

                existing = session.exec(
                    select(Payslip).where(
                        Payslip.user_id == 1,
                        Payslip.pay_date == pay_date,
                        Payslip.employer == employer,
                    )
                ).first()
                if existing:
                    entry.update({"status": "duplicate", "detail": f"Already exists (ID {existing.id})"})
                else:
                    prev = session.exec(
                        select(Payslip).where(
                            Payslip.user_id == 1,
                            Payslip.employer == employer,
                            Payslip.pay_date < pay_date,
                        ).order_by(Payslip.pay_date.desc()).limit(1)
                    ).first()
                    flags = _check_variations(data, prev)

                    def _cents(val) -> int:
                        return round((val or 0) * 100)

                    payslip = Payslip(
                        pay_date=pay_date,
                        period_start=date.fromisoformat(data["period_start"]) if data.get("period_start") else None,
                        period_end=date.fromisoformat(data["period_end"]) if data.get("period_end") else None,
                        employer=employer,
                        pay_frequency=data.get("pay_frequency"),
                        gross_pay_cents=_cents(data.get("gross_pay")),
                        net_pay_cents=_cents(data.get("net_pay")),
                        tax_withheld_cents=_cents(data.get("tax_withheld")),
                        super_cents=_cents(data.get("super_amount")),
                        annual_leave_hours=data.get("annual_leave_hours"),
                        sick_leave_hours=data.get("sick_leave_hours"),
                        long_service_hours=data.get("long_service_leave_hours"),
                        ytd_gross_cents=_cents(data.get("ytd_gross")) if data.get("ytd_gross") else None,
                        ytd_tax_cents=_cents(data.get("ytd_tax")) if data.get("ytd_tax") else None,
                        ytd_super_cents=_cents(data.get("ytd_super")) if data.get("ytd_super") else None,
                        hours_worked=data.get("hours_worked"),
                        allowances_json=json.dumps(data.get("allowances") or []),
                        deductions_json=json.dumps(data.get("deductions") or []),
                        flags_json=json.dumps(flags),
                        raw_extracted=json.dumps(data),
                        source="watch",
                        filename=filename,
                        is_reviewed=False,
                        user_id=1,
                    )
                    session.add(payslip)
                    session.commit()
                    session.refresh(payslip)
                    _save_pdf(payslip.id, file_bytes)
                    entry.update({
                        "status": "ok",
                        "employer": employer,
                        "pay_date": str(pay_date),
                        "gross_pay": payslip.gross_pay_cents / 100,
                        "flag_count": len(flags),
                    })

            # Move to processed/
            dst = os.path.join(PAYSLIP_WATCH_DIR, "processed", filename)
            if os.path.exists(dst):
                base, ext = os.path.splitext(filename)
                dst = os.path.join(PAYSLIP_WATCH_DIR, "processed",
                                   f"{base}_{datetime.now().strftime('%H%M%S')}{ext}")
            shutil.move(src, dst)

            # HA notification on success (uses ha_notify_targets from settings)
            if entry.get("status") == "ok":
                try:
                    import requests
                    from sqlalchemy import text as _text
                    from database import engine as _engine
                    token = os.environ.get("SUPERVISOR_TOKEN", "")
                    if token:
                        with _engine.connect() as _conn:
                            row = _conn.execute(_text("SELECT value FROM setting WHERE key='ha_notify_targets'")).fetchone()
                        targets_str = row[0] if row else ""
                        targets = [t.strip() for t in targets_str.split(",") if t.strip()]
                        msg = f"{employer} — {pay_date}, gross ${payslip.gross_pay_cents / 100:,.2f}"
                        for target in targets:
                            try:
                                requests.post(
                                    f"http://supervisor/core/api/services/notify/{target}",
                                    headers={"Authorization": f"Bearer {token}"},
                                    json={"title": "Finance Tracker — Payslip imported", "message": msg},
                                    timeout=5,
                                )
                            except Exception:
                                pass
                except Exception:
                    pass

        except Exception as e:
            entry.update({"status": "error", "detail": str(e)[:200]})
            try:
                import shutil as _sh
                dst = os.path.join(PAYSLIP_WATCH_DIR, "failed", filename)
                _sh.move(src, dst)
            except Exception:
                pass

        _payslip_watch_log.insert(0, entry)
        if len(_payslip_watch_log) > 20:
            _payslip_watch_log.pop()
