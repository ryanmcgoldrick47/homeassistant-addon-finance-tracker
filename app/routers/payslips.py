from __future__ import annotations

import io
import json
import os
from datetime import date
from typing import Optional

from fastapi import APIRouter, Depends, HTTPException, UploadFile, File
from sqlmodel import Session, select

from database import Payslip, Setting, get_session
from deps import get_setting

router = APIRouter(prefix="/api/payslips", tags=["payslips"])

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
def list_payslips(session: Session = Depends(get_session)):
    rows = session.exec(select(Payslip).order_by(Payslip.pay_date.desc())).all()
    result = []
    for p in rows:
        result.append({
            **_payslip_dict(p),
            "flags": json.loads(p.flags_json or "[]"),
            "allowances": json.loads(p.allowances_json or "[]"),
            "deductions": json.loads(p.deductions_json or "[]"),
        })
    return result


@router.get("/summary")
def payslip_summary(session: Session = Depends(get_session)):
    """Latest leave balances, YTD totals, pay stats."""
    latest = session.exec(select(Payslip).order_by(Payslip.pay_date.desc()).limit(1)).first()
    if not latest:
        return {"has_data": False}

    # YTD from latest payslip (most accurate)
    return {
        "has_data": True,
        "latest_pay_date": str(latest.pay_date),
        "employer": latest.employer,
        "annual_leave_hours": latest.annual_leave_hours,
        "sick_leave_hours": latest.sick_leave_hours,
        "long_service_hours": latest.long_service_hours,
        "ytd_gross": (latest.ytd_gross_cents or 0) / 100,
        "ytd_tax": (latest.ytd_tax_cents or 0) / 100,
        "ytd_super": (latest.ytd_super_cents or 0) / 100,
        "ytd_net": ((latest.ytd_gross_cents or 0) - (latest.ytd_tax_cents or 0)) / 100,
        "latest_gross": latest.gross_pay_cents / 100,
        "latest_net": latest.net_pay_cents / 100,
        "latest_super": latest.super_cents / 100,
        "pay_frequency": latest.pay_frequency,
        "unreviewed_flags": sum(
            1 for p in session.exec(select(Payslip).where(Payslip.is_reviewed == False)).all()
            if json.loads(p.flags_json or "[]")
        ),
    }


@router.post("/upload")
async def upload_payslip(
    file: UploadFile = File(...),
    session: Session = Depends(get_session),
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

    # Check for duplicate (same pay_date + employer)
    employer = data.get("employer") or ""
    existing = session.exec(
        select(Payslip).where(Payslip.pay_date == pay_date, Payslip.employer == employer)
    ).first()
    if existing:
        raise HTTPException(409, f"A payslip for {pay_date} from {employer} already exists (ID {existing.id}).")

    # Get previous payslip for comparison
    prev = session.exec(
        select(Payslip).where(Payslip.pay_date < pay_date).order_by(Payslip.pay_date.desc()).limit(1)
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
    )
    session.add(payslip)
    session.commit()
    session.refresh(payslip)

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
                select(Payslip).where(Payslip.pay_date == pay_date, Payslip.employer == employer)
            ).first()
            if existing:
                result["status"] = "duplicate"
                result["detail"] = f"Already exists (ID {existing.id}) for {pay_date}"
                result["pay_date"] = str(pay_date)
                results.append(result)
                continue

            prev = session.exec(
                select(Payslip).where(Payslip.pay_date < pay_date).order_by(Payslip.pay_date.desc()).limit(1)
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
            )
            session.add(payslip)
            session.commit()
            session.refresh(payslip)

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


@router.patch("/{payslip_id}/review")
def mark_reviewed(payslip_id: int, session: Session = Depends(get_session)):
    p = session.get(Payslip, payslip_id)
    if not p:
        raise HTTPException(404, "Not found")
    p.is_reviewed = True
    session.add(p)
    session.commit()
    return {"ok": True}


@router.delete("/{payslip_id}")
def delete_payslip(payslip_id: int, session: Session = Depends(get_session)):
    p = session.get(Payslip, payslip_id)
    if not p:
        raise HTTPException(404, "Not found")
    session.delete(p)
    session.commit()
    return {"ok": True}


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
        "flag_count": len(json.loads(p.flags_json or "[]")),
    }
