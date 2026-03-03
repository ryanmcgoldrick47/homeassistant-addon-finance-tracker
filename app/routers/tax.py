from __future__ import annotations

import csv
import io
import os
import zipfile
from datetime import date
from typing import Optional
from fastapi import APIRouter, Depends
from fastapi.responses import HTMLResponse, StreamingResponse
from sqlmodel import Session, select, func

from database import Transaction, Category, Payslip, get_session
from deps import get_setting

RECEIPTS_DIR = "/config/finance_tracker/receipts"

router = APIRouter(prefix="/api/tax", tags=["tax"])

WFH_RATE_PER_HOUR = 0.67  # ATO fixed rate method 2024+
WFH_HOURS_PER_DAY = 8


@router.get("/summary")
def tax_summary(
    fy: int,  # financial year ending, e.g. 2025 = Jul 2024 – Jun 2025
    session: Session = Depends(get_session),
):
    start = date(fy - 1, 7, 1)
    end = date(fy, 6, 30)

    txns = session.exec(
        select(Transaction).where(
            Transaction.date >= start,
            Transaction.date <= end,
        )
    ).all()

    total_income = 0.0
    total_deductible = 0.0
    by_tax_category: dict[str, float] = {}
    deductible_transactions = []

    for t in txns:
        if t.is_credit:
            total_income += t.amount
        elif t.tax_deductible:
            total_deductible += t.amount
            key = t.tax_category or "General"
            by_tax_category[key] = by_tax_category.get(key, 0) + t.amount
            cat = session.get(Category, t.category_id) if t.category_id else None
            deductible_transactions.append({
                "date": str(t.date),
                "description": t.description,
                "amount": t.amount,
                "tax_category": t.tax_category or "General",
                "category_name": cat.name if cat else None,
                "notes": t.notes,
                "receipt_path": t.receipt_path,
            })

    # WFH deduction
    wfh_days_str = get_setting(session, "wfh_days", "0")
    wfh_days = int(wfh_days_str) if wfh_days_str.isdigit() else 0
    wfh_deduction = wfh_days * WFH_HOURS_PER_DAY * WFH_RATE_PER_HOUR

    # GST estimate for sole traders (1/11 of GST-inclusive amounts)
    gst_collected = total_income / 11
    gst_on_expenses = total_deductible / 11

    return {
        "fy": fy,
        "fy_label": f"{fy-1}–{fy}",
        "period": {"start": str(start), "end": str(end)},
        "total_income": round(total_income, 2),
        "total_deductible": round(total_deductible, 2),
        "wfh_days": wfh_days,
        "wfh_deduction": round(wfh_deduction, 2),
        "total_deductions": round(total_deductible + wfh_deduction, 2),
        "taxable_income_estimate": round(total_income - total_deductible - wfh_deduction, 2),
        "by_tax_category": {k: round(v, 2) for k, v in by_tax_category.items()},
        "gst_collected_estimate": round(gst_collected, 2),
        "gst_on_expenses_estimate": round(gst_on_expenses, 2),
        "net_gst_payable": round(gst_collected - gst_on_expenses, 2),
        "deductible_transactions": deductible_transactions,
    }


@router.get("/export", response_class=HTMLResponse)
def export_tax_summary(fy: int, session: Session = Depends(get_session)):
    data = tax_summary(fy=fy, session=session)

    rows = "".join(
        f"""<tr>
            <td>{t['date']}</td>
            <td>{t['description']}</td>
            <td>{t.get('category_name','')}</td>
            <td>{t.get('tax_category','')}</td>
            <td style="text-align:right">${t['amount']:.2f}</td>
            <td>{t.get('notes','')}</td>
        </tr>"""
        for t in data["deductible_transactions"]
    )

    by_cat_rows = "".join(
        f"<tr><td>{k}</td><td style='text-align:right'>${v:.2f}</td></tr>"
        for k, v in data["by_tax_category"].items()
    )

    html = f"""<!DOCTYPE html>
<html><head><meta charset="utf-8">
<title>Tax Summary FY{data['fy_label']}</title>
<style>
  body {{ font-family: Arial, sans-serif; margin: 40px; color: #333; }}
  h1 {{ color: #1e3a5f; }}
  h2 {{ color: #374151; border-bottom: 2px solid #e5e7eb; padding-bottom: 6px; }}
  table {{ border-collapse: collapse; width: 100%; margin-bottom: 24px; }}
  th, td {{ border: 1px solid #d1d5db; padding: 8px 12px; font-size: 13px; }}
  th {{ background: #f3f4f6; font-weight: 600; }}
  .summary-grid {{ display: grid; grid-template-columns: 1fr 1fr; gap: 20px; margin-bottom: 30px; }}
  .card {{ background: #f9fafb; border: 1px solid #e5e7eb; border-radius: 8px; padding: 16px; }}
  .card h3 {{ margin: 0 0 8px; font-size: 14px; color: #6b7280; }}
  .card .value {{ font-size: 24px; font-weight: 700; color: #1e3a5f; }}
  @media print {{ body {{ margin: 20px; }} }}
</style>
</head>
<body>
<h1>Australian Tax Summary — FY {data['fy_label']}</h1>
<p><strong>Period:</strong> {data['period']['start']} to {data['period']['end']} &nbsp;|&nbsp;
   <strong>Prepared:</strong> {date.today()}</p>

<div class="summary-grid">
  <div class="card"><h3>Total Income</h3><div class="value">${data['total_income']:,.2f}</div></div>
  <div class="card"><h3>Total Deductions</h3><div class="value">${data['total_deductions']:,.2f}</div></div>
  <div class="card"><h3>WFH Deduction ({data['wfh_days']} days × {WFH_HOURS_PER_DAY}h × ${WFH_RATE_PER_HOUR})</h3>
    <div class="value">${data['wfh_deduction']:,.2f}</div></div>
  <div class="card"><h3>Estimated Taxable Income</h3><div class="value">${data['taxable_income_estimate']:,.2f}</div></div>
</div>

<h2>Deductions by ATO Category</h2>
<table><thead><tr><th>Category</th><th>Amount</th></tr></thead>
<tbody>{by_cat_rows}</tbody>
<tfoot><tr><td><strong>Total</strong></td>
<td style="text-align:right"><strong>${data['total_deductible']:,.2f}</strong></td></tr></tfoot>
</table>

<h2>GST Estimates (Sole Trader)</h2>
<table>
  <tr><td>GST collected on income (1/11)</td><td style="text-align:right">${data['gst_collected_estimate']:,.2f}</td></tr>
  <tr><td>GST credits on expenses (1/11)</td><td style="text-align:right">${data['gst_on_expenses_estimate']:,.2f}</td></tr>
  <tr><td><strong>Net GST payable (estimate)</strong></td><td style="text-align:right"><strong>${data['net_gst_payable']:,.2f}</strong></td></tr>
</table>

<h2>Itemised Deductible Transactions</h2>
<table>
  <thead><tr><th>Date</th><th>Description</th><th>Category</th><th>ATO Category</th><th>Amount</th><th>Notes</th></tr></thead>
  <tbody>{rows}</tbody>
</table>

<p style="color:#6b7280;font-size:12px;margin-top:40px;">
  This summary is a guide only. Consult a registered tax agent for advice.
</p>
</body></html>"""
    return html


@router.get("/audit-export")
def audit_export(fy: int, session: Session = Depends(get_session)):
    """
    Download a ZIP containing:
    - deductible_transactions_FY{fy}.csv  — all tax-deductible transactions
    - receipts/                            — attached receipt files
    - README.txt                           — summary totals
    """
    start = date(fy - 1, 7, 1)
    end = date(fy, 6, 30)

    txns = session.exec(
        select(Transaction).where(
            Transaction.date >= start,
            Transaction.date <= end,
            Transaction.tax_deductible == True,
            Transaction.is_credit == False,
        ).order_by(Transaction.date)
    ).all()

    total = round(sum(t.amount for t in txns), 2)
    receipts_attached = sum(1 for t in txns if t.receipt_path)
    missing_receipts = [t for t in txns if not t.receipt_path and t.amount >= 300]

    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as zf:
        # CSV
        csv_buf = io.StringIO()
        writer = csv.writer(csv_buf)
        writer.writerow(["Date", "Description", "Category", "ATO Category", "Amount", "Notes", "Receipt"])
        for t in txns:
            cat = session.get(Category, t.category_id) if t.category_id else None
            writer.writerow([
                str(t.date),
                t.description,
                cat.name if cat else "",
                t.tax_category or "General",
                f"{t.amount:.2f}",
                t.notes or "",
                t.receipt_path or "",
            ])
        zf.writestr(f"deductible_transactions_FY{fy}.csv", csv_buf.getvalue())

        # Receipts
        for t in txns:
            if not t.receipt_path:
                continue
            src = os.path.join(RECEIPTS_DIR, os.path.basename(t.receipt_path))
            if os.path.exists(src):
                with open(src, "rb") as f:
                    zf.writestr(f"receipts/{t.receipt_path}", f.read())

        # README
        missing_lines = "\n".join(
            f"  - {t.date}  {t.description}  ${t.amount:.2f}"
            for t in missing_receipts
        ) or "  None"

        readme = f"""ATO Tax Deduction Audit Pack — FY {fy-1}–{fy}
Generated: {date.today()}
Period: {start} to {end}

SUMMARY
-------
Total deductible transactions : {len(txns)}
Total deductions               : ${total:,.2f}
Receipts attached              : {receipts_attached} of {len(txns)}

MISSING RECEIPTS (≥ $300 — ATO substantiation required)
--------------------------------------------------------
{missing_lines}

NOTE: This pack is a guide only. Consult a registered tax agent for advice.
"""
        zf.writestr("README.txt", readme)

    buf.seek(0)
    filename = f"AuditPack_FY{fy}.zip"
    return StreamingResponse(
        buf,
        media_type="application/zip",
        headers={"Content-Disposition": f'attachment; filename="{filename}"'},
    )


@router.patch("/wfh")
def update_wfh_days(days: int, session: Session = Depends(get_session)):
    from deps import set_setting
    set_setting(session, "wfh_days", str(days))
    return {"wfh_days": days}


# ---------------------------------------------------------------------------
# Tax brackets and helpers — ATO 2024-25 Stage 3 revised (from 1 Jul 2024)
# ---------------------------------------------------------------------------

_BRACKETS = [
    (18_200,    0,       0.00),
    (45_000,    0,       0.16),
    (135_000,   4_288,   0.30),
    (190_000,   31_288,  0.37),
    (None,      51_638,  0.45),
]

_HELP_RATES = [
    (54_435, 0.000), (62_738, 0.010), (66_308, 0.020), (70_088, 0.025),
    (74_106, 0.030), (79_207, 0.035), (84_743, 0.040), (90_596, 0.045),
    (96_943, 0.050), (103_766, 0.055), (111_113, 0.060), (119_190, 0.065),
    (127_437, 0.070), (136_357, 0.075), (145_926, 0.080), (156_134, 0.085),
    (166_951, 0.090), (178_615, 0.095), (None, 0.100),
]


def _income_tax(taxable: float) -> float:
    if taxable <= 0:
        return 0.0
    prev = 0
    for threshold, base, rate in _BRACKETS:
        if threshold is None or taxable <= threshold:
            return round(base + (taxable - prev) * rate, 2)
        prev = threshold
    return 0.0


def _lito(taxable: float) -> float:
    if taxable <= 37_500:
        return 700.0
    if taxable <= 45_000:
        return round(700 - (taxable - 37_500) * 0.05, 2)
    if taxable <= 66_667:
        return round(max(0, 325 - (taxable - 45_000) * 0.015), 2)
    return 0.0


def _medicare(taxable: float) -> float:
    if taxable <= 26_000:
        return 0.0
    if taxable <= 32_500:
        return round((taxable - 26_000) * 0.10, 2)
    return round(taxable * 0.02, 2)


def _help_repayment(taxable: float) -> float:
    for upper, rate in _HELP_RATES:
        if upper is None or taxable <= upper:
            return round(taxable * rate, 2)
    return 0.0


@router.get("/estimate")
def tax_estimate(fy: int, session: Session = Depends(get_session)):
    """
    Project annual income tax using payslip data.
    fy = year ending, e.g. 2026 = Jul 2025 – Jun 2026.
    Uses 2024-25 Stage 3 revised ATO tax brackets.
    """
    fy_start = date(fy - 1, 7, 1)
    fy_end = date(fy, 6, 30)

    payslips = session.exec(
        select(Payslip).where(
            Payslip.pay_date >= fy_start,
            Payslip.pay_date <= fy_end,
        ).order_by(Payslip.pay_date.asc())
    ).all()

    if not payslips:
        return {
            "has_data": False,
            "fy": fy,
            "fy_label": f"{fy-1}–{fy}",
            "message": "No payslips found for this financial year.",
        }

    latest = payslips[-1]
    today = date.today()
    reference_date = min(today, fy_end)
    days_elapsed = max((reference_date - fy_start).days, 1)
    weeks_elapsed = days_elapsed / 7

    # Prefer YTD gross if available on the latest payslip
    if latest.ytd_gross_cents and latest.ytd_gross_cents > 0:
        ytd_gross = latest.ytd_gross_cents / 100
        ytd_tax_withheld = (latest.ytd_tax_cents or 0) / 100
    else:
        ytd_gross = sum(p.gross_pay_cents for p in payslips) / 100
        ytd_tax_withheld = sum(p.tax_withheld_cents for p in payslips) / 100

    projected_annual_gross = round(ytd_gross / weeks_elapsed * 52, 2)
    projected_annual_tax_withheld = round(ytd_tax_withheld / weeks_elapsed * 52, 2)

    # Transaction deductions (tax_deductible=True)
    txn_deductions = float(session.exec(
        select(func.coalesce(func.sum(Transaction.amount), 0)).where(
            Transaction.date >= fy_start,
            Transaction.date <= fy_end,
            Transaction.tax_deductible == True,
            Transaction.is_credit == False,
        )
    ).one())

    wfh_days_str = get_setting(session, "wfh_days", "0")
    wfh_days = int(wfh_days_str) if wfh_days_str.isdigit() else 0
    wfh_deduction = wfh_days * WFH_HOURS_PER_DAY * WFH_RATE_PER_HOUR

    taxable = max(projected_annual_gross - txn_deductions - wfh_deduction, 0)

    ato_tax = _income_tax(taxable)
    lito = _lito(taxable)
    medicare = _medicare(taxable)
    help_amt = _help_repayment(taxable)
    net_tax = round(max(ato_tax - lito + medicare, 0), 2)
    refund_or_bill = round(projected_annual_tax_withheld - net_tax, 2)

    return {
        "has_data": True,
        "fy": fy,
        "fy_label": f"{fy-1}–{fy}",
        "payslip_count": len(payslips),
        "latest_pay_date": str(latest.pay_date),
        "pay_frequency": latest.pay_frequency or "unknown",
        "weeks_elapsed": round(weeks_elapsed, 1),
        "ytd_gross": round(ytd_gross, 2),
        "ytd_tax_withheld": round(ytd_tax_withheld, 2),
        "projected_annual_gross": projected_annual_gross,
        "projected_annual_tax_withheld": projected_annual_tax_withheld,
        "total_deductions": round(txn_deductions, 2),
        "wfh_deduction": round(wfh_deduction, 2),
        "taxable_income": round(taxable, 2),
        "ato_tax": ato_tax,
        "lito": lito,
        "medicare_levy": medicare,
        "help_repayment": help_amt,
        "net_tax": net_tax,
        "estimated_refund": refund_or_bill,
        "brackets_year": "2024-25 Stage 3 revised",
    }
