from __future__ import annotations

import csv
import io
import os
import zipfile
from datetime import date, datetime
from typing import Optional
from fastapi import APIRouter, Depends
from fastapi.responses import HTMLResponse, StreamingResponse
from sqlmodel import Session, select, func

from database import Transaction, Category, Payslip, Dividend, Disposal, User, get_session
from deps import get_current_user, get_setting, set_setting

import os as _os
_DATA_DIR    = _os.environ.get("FINANCE_DATA_DIR", "/data")
RECEIPTS_DIR = _os.path.join(_DATA_DIR, "receipts")
PAYSLIPS_DIR = _os.path.join(_DATA_DIR, "payslips")

router = APIRouter(prefix="/api/tax", tags=["tax"])

WFH_RATE_PER_HOUR = 0.67  # ATO fixed rate method 2024+
WFH_HOURS_PER_DAY = 8


@router.get("/ato-prefill")
def ato_prefill(
    fy: int,
    current_user: User = Depends(get_current_user),
    session: Session = Depends(get_session),
):
    """ATO-structured tax prefill data for a financial year."""
    from database import Dividend, Disposal
    start = date(fy - 1, 7, 1)
    end = date(fy, 6, 30)

    # Item 1: Salary from payslips
    payslips = session.exec(
        select(Payslip).where(
            Payslip.user_id == current_user.id,
            Payslip.pay_date >= start,
            Payslip.pay_date <= end,
        )
    ).all()
    salary_gross = sum(p.gross_pay_cents for p in payslips) / 100
    salary_tax = sum(p.tax_withheld_cents for p in payslips) / 100

    # Item 11: Dividends
    dividends = session.exec(
        select(Dividend).where(
            Dividend.user_id == current_user.id,
            Dividend.pay_date >= start,
            Dividend.pay_date <= end,
        )
    ).all()
    div_cash = sum(d.amount_aud for d in dividends)
    div_franking = sum(d.franking_credits_aud for d in dividends)
    div_grossed_up = div_cash + div_franking

    # Item 18: CGT from disposals split by asset type
    disposals = session.exec(
        select(Disposal).where(
            Disposal.user_id == current_user.id,
            Disposal.disposed_date >= start,
            Disposal.disposed_date <= end,
        )
    ).all()
    shares_gains = sum(d.gain_aud for d in disposals if d.asset_type == "share" and d.gain_aud > 0)
    shares_losses = abs(sum(d.gain_aud for d in disposals if d.asset_type == "share" and d.gain_aud < 0))
    shares_discount = sum(d.gain_aud * 0.5 for d in disposals if d.asset_type == "share" and d.gain_aud > 0 and d.discount_eligible)
    crypto_gains = sum(d.gain_aud for d in disposals if d.asset_type == "crypto" and d.gain_aud > 0)
    crypto_losses = abs(sum(d.gain_aud for d in disposals if d.asset_type == "crypto" and d.gain_aud < 0))
    crypto_discount = sum(d.gain_aud * 0.5 for d in disposals if d.asset_type == "crypto" and d.gain_aud > 0 and d.discount_eligible)
    net_cgt = (shares_gains - shares_discount - shares_losses) + (crypto_gains - crypto_discount - crypto_losses)

    # WFH deduction
    wfh_days_str = get_setting(session, "wfh_days", "0")
    wfh_days = int(wfh_days_str) if wfh_days_str.isdigit() else 0
    wfh_deduction = round(wfh_days * WFH_HOURS_PER_DAY * WFH_RATE_PER_HOUR, 2)

    # D5 other deductions
    txns_d5 = session.exec(
        select(Transaction).where(
            Transaction.user_id == current_user.id,
            Transaction.tax_deductible == True,
            Transaction.date >= start,
            Transaction.date <= end,
        )
    ).all()
    deductions_d5 = sum(t.amount for t in txns_d5)

    # Manual overrides from settings
    def _manual(key, default=0.0):
        v = get_setting(session, f"tax_{fy}_{key}", "")
        try:
            return float(v) if v else default
        except ValueError:
            return default

    return {
        "fy": fy,
        "item1_salary": {"gross": round(salary_gross, 2), "tax_withheld": round(salary_tax, 2)},
        "item11_dividends": {"cash": round(div_cash, 2), "franking_credits": round(div_franking, 2), "grossed_up": round(div_grossed_up, 2)},
        "item18_cgt": {
            "shares_gains": round(shares_gains, 2),
            "shares_losses": round(shares_losses, 2),
            "shares_discount": round(shares_discount, 2),
            "crypto_gains": round(crypto_gains, 2),
            "crypto_losses": round(crypto_losses, 2),
            "crypto_discount": round(crypto_discount, 2),
            "net_cgt": round(max(net_cgt, 0), 2),
        },
        "deductions_d1_wfh": wfh_deduction,
        "deductions_d5_other": round(deductions_d5, 2),
        "manual_overrides": {
            "interest_income": _manual("interest"),
            "rental_income": _manual("rental"),
            "other_income": _manual("other_income"),
            "d3_self_education": _manual("d3_self_education"),
            "d4_other_work": _manual("d4_other_work"),
        },
    }


@router.patch("/ato-manual")
def ato_manual_save(
    data: dict,
    current_user: User = Depends(get_current_user),
    session: Session = Depends(get_session),
):
    """Save a manual override field for ATO prefill. Body: {fy, field, value}"""
    fy = data.get("fy")
    field = data.get("field")
    value = data.get("value", 0)
    allowed_fields = {"interest", "rental", "other_income", "d3_self_education", "d4_other_work"}
    if not fy or field not in allowed_fields:
        from fastapi import HTTPException
        raise HTTPException(400, f"Invalid field. Allowed: {allowed_fields}")
    set_setting(session, f"tax_{fy}_{field}", str(value))
    return {"ok": True}


@router.get("/summary")
def tax_summary(
    fy: int,
    current_user: User = Depends(get_current_user),
    session: Session = Depends(get_session),
):
    start = date(fy - 1, 7, 1)
    end = date(fy, 6, 30)

    txns = session.exec(
        select(Transaction).where(
            Transaction.user_id == current_user.id,
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
def export_tax_summary(
    fy: int,
    current_user: User = Depends(get_current_user),
    session: Session = Depends(get_session),
):
    data = tax_summary(fy=fy, current_user=current_user, session=session)

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
def audit_export(
    fy: int,
    current_user: User = Depends(get_current_user),
    session: Session = Depends(get_session),
):
    """
    Download a ZIP containing:
    - deductible_transactions_FY{fy}.csv  — all tax-deductible transactions
    - receipts/                            — attached receipt files
    - dividend_income_FY{fy}.csv          — all dividends for the FY
    - payslips_FY{fy}/payslip_summary.csv — payslip summary for the FY
    - payslips_FY{fy}/payslip_{id}.pdf    — individual payslip PDFs (if available)
    - tax_summary_FY{fy}.html             — ATO-style printable summary
    - README.txt                           — totals and missing receipt warnings
    """
    uid = current_user.id
    start = date(fy - 1, 7, 1)
    end = date(fy, 6, 30)

    # --- Deductible transactions ---
    txns = session.exec(
        select(Transaction).where(
            Transaction.user_id == uid,
            Transaction.date >= start,
            Transaction.date <= end,
            Transaction.tax_deductible == True,
            Transaction.is_credit == False,
        ).order_by(Transaction.date)
    ).all()

    total_deductions = round(sum(t.amount for t in txns), 2)
    receipts_attached = sum(1 for t in txns if t.receipt_path)
    missing_receipts = [t for t in txns if not t.receipt_path and t.amount >= 300]

    # --- Dividends ---
    dividends = session.exec(
        select(Dividend).where(
            Dividend.user_id == uid,
            Dividend.pay_date >= start,
            Dividend.pay_date <= end,
        ).order_by(Dividend.pay_date)
    ).all()

    # --- Payslips ---
    payslips = session.exec(
        select(Payslip).where(
            Payslip.user_id == uid,
            Payslip.pay_date >= start,
            Payslip.pay_date <= end,
        ).order_by(Payslip.pay_date)
    ).all()

    # --- CGT disposals (for summary) ---
    disposals = session.exec(
        select(Disposal).where(
            Disposal.user_id == uid,
            Disposal.disposed_date >= start,
            Disposal.disposed_date <= end,
        )
    ).all()
    net_cgt = round(sum(d.gain_aud for d in disposals), 2)

    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as zf:

        # 1. Deductible transactions CSV
        csv_buf = io.StringIO()
        writer = csv.writer(csv_buf)
        writer.writerow(["Date", "Description", "Category", "ATO Category", "Amount", "Notes", "Receipt"])
        by_tax_cat: dict[str, float] = {}
        for t in txns:
            cat = session.get(Category, t.category_id) if t.category_id else None
            key = t.tax_category or "General"
            by_tax_cat[key] = by_tax_cat.get(key, 0) + t.amount
            writer.writerow([
                str(t.date), t.description, cat.name if cat else "",
                key, f"{t.amount:.2f}", t.notes or "", t.receipt_path or "",
            ])
        zf.writestr(f"deductible_transactions_FY{fy}.csv", csv_buf.getvalue())

        # 2. Receipt files
        for t in txns:
            if not t.receipt_path:
                continue
            src = os.path.join(RECEIPTS_DIR, os.path.basename(t.receipt_path))
            if os.path.exists(src):
                with open(src, "rb") as f:
                    zf.writestr(f"receipts/{t.receipt_path}", f.read())

        # 3. Dividend income CSV
        if dividends:
            div_buf = io.StringIO()
            dw = csv.writer(div_buf)
            dw.writerow(["Ticker", "Pay Date", "Ex Date", "Cash Amount (AUD)",
                         "Franking Credits (AUD)", "Grossed-Up Income (AUD)", "Notes"])
            for d in dividends:
                grossed_up = round(d.amount_aud + d.franking_credits_aud, 2)
                dw.writerow([
                    d.ticker, str(d.pay_date), str(d.ex_date) if d.ex_date else "",
                    f"{d.amount_aud:.2f}", f"{d.franking_credits_aud:.2f}",
                    f"{grossed_up:.2f}", d.notes or "",
                ])
            zf.writestr(f"dividend_income_FY{fy}.csv", div_buf.getvalue())

        # 4. Payslip summary CSV + individual PDFs
        if payslips:
            ps_buf = io.StringIO()
            pw = csv.writer(ps_buf)
            pw.writerow(["Pay Date", "Period Start", "Period End", "Employer",
                         "Gross Pay", "Net Pay", "Tax Withheld", "Super"])
            for p in payslips:
                pw.writerow([
                    str(p.pay_date),
                    str(p.period_start) if p.period_start else "",
                    str(p.period_end) if p.period_end else "",
                    p.employer or "",
                    f"{p.gross_pay_cents / 100:.2f}",
                    f"{p.net_pay_cents / 100:.2f}",
                    f"{p.tax_withheld_cents / 100:.2f}",
                    f"{p.super_cents / 100:.2f}",
                ])
            zf.writestr(f"payslips_FY{fy}/payslip_summary.csv", ps_buf.getvalue())

            for p in payslips:
                pdf_src = os.path.join(PAYSLIPS_DIR, f"payslip_{p.id}.pdf")
                if os.path.exists(pdf_src):
                    with open(pdf_src, "rb") as f:
                        fname = f"payslip_{p.id}_{p.pay_date}.pdf"
                        zf.writestr(f"payslips_FY{fy}/{fname}", f.read())

        # 5. ATO-style HTML tax summary
        # Income from payslips
        total_gross = sum(p.gross_pay_cents for p in payslips) / 100
        total_tax_withheld = sum(p.tax_withheld_cents for p in payslips) / 100
        total_super = sum(p.super_cents for p in payslips) / 100

        # Prefer YTD from latest payslip if available
        if payslips and payslips[-1].ytd_gross_cents:
            total_gross = payslips[-1].ytd_gross_cents / 100
            total_tax_withheld = (payslips[-1].ytd_tax_cents or 0) / 100
            total_super = (payslips[-1].ytd_super_cents or 0) / 100

        total_div_cash = round(sum(d.amount_aud for d in dividends), 2)
        total_franking = round(sum(d.franking_credits_aud for d in dividends), 2)
        total_grossed_up = round(total_div_cash + total_franking, 2)

        wfh_days_str = get_setting(session, "wfh_days", "0")
        wfh_days = int(wfh_days_str) if wfh_days_str.isdigit() else 0
        wfh_deduction = round(wfh_days * WFH_HOURS_PER_DAY * WFH_RATE_PER_HOUR, 2)

        est_taxable = round(
            total_gross + total_grossed_up - total_deductions - wfh_deduction, 2
        )

        deduct_rows = "".join(
            f"<tr><td>{k}</td><td style='text-align:right'>${v:,.2f}</td></tr>"
            for k, v in by_tax_cat.items()
        )
        if wfh_deduction:
            deduct_rows += f"<tr><td>WFH ({wfh_days} days)</td><td style='text-align:right'>${wfh_deduction:,.2f}</td></tr>"
        total_all_deductions = round(total_deductions + wfh_deduction, 2)

        miss_rows = "".join(
            f"<tr><td>{t.date}</td><td>{t.description}</td><td style='text-align:right'>${t.amount:,.2f}</td></tr>"
            for t in missing_receipts
        ) or "<tr><td colspan='3' style='color:#16a34a'>All transactions ≥ $300 have receipts attached ✓</td></tr>"

        if not dividends:
            dividend_section = "<p style='color:#6b7280'>No dividends recorded for this financial year.</p>"
        else:
            dividend_section = (
                "<table><tr><th>Item</th><th>Amount</th></tr>"
                f"<tr><td>Cash Dividends Received</td><td style='text-align:right'>${total_div_cash:,.2f}</td></tr>"
                f"<tr><td>Imputation (Franking) Credits</td><td style='text-align:right'>${total_franking:,.2f}</td></tr>"
                f"<tr style='font-weight:700'><td>Grossed-Up Dividend Income (Include in Tax Return)</td><td style='text-align:right'>${total_grossed_up:,.2f}</td></tr>"
                "</table><p style='font-size:12px;color:#6b7280'>Include the grossed-up amount as income; claim franking credits as an offset. See ATO Item 11.</p>"
            )

        if not disposals:
            cgt_section = "<p style='color:#6b7280'>No CGT disposals recorded for this financial year.</p>"
        else:
            cgt_section = (
                f"<p>{len(disposals)} disposal(s) recorded this FY. "
                f"Net capital gain/loss: <strong>${net_cgt:+,.2f}</strong>. "
                f"See <code>dividend_income_FY{fy}.csv</code> and Investments &gt; CGT page for details.</p>"
            )

        html_summary = f"""<!DOCTYPE html>
<html><head><meta charset="utf-8">
<title>Tax Summary FY{fy-1}–{fy}</title>
<style>
  body{{font-family:Arial,sans-serif;margin:40px;color:#111;max-width:860px}}
  h1{{color:#1e3a5f;border-bottom:3px solid #6366f1;padding-bottom:10px}}
  h2{{color:#374151;border-bottom:1px solid #d1d5db;padding-bottom:4px;margin-top:32px}}
  table{{border-collapse:collapse;width:100%;margin-bottom:20px}}
  th,td{{border:1px solid #d1d5db;padding:8px 12px;font-size:13px}}
  th{{background:#f3f4f6;font-weight:600;text-align:left}}
  .grid{{display:grid;grid-template-columns:1fr 1fr 1fr;gap:16px;margin:20px 0}}
  .card{{background:#f9fafb;border:1px solid #e5e7eb;border-radius:8px;padding:14px}}
  .card .lbl{{font-size:12px;color:#6b7280;margin-bottom:4px}}
  .card .val{{font-size:22px;font-weight:700;color:#1e3a5f}}
  .card .val.green{{color:#16a34a}} .card .val.red{{color:#dc2626}}
  .disclaimer{{font-size:11px;color:#9ca3af;margin-top:40px;padding-top:16px;border-top:1px solid #e5e7eb}}
  @media print{{body{{margin:20px}} .no-print{{display:none}}}}
</style>
</head>
<body>
<h1>Tax Summary — FY {fy-1}–{fy}</h1>
<p><strong>Taxpayer:</strong> {current_user.name} &nbsp;|&nbsp;
   <strong>Period:</strong> {start} to {end} &nbsp;|&nbsp;
   <strong>Generated:</strong> {datetime.now().strftime('%d %b %Y')}</p>

<div class="grid">
  <div class="card"><div class="lbl">Employment Income</div><div class="val">${total_gross:,.2f}</div></div>
  <div class="card"><div class="lbl">Tax Withheld</div><div class="val">${total_tax_withheld:,.2f}</div></div>
  <div class="card"><div class="lbl">Employer Super</div><div class="val">${total_super:,.2f}</div></div>
  <div class="card"><div class="lbl">Dividend Income (Cash)</div><div class="val">${total_div_cash:,.2f}</div></div>
  <div class="card"><div class="lbl">Franking Credits</div><div class="val">${total_franking:,.2f}</div></div>
  <div class="card"><div class="lbl">Total Deductions</div><div class="val red">${total_all_deductions:,.2f}</div></div>
</div>

<h2>Employment Income (from Payslips)</h2>
<table>
  <tr><th>Item</th><th>Amount</th></tr>
  <tr><td>Gross Income (YTD)</td><td style='text-align:right'>${total_gross:,.2f}</td></tr>
  <tr><td>Tax Withheld (YTD)</td><td style='text-align:right'>${total_tax_withheld:,.2f}</td></tr>
  <tr><td>Employer Super (YTD)</td><td style='text-align:right'>${total_super:,.2f}</td></tr>
  <tr><td>Payslips in FY</td><td style='text-align:right'>{len(payslips)}</td></tr>
</table>

<h2>Work-Related Deductions</h2>
<table>
  <tr><th>ATO Category</th><th>Amount</th></tr>
  {deduct_rows}
  <tr style='font-weight:700'><td>Total Deductions</td><td style='text-align:right'>${total_all_deductions:,.2f}</td></tr>
</table>

<h2>Investment Income — Dividends</h2>
{dividend_section}

<h2>Capital Gains</h2>
{cgt_section}

<h2>Estimated Taxable Income</h2>
<table>
  <tr><td>Employment Income</td><td style='text-align:right'>${total_gross:,.2f}</td></tr>
  <tr><td>Grossed-Up Dividends</td><td style='text-align:right'>${total_grossed_up:,.2f}</td></tr>
  <tr><td>Less: Total Deductions</td><td style='text-align:right'>−${total_all_deductions:,.2f}</td></tr>
  <tr style='font-weight:700;background:#f3f4f6'><td>Estimated Taxable Income</td>
    <td style='text-align:right'>${est_taxable:,.2f}</td></tr>
</table>

<h2>Missing Receipts (ATO Substantiation ≥ $300)</h2>
<table>
  <tr><th>Date</th><th>Description</th><th>Amount</th></tr>
  {miss_rows}
</table>

<div class="disclaimer">
  <strong>Disclaimer:</strong> This summary is a guide only and does not constitute tax advice.
  Consult a registered tax agent before lodging your tax return.
  Generated by Finance Tracker on {datetime.now().strftime('%d %b %Y %H:%M')}.
</div>
</body></html>"""

        zf.writestr(f"tax_summary_FY{fy}.html", html_summary)

        # 6. README
        missing_lines = "\n".join(
            f"  - {t.date}  {t.description}  ${t.amount:.2f}"
            for t in missing_receipts
        ) or "  None"

        readme = f"""ATO Tax Deduction Audit Pack — FY {fy-1}–{fy}
Generated: {datetime.now().strftime('%d %b %Y %H:%M')}
Taxpayer:  {current_user.name}
Period:    {start} to {end}

CONTENTS
--------
  deductible_transactions_FY{fy}.csv  — itemised work-related deductions
  receipts/                            — attached receipt files
  dividend_income_FY{fy}.csv          — dividend income and franking credits
  payslips_FY{fy}/payslip_summary.csv — payslip summary
  payslips_FY{fy}/payslip_*.pdf       — individual payslip PDFs (if uploaded)
  tax_summary_FY{fy}.html             — printable ATO-style tax summary

SUMMARY
-------
  Total deductible transactions : {len(txns)}
  Total deductions               : ${total_deductions:,.2f}
  WFH deduction                  : ${wfh_deduction:,.2f}
  Receipts attached              : {receipts_attached} of {len(txns)}
  Dividends recorded             : {len(dividends)}
  Grossed-up dividend income     : ${total_grossed_up:,.2f}
  Payslips in FY                 : {len(payslips)}

MISSING RECEIPTS (≥ $300 — ATO substantiation required)
--------------------------------------------------------
{missing_lines}

NOTE: This pack is a guide only. Consult a registered tax agent for advice.
"""
        zf.writestr("README.txt", readme)

    buf.seek(0)
    filename = f"AuditPack_FY{fy}_{current_user.name}.zip"
    return StreamingResponse(
        buf,
        media_type="application/zip",
        headers={"Content-Disposition": f'attachment; filename="{filename}"'},
    )


@router.get("/fy-summary")
def fy_summary(
    fy: int,
    current_user: User = Depends(get_current_user),
    session: Session = Depends(get_session),
):
    """
    Comprehensive JSON FY summary for display on the Tax page.
    FY ending year, e.g. fy=2025 → FY2024-25 (1 Jul 2024 – 30 Jun 2025).
    """
    uid = current_user.id
    start = date(fy - 1, 7, 1)
    end   = date(fy, 6, 30)

    # --- Employment income from payslips ---
    payslips = session.exec(
        select(Payslip).where(
            Payslip.user_id == uid,
            Payslip.pay_date >= start,
            Payslip.pay_date <= end,
        )
    ).all()
    gross_income   = round(sum(p.gross_pay_cents for p in payslips) / 100, 2)
    tax_withheld   = round(sum(p.tax_withheld_cents for p in payslips) / 100, 2)
    super_total    = round(sum(p.super_cents for p in payslips) / 100, 2)
    payslip_count  = len(payslips)

    # --- Tax-deductible transactions ---
    txns = session.exec(
        select(Transaction).where(
            Transaction.user_id == uid,
            Transaction.date >= start,
            Transaction.date <= end,
            Transaction.tax_deductible == True,
            Transaction.is_credit == False,
        )
    ).all()
    deductions_total = round(sum(t.amount for t in txns), 2)
    receipts_ok      = sum(1 for t in txns if t.receipt_path)
    missing_receipt_high = [t for t in txns if not t.receipt_path and t.amount >= 300]

    # --- WFH deduction ---
    wfh_days       = int(get_setting(session, "wfh_days", "0") or 0)
    wfh_deduction  = round(wfh_days * WFH_HOURS_PER_DAY * WFH_RATE_PER_HOUR, 2)
    total_deductions = round(deductions_total + wfh_deduction, 2)

    # --- Dividends ---
    dividends = session.exec(
        select(Dividend).where(
            Dividend.user_id == uid,
            Dividend.pay_date >= start,
            Dividend.pay_date <= end,
        )
    ).all()
    div_cash      = round(sum(d.amount_aud for d in dividends), 2)
    div_franking  = round(sum(d.franking_credits_aud for d in dividends), 2)
    div_grossed   = round(div_cash + div_franking, 2)

    # --- CGT ---
    disposals = session.exec(
        select(Disposal).where(
            Disposal.user_id == uid,
            Disposal.disposed_date >= start,
            Disposal.disposed_date <= end,
        )
    ).all()
    net_cgt = round(sum(d.gain_aud for d in disposals), 2)
    cgt_discount_eligible = round(sum(
        d.gain_aud * 0.5 for d in disposals
        if d.gain_aud > 0 and d.discount_eligible
    ), 2)
    net_cgt_after_discount = round(net_cgt - cgt_discount_eligible, 2)

    # --- Deductions breakdown by category ---
    cat_totals: dict[str, float] = {}
    for t in txns:
        cat = session.get(Category, t.category_id) if t.category_id else None
        label = cat.name if cat else "Uncategorised"
        cat_totals[label] = round(cat_totals.get(label, 0) + t.amount, 2)
    deductions_by_category = [{"name": k, "amount": v} for k, v in
                               sorted(cat_totals.items(), key=lambda x: -x[1])]

    return {
        "fy_label":        f"{fy-1}–{fy}",
        "fy":              fy,
        "period_start":    str(start),
        "period_end":      str(end),
        # Employment
        "gross_income":    gross_income,
        "tax_withheld":    tax_withheld,
        "super_total":     super_total,
        "payslip_count":   payslip_count,
        # Deductions
        "deductions_total":         deductions_total,
        "wfh_deduction":            wfh_deduction,
        "total_deductions":         total_deductions,
        "deductions_by_category":   deductions_by_category,
        "receipts_attached":        receipts_ok,
        "txn_count":                len(txns),
        "missing_receipts_over300": len(missing_receipt_high),
        # Investment income
        "div_cash":        div_cash,
        "div_franking":    div_franking,
        "div_grossed_up":  div_grossed,
        "div_count":       len(dividends),
        # CGT
        "net_cgt":                  net_cgt,
        "cgt_discount":             cgt_discount_eligible,
        "net_cgt_after_discount":   net_cgt_after_discount,
        "disposals_count":          len(disposals),
        # Totals
        "total_income":     round(gross_income + div_grossed + max(0, net_cgt_after_discount), 2),
        "estimated_taxable":round(gross_income + div_grossed + max(0, net_cgt_after_discount) - total_deductions, 2),
    }


@router.patch("/wfh")
def update_wfh_days(
    days: int,
    current_user: User = Depends(get_current_user),
    session: Session = Depends(get_session),
):
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
def tax_estimate(
    fy: int,
    current_user: User = Depends(get_current_user),
    session: Session = Depends(get_session),
):
    """
    Project annual income tax using payslip data.
    fy = year ending, e.g. 2026 = Jul 2025 – Jun 2026.
    Uses 2024-25 Stage 3 revised ATO tax brackets.
    """
    fy_start = date(fy - 1, 7, 1)
    fy_end = date(fy, 6, 30)

    payslips = session.exec(
        select(Payslip).where(
            Payslip.user_id == current_user.id,
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

    # Determine distinct employers in this FY's payslips
    employers = {(p.employer or "").strip().lower() for p in payslips}
    multi_employer = len(employers) > 1

    if not multi_employer and latest.ytd_gross_cents and latest.ytd_gross_cents > 0:
        # Single employer with YTD on payslip — most accurate
        ytd_gross = latest.ytd_gross_cents / 100
        ytd_tax_withheld = (latest.ytd_tax_cents or 0) / 100
    else:
        # Multiple employers (or no YTD field): sum individual payslip amounts
        ytd_gross = sum(p.gross_pay_cents for p in payslips) / 100
        ytd_tax_withheld = sum(p.tax_withheld_cents for p in payslips) / 100

    projected_annual_gross = round(ytd_gross / weeks_elapsed * 52, 2)
    projected_annual_tax_withheld = round(ytd_tax_withheld / weeks_elapsed * 52, 2)

    # Transaction deductions (tax_deductible=True)
    txn_deductions = float(session.exec(
        select(func.coalesce(func.sum(Transaction.amount), 0)).where(
            Transaction.user_id == current_user.id,
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
        "multi_employer": multi_employer,
        "employer_count": len(employers),
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
