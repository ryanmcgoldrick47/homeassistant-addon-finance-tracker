from __future__ import annotations

import asyncio
import json as _json
import os
import re as _re
import time as _time
from contextlib import asynccontextmanager
from fastapi import FastAPI, Request
from fastapi.responses import FileResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
from fastapi.middleware.cors import CORSMiddleware
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.responses import Response as _Response
from sqlmodel import Session, select

from database import create_db, engine, Setting
from routers import transactions, import_csv, categories, budgets, bills, tax, ai, gmail, insights, payslips, notify, crypto, investments, networth, profile, cgt, dividends, goals, score, chat, newsletter, basiq, auth
import routers.super_tracker as super_tracker
import routers.stake_sync as stake_sync
import routers.receipts as receipts
import routers.merchants as merchants
import routers.reports as reports
import routers.data_export as data_export
import routers.market_pulse as market_pulse
import routers.loans as loans
import routers.advisor as advisor
import routers.trips as trips


# ---------------------------------------------------------------------------
# Ingress path prefix middleware
# HA Ingress proxies to the add-on at a path like /api/hassio_ingress/<token>/
# The X-Ingress-Path header tells us the base path so we can rewrite it.
# ---------------------------------------------------------------------------
class IngressMiddleware(BaseHTTPMiddleware):
    async def dispatch(self, request: Request, call_next):
        ingress_path = request.headers.get("X-Ingress-Path", "")
        if ingress_path:
            # Strip the ingress prefix from the path for internal routing
            scope = request.scope
            path = scope["path"]
            if path.startswith(ingress_path):
                scope["path"] = path[len(ingress_path):] or "/"
            # Store for use in URL generation
            scope["root_path"] = ingress_path
        return await call_next(request)


def _apply_demo_mask(path: str, data):
    from demo_mode import (mask_transactions_list, mask_holding,
                           mask_crypto_holding, mask_payslip, mask_amount, mask_description,
                           mask_ticker)

    # /api/transactions/review-queue
    if path.endswith("/review-queue"):
        if isinstance(data, dict) and "items" in data:
            data["items"] = mask_transactions_list(data["items"])
        return data

    # /api/transactions/merchants
    if path.endswith("/merchants"):
        if isinstance(data, list):
            for m in data:
                seed = str(m.get("name", ""))
                m["name"] = mask_description(seed, seed)
                m["total"] = mask_amount(m.get("total") or 50, f"mer_total_{seed}")
                m["avg"] = mask_amount(m.get("avg") or 20, f"mer_avg_{seed}")
        return data

    # /api/transactions list (exact path)
    if _re.fullmatch(r"/api/transactions/?", path):
        if isinstance(data, dict) and "items" in data:
            data["items"] = mask_transactions_list(data["items"])
        return data

    # /api/transactions/{id} individual transaction
    if _re.match(r"/api/transactions/\d+", path):
        if isinstance(data, dict) and "description" in data:
            return mask_transaction(data)
        return data

    # /api/dashboard (main stats only, not /trend or /net-worth)
    if _re.fullmatch(r"/api/dashboard/?", path):
        for field in ("month_spend", "month_income", "prev_month_spend", "prev_month_income"):
            if field in data:
                data[field] = mask_amount(data[field] or 100, f"dash_{field}")
        if "net" in data:
            raw = data["net"]
            data["net"] = mask_amount(abs(raw) or 50, "net") * (-1 if raw < 0 else 1)
        if "top_transactions" in data:
            data["top_transactions"] = mask_transactions_list(data["top_transactions"])
        for k in list(data.get("by_category", {}).keys()):
            data["by_category"][k] = mask_amount(data["by_category"][k] or 50, f"cat_{k}")
        for k in list(data.get("income_by_category", {}).keys()):
            data["income_by_category"][k] = mask_amount(data["income_by_category"][k] or 50, f"inc_{k}")
        return data

    # /api/dashboard/trend
    if path.endswith("/trend"):
        if isinstance(data, list):
            for m in data:
                lbl = m.get("label", "x")
                m["spend"] = mask_amount(m.get("spend") or 100, f"trend_spend_{lbl}")
                m["income"] = mask_amount(m.get("income") or 100, f"trend_inc_{lbl}")
        return data

    # /api/investments (holdings list)
    if _re.fullmatch(r"/api/investments/?", path):
        if isinstance(data, list):
            return [mask_holding(h) for h in data]
        return data

    # /api/investments/summary
    if path.endswith("/summary") and "/investments" in path:
        if isinstance(data, dict):
            for f in ("total_value_aud", "total_cost_basis_aud", "total_gain_aud"):
                if data.get(f) is not None:
                    data[f] = mask_amount(data[f] or 100, f"inv_sum_{f}")
        return data

    # /api/investments/benchmark
    if path.endswith("/benchmark") and "/investments" in path:
        if isinstance(data, dict):
            for f in ("portfolio_value_aud", "portfolio_cost_aud"):
                if data.get(f) is not None:
                    data[f] = mask_amount(data[f] or 100, f"inv_bench_{f}")
        return data

    # /api/crypto (holdings)
    if _re.fullmatch(r"/api/crypto/?", path):
        if isinstance(data, dict) and "holdings" in data:
            data["holdings"] = [mask_crypto_holding(c) for c in data["holdings"]]
            if "total_aud" in data:
                data["total_aud"] = mask_amount(data["total_aud"] or 1000, "crypto_total")
            if "total_gain_aud" in data:
                raw = data["total_gain_aud"]
                data["total_gain_aud"] = mask_amount(abs(raw) or 100, "crypto_gain") * (-1 if raw < 0 else 1)
        return data

    # /api/payslips
    if _re.match(r"/api/payslips", path):
        if isinstance(data, list):
            return [mask_payslip(p) for p in data]
        if isinstance(data, dict) and "items" in data:
            data["items"] = [mask_payslip(p) for p in data["items"]]
        if isinstance(data, dict) and "gross_pay_cents" in data:
            return mask_payslip(data)
        return data

    # /api/networth (list, latest, chart, forecast)
    if "/networth" in path:
        NW_SNAP_FIELDS = (
            "net_worth", "total_assets", "total_liabilities",
            "cash_savings", "super_balance", "property_value",
            "shares_value", "crypto_value", "other_assets",
            "mortgage_balance", "car_loan", "credit_card", "hecs_debt", "other_liabilities",
        )
        def _mask_snap(snap: dict):
            sid = snap.get("id", snap.get("snapshot_date", "x"))
            for f in NW_SNAP_FIELDS:
                if snap.get(f) is not None:
                    snap[f] = mask_amount(snap[f] or 1000, f"nw_{f}_{sid}")

        if path.endswith("/chart"):
            if isinstance(data, list):
                for item in data:
                    lbl = item.get("date", "x")
                    for f in ("net_worth", "total_assets", "total_liabilities"):
                        if item.get(f) is not None:
                            item[f] = mask_amount(item[f] or 1000, f"nw_ch_{f}_{lbl}")
            return data

        if path.endswith("/forecast"):
            if isinstance(data, dict):
                for f in ("current_nw", "monthly_savings"):
                    if data.get(f) is not None:
                        raw = data[f]
                        data[f] = mask_amount(abs(raw) or 100, f"nw_fc_{f}") * (1 if raw >= 0 else -1)
                for proj in data.get("projections", []):
                    years = proj.get("years", "x")
                    for f in ("projected_nw", "from_growth", "from_savings"):
                        if proj.get(f) is not None:
                            raw = proj[f]
                            proj[f] = mask_amount(abs(raw) or 1000, f"nw_fc_{f}_{years}") * (1 if raw >= 0 else -1)
            return data

        if path.endswith("/latest"):
            if isinstance(data, dict):
                _mask_snap(data)
            return data

        # /api/networth (list)
        if isinstance(data, list):
            for snap in data:
                _mask_snap(snap)
        return data

    # /api/super (summary dict, snapshots list, chart list)
    if "/super" in path:
        FAKE_FUNDS = ["Australian Retirement Trust", "Hostplus", "REST Industry Super",
                      "UniSuper", "AwareSuper", "CareSuper", "HESTA", "Sunsuper"]
        if isinstance(data, dict) and "latest_balance" in data:
            # /api/super/summary
            from demo_mode import _seed
            data["fund_name"] = _seed("super_fund").choice(FAKE_FUNDS) if data.get("fund_name") else None
            for f in ("latest_balance", "median_balance", "asfa_target", "projected_at_67",
                      "ytd_employer_contributions", "ytd_voluntary_contributions", "monthly_savings"):
                if data.get(f) is not None:
                    data[f] = mask_amount(float(data[f]) or 1000, f"super_{f}")
            for c in data.get("contributions", []):
                cid = str(c.get("id", ""))
                if c.get("amount_aud") is not None:
                    c["amount_aud"] = mask_amount(c["amount_aud"] or 100, f"super_contrib_{cid}")
                c["source"] = None
                c["notes"] = None
        elif isinstance(data, list):
            for snap in data:
                if snap.get("balance_aud") is not None:
                    snap["balance_aud"] = mask_amount(snap["balance_aud"] or 1000, f"super_{snap.get('id', 0)}")
                snap["fund_name"] = "Demo Fund"
        return data

    # /api/bills
    if _re.match(r"/api/bills", path):
        FAKE_BILL_NAMES = [
            "Electricity", "Gas", "Water", "Internet", "Mobile Plan",
            "Health Insurance", "Car Insurance", "Home Insurance",
            "Gym Membership", "Streaming Service", "Cloud Storage",
            "Council Rates", "Strata Levy", "Body Corporate",
        ]
        items = data if isinstance(data, list) else []
        for b in items:
            sid = str(b.get("id", b.get("name", "")))
            import random, hashlib
            r = random.Random(int(hashlib.md5(f"bill_{sid}".encode()).hexdigest(), 16))
            b["name"] = r.choice(FAKE_BILL_NAMES)
            for f in ("amount_cents",):
                if b.get(f) is not None:
                    b[f] = int(mask_amount(b[f] / 100 or 50, f"bill_{f}_{sid}") * 100)
        return data

    # /api/goals/savings-rate
    if path.endswith("/savings-rate"):
        from demo_mode import _seed
        if isinstance(data, dict):
            for mo in data.get("months", []):
                lbl = mo.get("label", "x")
                for f in ("income", "spend", "net"):
                    if mo.get(f) is not None:
                        raw = mo[f]
                        mo[f] = mask_amount(abs(raw) or 100, f"sr_{f}_{lbl}") * (1 if raw >= 0 else -1)
                mo["actual_pct"] = round(_seed(f"sr_pct_{lbl}").uniform(5, 35), 1)
            for f in ("avg_pct", "target_pct"):
                if data.get(f) is not None:
                    data[f] = round(_seed(f"sr_{f}").uniform(10, 30), 1)
        return data

    # /api/goals
    if _re.match(r"/api/goals", path):
        FAKE_GOAL_NAMES = [
            "Holiday Fund", "Emergency Fund", "New Car", "Home Deposit",
            "Wedding Fund", "Education Fund", "Home Renovation", "Investment Buffer",
        ]
        items = data if isinstance(data, list) else []
        for g in items:
            sid = str(g.get("id", g.get("name", "")))
            import random, hashlib
            r = random.Random(int(hashlib.md5(f"goal_{sid}".encode()).hexdigest(), 16))
            g["name"] = r.choice(FAKE_GOAL_NAMES)
            for f in ("target_cents", "current_cents"):
                if g.get(f) is not None:
                    g[f] = int(mask_amount(g[f] / 100 or 500, f"goal_{f}_{sid}") * 100)
            if "target_aud" in g:
                g["target_aud"] = round(g["target_cents"] / 100, 2)
            if "current_aud" in g:
                g["current_aud"] = round(g["current_cents"] / 100, 2)
            if "remaining_aud" in g:
                g["remaining_aud"] = round(max(0, g["target_cents"] - g["current_cents"]) / 100, 2)
        return data

    # /api/accounts (account names)
    if _re.match(r"/api/accounts", path):
        FAKE_ACCOUNT_NAMES = ["Everyday Account", "Savings Account", "Offset Account", "Credit Card", "Investment Account"]
        items = data if isinstance(data, list) else []
        for a in items:
            sid = str(a.get("id", ""))
            import random, hashlib
            r = random.Random(int(hashlib.md5(f"acc_{sid}".encode()).hexdigest(), 16))
            a["name"] = r.choice(FAKE_ACCOUNT_NAMES)
            if a.get("balance") is not None:
                a["balance"] = mask_amount(a["balance"] or 1000, f"acc_bal_{sid}")
        return data

    # /api/tax/summary
    if path.endswith("/summary") and "/api/tax" in path:
        if isinstance(data, dict):
            for f in ("total_income", "total_deductible", "wfh_deduction", "total_deductions",
                      "taxable_income_estimate", "gst_collected_estimate",
                      "gst_on_expenses_estimate", "net_gst_payable"):
                if data.get(f) is not None:
                    raw = data[f]
                    data[f] = mask_amount(abs(raw) or 100, f"tax_sum_{f}") * (1 if raw >= 0 else -1)
            for k in list(data.get("by_tax_category", {}).keys()):
                data["by_tax_category"][k] = mask_amount(data["by_tax_category"][k] or 50, f"tax_cat_{k}")
            if isinstance(data.get("deductible_transactions"), list):
                for t in data["deductible_transactions"]:
                    tid = str(t.get("description", ""))
                    t["description"] = mask_description(tid, tid)
                    t["amount"] = mask_amount(t.get("amount") or 50, f"tax_txn_{tid}")
                    if t.get("notes"):
                        t["notes"] = "Note hidden in demo mode"
        return data

    # /api/tax/estimate
    if path.endswith("/estimate") and "/api/tax" in path:
        if isinstance(data, dict) and data.get("has_data"):
            for f in ("ytd_gross", "ytd_tax_withheld", "projected_annual_gross",
                      "projected_annual_tax_withheld", "total_deductions", "wfh_deduction",
                      "taxable_income", "ato_tax", "lito", "medicare_levy",
                      "help_repayment", "net_tax"):
                if data.get(f) is not None:
                    data[f] = mask_amount(data[f] or 100, f"tax_est_{f}")
            if data.get("estimated_refund") is not None:
                raw = data["estimated_refund"]
                data["estimated_refund"] = mask_amount(abs(raw) or 100, "tax_est_refund") * (1 if raw >= 0 else -1)
        return data

    # /api/budgets (list, vs-spend, zbb-summary)
    if _re.match(r"/api/budgets", path):
        if path.endswith("/vs-spend"):
            if isinstance(data, list):
                for b in data:
                    sid = str(b.get("id", b.get("category_id", "")))
                    for f in ("budget", "spend", "remaining"):
                        if b.get(f) is not None:
                            raw = b[f]
                            b[f] = mask_amount(abs(raw) or 50, f"bvs_{f}_{sid}") * (1 if raw >= 0 else -1)
            return data
        if path.endswith("/zbb-summary"):
            if isinstance(data, dict):
                for f in ("income", "allocated", "unallocated"):
                    if data.get(f) is not None:
                        raw = data[f]
                        data[f] = mask_amount(abs(raw) or 100, f"zbb_{f}") * (1 if raw >= 0 else -1)
            return data
        # /api/budgets list
        if isinstance(data, list):
            for b in data:
                sid = str(b.get("id", ""))
                if b.get("amount_cents") is not None:
                    b["amount_cents"] = int(mask_amount(b["amount_cents"] / 100 or 50, f"bud_{sid}") * 100)
        return data

    # /api/cgt/lots
    if path.endswith("/cgt/lots"):
        if isinstance(data, list):
            for lot in data:
                sid = str(lot.get("id", lot.get("ticker", "")))
                lot["ticker"] = mask_ticker(lot.get("ticker", "XXX"))
                for f in ("qty", "disposed_qty", "remaining_qty", "cost_per_unit_aud", "brokerage_aud"):
                    if lot.get(f) is not None:
                        lot[f] = round(mask_amount(float(lot[f]) or 10, f"cgt_lot_{f}_{sid}"), 4)
                if lot.get("notes"):
                    lot["notes"] = "Note hidden in demo mode"
        return data

    # /api/cgt/disposals
    if path.endswith("/cgt/disposals"):
        if isinstance(data, list):
            for d in data:
                sid = str(d.get("id", ""))
                d["ticker"] = mask_ticker(d.get("ticker", "XXX"))
                for f in ("qty", "proceeds_per_unit_aud", "brokerage_aud"):
                    if d.get(f) is not None:
                        d[f] = round(mask_amount(float(d[f]) or 10, f"cgt_dis_{f}_{sid}"), 4)
                if d.get("gain_aud") is not None:
                    raw = d["gain_aud"]
                    d["gain_aud"] = round(mask_amount(abs(raw) or 100, f"cgt_dis_gain_{sid}") * (1 if raw >= 0 else -1), 2)
                if d.get("notes"):
                    d["notes"] = "Note hidden in demo mode"
        return data

    # /api/cgt/summary
    if path.endswith("/cgt/summary"):
        if isinstance(data, dict):
            for f in ("short_term_gains", "long_term_gains", "capital_losses",
                      "discount_amount", "gross_after_discount", "net_capital_gain",
                      "share_gains", "crypto_gains"):
                if data.get(f) is not None:
                    raw = data[f]
                    data[f] = round(mask_amount(abs(raw) or 100, f"cgt_sum_{f}") * (1 if raw >= 0 else -1), 2)
            if isinstance(data.get("rows"), list):
                for row in data["rows"]:
                    rid = str(row.get("id", ""))
                    row["ticker"] = mask_ticker(row.get("ticker", "XXX"))
                    if row.get("gain_aud") is not None:
                        raw = row["gain_aud"]
                        row["gain_aud"] = round(mask_amount(abs(raw) or 100, f"cgt_row_gain_{rid}") * (1 if raw >= 0 else -1), 2)
                    if row.get("qty") is not None:
                        row["qty"] = round(mask_amount(float(row["qty"]) or 10, f"cgt_row_qty_{rid}"), 4)
        return data

    # /api/dividends list and summary
    if _re.match(r"/api/dividends", path):
        if path.endswith("/summary"):
            if isinstance(data, dict):
                for f in ("total_cash", "total_franking", "total_grossed_up"):
                    if data.get(f) is not None:
                        data[f] = mask_amount(data[f] or 50, f"div_sum_{f}")
                for ticker_key, v in list(data.get("by_ticker", {}).items()):
                    fake_t = mask_ticker(ticker_key)
                    for f in ("cash", "franking", "grossed_up"):
                        if v.get(f) is not None:
                            v[f] = mask_amount(v[f] or 10, f"div_tick_{f}_{ticker_key}")
                    v["ticker"] = fake_t
                    if fake_t != ticker_key:
                        data["by_ticker"][fake_t] = data["by_ticker"].pop(ticker_key)
            return data
        # list
        if isinstance(data, list):
            for d in data:
                did = str(d.get("id", ""))
                d["ticker"] = mask_ticker(d.get("ticker", "XXX"))
                for f in ("amount_aud", "franking_credits_aud", "grossed_up_aud"):
                    if d.get(f) is not None:
                        d[f] = mask_amount(d[f] or 10, f"div_{f}_{did}")
                if d.get("notes"):
                    d["notes"] = "Note hidden in demo mode"
        return data

    # /api/score (includes income/spend/net/budget_detail)
    if path.endswith("/api/score") or _re.fullmatch(r"/api/score/?", path):
        if isinstance(data, dict):
            for f in ("income", "spend", "net"):
                if data.get(f) is not None:
                    raw = data[f]
                    data[f] = mask_amount(abs(raw) or 100, f"score_{f}") * (1 if raw >= 0 else -1)
            if isinstance(data.get("budget_detail"), list):
                for b in data["budget_detail"]:
                    cid = str(b.get("category_id", ""))
                    for f in ("spend", "budget"):
                        if b.get(f) is not None:
                            b[f] = mask_amount(b[f] or 50, f"score_bd_{f}_{cid}")
        return data

    # /api/challenges (progress + target values)
    if _re.match(r"/api/challenges", path):
        if isinstance(data, list):
            for c in data:
                cid = str(c.get("id", ""))
                if c.get("target_value") is not None:
                    c["target_value"] = mask_amount(c["target_value"] or 100, f"chal_target_{cid}")
                if c.get("progress") is not None:
                    raw = c["progress"]
                    c["progress"] = mask_amount(abs(raw) or 50, f"chal_prog_{cid}") * (1 if raw >= 0 else -1)
        return data

    # /api/insights — suppress cached AI text (contains real spending patterns)
    if _re.match(r"/api/insights", path):
        if isinstance(data, dict):
            data["data"] = None
            data["cached"] = False
            data["generated_at"] = None
        return data

    # /api/newsletter — suppress financial summary (contains real name + spend data)
    if "/newsletter" in path:
        return {"masked": True, "message": "Newsletter preview hidden in demo mode."}

    return data


class DemoModeMiddleware(BaseHTTPMiddleware):
    _enabled: bool = False
    _cache_ts: float = 0.0

    def _is_demo(self) -> bool:
        now = _time.monotonic()
        if now - DemoModeMiddleware._cache_ts > 2:
            try:
                with Session(engine) as session:
                    s = session.get(Setting, "demo_mode")
                    DemoModeMiddleware._enabled = bool(s and s.value == "1")
            except Exception:
                pass
            DemoModeMiddleware._cache_ts = now
        return DemoModeMiddleware._enabled

    async def dispatch(self, request: Request, call_next):
        response = await call_next(request)
        path = request.url.path

        # Only intercept JSON GET responses under /api/ (skip auth endpoints)
        if request.method != "GET" or not path.startswith("/api/"):
            return response
        if path.startswith("/api/auth/"):
            return response
        if "application/json" not in response.headers.get("content-type", ""):
            return response
        if not self._is_demo():
            return response

        # Buffer body
        body = b""
        async for chunk in response.body_iterator:
            body += chunk if isinstance(chunk, bytes) else chunk.encode()

        try:
            data = _json.loads(body)
        except Exception:
            return _Response(content=body, status_code=response.status_code,
                             media_type=response.headers.get("content-type"))

        data = _apply_demo_mask(path, data)
        return _Response(
            content=_json.dumps(data),
            status_code=response.status_code,
            media_type="application/json",
        )


async def _folder_watch_loop():
    """Background task: poll import_watch/ every 30 seconds."""
    from routers.import_csv import folder_watch_tick
    while True:
        try:
            await folder_watch_tick()
        except Exception:
            pass
        await asyncio.sleep(30)


async def _payslip_watch_loop():
    """Background task: poll payslip_watch/ every 60 seconds."""
    from routers.payslips import payslip_watch_tick
    while True:
        try:
            await payslip_watch_tick()
        except Exception:
            pass
        await asyncio.sleep(60)


async def _send_newsletter_now(session: Session):
    """Send newsletter from a background task (no Request object needed)."""
    import smtplib
    import ssl
    from email.mime.multipart import MIMEMultipart
    from email.mime.text import MIMEText
    from routers.newsletter import _gather, _generate_insights, _build_html
    from deps import get_setting

    gmail_address    = get_setting(session, "gmail_address", "")
    gmail_password   = get_setting(session, "gmail_app_password", "")
    newsletter_email = get_setting(session, "newsletter_email", "") or gmail_address
    app_url          = get_setting(session, "app_url", "") or "http://homeassistant.local:8123/api/finance_proxy/"
    api_key          = get_setting(session, "anthropic_api_key", "") or os.environ.get("ANTHROPIC_API_KEY", "")

    if not gmail_address or not gmail_password or not newsletter_email:
        return

    data     = _gather(session)
    insights = await _generate_insights(data, api_key) if api_key else ""
    html     = _build_html(data, app_url, insights)

    subject = f"Finance Weekly: {data['week_label']} — Score {data['score']['score']}/100"
    if (data["action_items"]["uncategorised"] > 0 or
            data["action_items"]["flagged"] > 0 or
            data["action_items"]["overdue_bills"]):
        subject += " ⚠"

    msg = MIMEMultipart("alternative")
    msg["Subject"] = subject
    msg["From"]    = f"Finance Tracker <{gmail_address}>"
    msg["To"]      = newsletter_email
    msg.attach(MIMEText(html, "html"))

    def _smtp():
        ctx = ssl.create_default_context()
        with smtplib.SMTP_SSL("smtp.gmail.com", 465, context=ctx) as smtp:
            smtp.login(gmail_address, gmail_password)
            smtp.sendmail(gmail_address, newsletter_email, msg.as_string())

    await asyncio.to_thread(_smtp)


async def _newsletter_loop():
    """Background task: send newsletter on the configured schedule."""
    while True:
        await asyncio.sleep(60)
        try:
            with Session(engine) as session:
                from deps import get_setting, set_setting
                if get_setting(session, "newsletter_schedule_enabled") != "1":
                    continue
                try:
                    from zoneinfo import ZoneInfo
                    tz = ZoneInfo("Australia/Sydney")
                except Exception:
                    tz = None
                from datetime import datetime as _dt
                now          = _dt.now(tz) if tz else _dt.now()
                day_setting  = get_setting(session, "newsletter_day", "6")
                time_setting = get_setting(session, "newsletter_time", "08:00")
                last_sent    = get_setting(session, "newsletter_last_sent", "")
                today_str    = now.strftime("%Y-%m-%d")
                now_time     = now.strftime("%H:%M")
                day_match    = day_setting == "daily" or str(now.weekday()) == day_setting
                if day_match and now_time == time_setting and last_sent != today_str:
                    await _send_newsletter_now(session)
                    set_setting(session, "newsletter_last_sent", today_str)
        except Exception:
            pass


async def _gmail_scan_loop():
    """Background task: daily Gmail scan for payslips and expense receipts."""
    await asyncio.sleep(120)  # wait 2 min after startup before first check
    while True:
        try:
            with Session(engine) as session:
                from deps import get_setting
                enabled = get_setting(session, "gmail_auto_scan_enabled", "1")
                if enabled != "0":
                    last_scan = get_setting(session, "gmail_last_scan", "")
                    run_now = True
                    if last_scan:
                        try:
                            from datetime import datetime as _dt
                            last_dt = _dt.fromisoformat(last_scan)
                            if (_dt.now() - last_dt).total_seconds() < 23 * 3600:
                                run_now = False
                        except ValueError:
                            pass
                    if run_now:
                        payslip_label = get_setting(session, "gmail_payslip_label", "")
                        expense_label = get_setting(session, "gmail_expense_label", "")
                        if payslip_label or expense_label:
                            from routers.gmail import _run_gmail_scan
                            await _run_gmail_scan(session, 1)
        except Exception:
            pass
        await asyncio.sleep(3600)  # check every hour


async def _price_refresh_loop():
    """Background task: auto-refresh investment and crypto prices."""
    await asyncio.sleep(300)  # wait 5 min after startup
    while True:
        try:
            with Session(engine) as session:
                from deps import get_setting, set_setting
                if get_setting(session, "price_refresh_enabled") != "1":
                    await asyncio.sleep(3600)
                    continue
                interval_hours = int(get_setting(session, "price_refresh_interval", "8") or "8")
                last_refresh   = get_setting(session, "price_last_refreshed", "")
                from datetime import datetime as _dt, timezone as _tz
                now     = _dt.now(_tz.utc)
                run_now = True
                if last_refresh:
                    try:
                        last_dt = _dt.fromisoformat(last_refresh)
                        if (now - last_dt).total_seconds() < interval_hours * 3600:
                            run_now = False
                    except ValueError:
                        pass
                if run_now:
                    from routers.investments import refresh_prices
                    from routers.crypto import sync_binance
                    try:
                        await refresh_prices(session=session)
                    except Exception:
                        pass
                    try:
                        if get_setting(session, "binance_api_key", ""):
                            await sync_binance(session=session)
                    except Exception:
                        pass
                    set_setting(session, "price_last_refreshed", now.isoformat())
        except Exception:
            pass
        await asyncio.sleep(3600)


@asynccontextmanager
async def lifespan(app: FastAPI):
    create_db()
    # Pull API key from env into DB if not set
    api_key = os.environ.get("ANTHROPIC_API_KEY", "")
    if api_key:
        with Session(engine) as session:
            s = session.get(Setting, "anthropic_api_key")
            if s and not s.value:
                s.value = api_key
                session.add(s)
                session.commit()
    task1 = asyncio.create_task(_folder_watch_loop())
    task2 = asyncio.create_task(_newsletter_loop())
    task3 = asyncio.create_task(_price_refresh_loop())
    task4 = asyncio.create_task(_payslip_watch_loop())
    task5 = asyncio.create_task(_gmail_scan_loop())
    yield
    task1.cancel()
    task2.cancel()
    task3.cancel()
    task4.cancel()
    task5.cancel()


app = FastAPI(title="Finance Tracker", lifespan=lifespan)

app.add_middleware(DemoModeMiddleware)
app.add_middleware(IngressMiddleware)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

# Routers
app.include_router(transactions.router)
app.include_router(import_csv.router)
app.include_router(categories.router)
app.include_router(budgets.router)
app.include_router(bills.router)
app.include_router(tax.router)
app.include_router(ai.router)
app.include_router(gmail.router)
app.include_router(insights.router)
app.include_router(payslips.router)
app.include_router(notify.router)
app.include_router(crypto.router)
app.include_router(investments.router)
app.include_router(networth.router)
app.include_router(profile.router)
app.include_router(super_tracker.router)
app.include_router(cgt.router)
app.include_router(dividends.router)
app.include_router(goals.router)
app.include_router(score.router)
app.include_router(chat.router)
app.include_router(newsletter.router)
app.include_router(stake_sync.router)
app.include_router(receipts.router)
app.include_router(merchants.router)
app.include_router(basiq.router)
app.include_router(auth.router)
app.include_router(reports.router)
app.include_router(data_export.router)
app.include_router(market_pulse.router)
app.include_router(loans.router)
app.include_router(advisor.router)
app.include_router(trips.router)

# MCP server (Finance Tracker tools for Claude Code on desktop)
try:
    from routers.mcp_server import build_mcp_app
    _mcp = build_mcp_app()
    if _mcp is not None:
        app.mount("/mcp", _mcp.http_app())
except Exception as _mcp_err:
    import logging as _logging
    _logging.getLogger("finance_tracker").warning(f"MCP server not loaded: {_mcp_err}")


# Settings endpoints
@app.get("/api/settings")
def get_settings():
    with Session(engine) as session:
        settings = session.exec(select(Setting)).all()
        result = {s.key: s.value for s in settings}
        # Never expose secrets
        for secret_key in ("anthropic_api_key", "gemini_api_key", "gmail_app_password", "ha_token",
                            "binance_api_key", "binance_api_secret", "stake_session_token"):
            if secret_key in result:
                result[secret_key] = "***" if result[secret_key] else ""
        return result


@app.post("/api/settings")
async def update_settings(request: Request):
    data = await request.json()
    with Session(engine) as session:
        for key, value in data.items():
            if key in ("anthropic_api_key", "gemini_api_key", "gmail_app_password", "ha_token",
                       "binance_api_key", "binance_api_secret", "stake_session_token") and value == "***":
                continue  # don't overwrite with masked value
            s = session.get(Setting, key)
            if s:
                s.value = str(value)
            else:
                s = Setting(key=key, value=str(value))
            session.add(s)
        session.commit()
    return {"ok": True}


@app.get("/api/dashboard")
def dashboard(
    month: int = None,
    year: int = None,
    authorization: str = __import__("fastapi").Header(default=None, alias="authorization"),
):
    """Quick stats for the dashboard."""
    from datetime import date, timedelta
    from calendar import month_name, monthrange
    from sqlmodel import func
    from database import Transaction, Bill, Category
    from deps import get_current_user as _get_user
    from fastapi import Header as _Header

    # Resolve user_id from auth token (default 1 for backward compat)
    user_id = 1
    if authorization and authorization.startswith("Bearer "):
        try:
            with Session(engine) as _s:
                from database import UserSession, User as _User
                tok = authorization[7:]
                sess = _s.get(UserSession, tok)
                if sess:
                    user_id = sess.user_id
        except Exception:
            pass

    today = date.today()
    this_month = month or today.month
    this_year = year or today.year

    # Previous month
    if this_month == 1:
        prev_month, prev_year = 12, this_year - 1
    else:
        prev_month, prev_year = this_month - 1, this_year

    # Days elapsed / in month (for pace projection)
    days_in_month = monthrange(this_year, this_month)[1]
    if this_year == today.year and this_month == today.month:
        days_elapsed = today.day
    else:
        days_elapsed = days_in_month

    with Session(engine) as session:
        # IDs of categories excluded from personal spend (e.g. Transfers)
        _excl_ids = [c.id for c in session.exec(
            select(Category).where(Category.exclude_from_spend == True)
        ).all()]

        def _sum(is_credit, m, y):
            filters = [
                Transaction.is_credit == is_credit,
                Transaction.user_id == user_id,
                func.strftime("%m", Transaction.date) == f"{m:02d}",
                func.strftime("%Y", Transaction.date) == str(y),
            ]
            if not is_credit:
                filters.append(Transaction.is_reimbursable == False)
                if _excl_ids:
                    filters.append(Transaction.category_id.notin_(_excl_ids))
            return float(session.exec(
                select(func.coalesce(func.sum(Transaction.amount), 0)).where(*filters)
            ).one())

        month_spend  = _sum(False, this_month, this_year)
        month_income = _sum(True,  this_month, this_year)
        prev_spend   = _sum(False, prev_month, prev_year)
        prev_income  = _sum(True,  prev_month, prev_year)

        # Flagged count
        flagged = session.exec(
            select(func.count()).where(
                Transaction.is_flagged == True,
                Transaction.is_reviewed == False,
                Transaction.user_id == user_id,
            )
        ).one()

        # Uncategorised count
        uncat = session.exec(select(Category).where(Category.name == "Uncategorised")).first()
        uncategorised = 0
        if uncat:
            uncategorised = session.exec(
                select(func.count()).where(
                    Transaction.category_id == uncat.id,
                    Transaction.user_id == user_id,
                )
            ).one()

        # Bills due in 14 days (count + list)
        bill_cutoff = today + timedelta(days=14)
        bills_soon = session.exec(
            select(func.count()).where(
                Bill.is_active == True, Bill.next_due != None,
                Bill.next_due <= bill_cutoff,
                Bill.user_id == user_id,
            )
        ).one()
        upcoming_bill_rows = session.exec(
            select(Bill).where(
                Bill.is_active == True, Bill.next_due != None,
                Bill.next_due <= bill_cutoff,
                Bill.user_id == user_id,
            ).order_by(Bill.next_due)
        ).all()
        upcoming_bills = [
            {"name": b.name, "amount": round(b.amount_cents / 100, 2), "next_due": str(b.next_due)}
            for b in upcoming_bill_rows
        ]

        # Total transactions for selected month
        total_txns = session.exec(
            select(func.count(Transaction.id)).where(
                Transaction.user_id == user_id,
                func.strftime("%m", Transaction.date) == f"{this_month:02d}",
                func.strftime("%Y", Transaction.date) == str(this_year),
            )
        ).one()

        # Spend & income by category
        all_txns = session.exec(
            select(Transaction).where(
                Transaction.user_id == user_id,
                func.strftime("%m", Transaction.date) == f"{this_month:02d}",
                func.strftime("%Y", Transaction.date) == str(this_year),
            )
        ).all()
        by_category: dict[str, float] = {}
        income_by_category: dict[str, float] = {}
        for t in all_txns:
            cat = session.get(Category, t.category_id) if t.category_id else None
            cat_name = cat.name if cat else "Uncategorised"
            if t.is_credit:
                income_by_category[cat_name] = round(income_by_category.get(cat_name, 0) + t.amount, 2)
            elif not t.is_reimbursable and (not cat or not cat.exclude_from_spend):
                by_category[cat_name] = round(by_category.get(cat_name, 0) + t.amount, 2)

        # Top 5 spend transactions this month
        _top_filters = [
            Transaction.is_credit == False,
            Transaction.is_reimbursable == False,
            Transaction.user_id == user_id,
            func.strftime("%m", Transaction.date) == f"{this_month:02d}",
            func.strftime("%Y", Transaction.date) == str(this_year),
        ]
        if _excl_ids:
            _top_filters.append(Transaction.category_id.notin_(_excl_ids))
        top_txn_rows = session.exec(
            select(Transaction).where(*_top_filters).order_by(Transaction.amount.desc()).limit(5)
        ).all()
        from database import MerchantEnrichment
        top_transactions = []
        for t in top_txn_rows:
            cat = session.get(Category, t.category_id) if t.category_id else None
            raw_key = (t.description or "")[:50].upper()
            enrich = session.get(MerchantEnrichment, raw_key)
            top_transactions.append({
                "id": t.id,
                "description": t.description,
                "amount": float(t.amount),
                "date": str(t.date),
                "category_name": cat.name if cat else "Uncategorised",
                "logo_domain": enrich.domain if enrich else None,
                "clean_name": enrich.clean_name if enrich else None,
            })

        # Income breakdown from payslips this month
        from database import Payslip
        payslips_this_month = session.exec(
            select(Payslip).where(
                Payslip.user_id == user_id,
                func.strftime("%m", Payslip.pay_date) == f"{this_month:02d}",
                func.strftime("%Y", Payslip.pay_date) == str(this_year),
            )
        ).all()
        payslip_gross = sum(p.gross_pay_cents for p in payslips_this_month) / 100
        payslip_tax = sum(p.tax_withheld_cents for p in payslips_this_month) / 100
        payslip_net = sum(p.net_pay_cents for p in payslips_this_month) / 100
        # Matched net pay credits (to subtract from "other" income)
        matched_txn_ids = {p.matched_txn_id for p in payslips_this_month if p.matched_txn_id}
        matched_net_sum = 0.0
        if matched_txn_ids:
            matched_txns = session.exec(
                select(Transaction).where(Transaction.id.in_(list(matched_txn_ids)))
            ).all()
            matched_net_sum = sum(t.amount for t in matched_txns)
        other_credits = max(round(month_income - matched_net_sum, 2), 0) if payslip_gross > 0 else month_income
        income_breakdown = {
            "payslip_gross": round(payslip_gross, 2),
            "payslip_tax_withheld": round(payslip_tax, 2),
            "payslip_net": round(payslip_net, 2),
            "other_credits": other_credits,
        }

    def pct_change(cur, prev):
        if prev == 0:
            return None
        return round((cur - prev) / prev * 100, 1)

    net = round(month_income - month_spend, 2)
    return {
        "month": f"{month_name[this_month]} {this_year}",
        "month_spend": round(month_spend, 2),
        "month_income": round(month_income, 2),
        "net": net,
        "prev_month_spend": round(prev_spend, 2),
        "prev_month_income": round(prev_income, 2),
        "spend_change_pct": pct_change(month_spend, prev_spend),
        "income_change_pct": pct_change(month_income, prev_income),
        "days_elapsed": days_elapsed,
        "days_in_month": days_in_month,
        "flagged_count": int(flagged),
        "uncategorised_count": int(uncategorised),
        "by_category": by_category,
        "income_by_category": income_by_category,
        "bills_due_soon": int(bills_soon),
        "upcoming_bills": upcoming_bills,
        "top_transactions": top_transactions,
        "total_transactions": int(total_txns),
        "income_breakdown": income_breakdown,
    }


@app.get("/api/dashboard/trend")
def dashboard_trend(months: int = 6, start: str = None, end: str = None, authorization: str = __import__("fastapi").Header(default=None, alias="authorization")):
    """Spend and income totals by month. Use months=N for last N months, or start/end as YYYY-MM."""
    from datetime import date
    from calendar import month_abbr
    from sqlmodel import func
    from database import Transaction

    user_id = 1
    if authorization and authorization.startswith("Bearer "):
        try:
            with Session(engine) as _s:
                from database import UserSession
                tok = authorization[7:]
                sess = _s.get(UserSession, tok)
                if sess:
                    user_id = sess.user_id
        except Exception:
            pass

    today = date.today()
    month_list = []

    if start and end:
        try:
            sy, sm = int(start[:4]), int(start[5:7])
            ey, em = int(end[:4]), int(end[5:7])
            y, m = sy, sm
            while (y < ey) or (y == ey and m <= em):
                month_list.append((m, y))
                m += 1
                if m > 12:
                    m = 1
                    y += 1
        except (ValueError, IndexError):
            pass

    if not month_list:
        months = max(1, min(months, 60))
        for i in range(months - 1, -1, -1):
            m = today.month - i
            y = today.year
            if m <= 0:
                m += 12
                y -= 1
            month_list.append((m, y))

    result = []
    with Session(engine) as session:
        from database import Category as _Cat
        _excl = [c.id for c in session.exec(select(_Cat).where(_Cat.exclude_from_spend == True)).all()]
        for m, y in month_list:
            _spend_filters = [
                Transaction.is_credit == False,
                Transaction.is_reimbursable == False,
                Transaction.user_id == user_id,
                func.strftime("%m", Transaction.date) == f"{m:02d}",
                func.strftime("%Y", Transaction.date) == str(y),
            ]
            if _excl:
                _spend_filters.append(Transaction.category_id.notin_(_excl))
            spend = float(session.exec(
                select(func.coalesce(func.sum(Transaction.amount), 0)).where(*_spend_filters)
            ).one())
            income = float(session.exec(
                select(func.coalesce(func.sum(Transaction.amount), 0)).where(
                    Transaction.is_credit == True,
                    Transaction.user_id == user_id,
                    func.strftime("%m", Transaction.date) == f"{m:02d}",
                    func.strftime("%Y", Transaction.date) == str(y),
                )
            ).one())
            result.append({
                "label": f"{month_abbr[m]} {str(y)[2:]}",
                "spend": round(spend, 2),
                "income": round(income, 2),
            })
    return result


@app.get("/api/dashboard/data-currency")
def dashboard_data_currency(authorization: str = __import__("fastapi").Header(default=None, alias="authorization")):
    """Return last transaction date per account and last payslip date per employer."""
    from datetime import date as _date
    from database import Transaction, Account, Payslip

    user_id = 1
    if authorization and authorization.startswith("Bearer "):
        try:
            with Session(engine) as _s:
                from database import UserSession
                tok = authorization[7:]
                sess = _s.get(UserSession, tok)
                if sess:
                    user_id = sess.user_id
        except Exception:
            pass

    today = _date.today()
    result: dict = {"transactions": [], "payslips": []}

    with Session(engine) as session:
        # Last transaction date per account
        accounts = session.exec(select(Account).where(Account.user_id == user_id)).all()
        for acct in accounts:
            last_date = session.exec(
                select(Transaction.date).where(
                    Transaction.user_id == user_id,
                    Transaction.account_id == acct.id,
                ).order_by(Transaction.date.desc()).limit(1)
            ).first()
            if last_date:
                days_ago = (today - last_date).days
                result["transactions"].append({"account": acct.name, "date": str(last_date), "days_ago": days_ago})

        # Last payslip date per employer
        employers = session.exec(
            select(Payslip.employer).where(
                Payslip.user_id == user_id,
                Payslip.employer != None,
            ).distinct()
        ).all()
        for employer in employers:
            last_date = session.exec(
                select(Payslip.pay_date).where(
                    Payslip.user_id == user_id,
                    Payslip.employer == employer,
                ).order_by(Payslip.pay_date.desc()).limit(1)
            ).first()
            if last_date:
                days_ago = (today - last_date).days
                result["payslips"].append({"employer": employer, "date": str(last_date), "days_ago": days_ago})

    result["transactions"].sort(key=lambda x: x["days_ago"])
    result["payslips"].sort(key=lambda x: x["days_ago"])
    return result


# Serve SPA for all non-API routes
static_dir = os.path.join(os.path.dirname(__file__), "static")

# Explicit routes for PWA static assets (must precede catch-all)
@app.get("/api/changelog")
async def get_changelog():
    """Return changelog entries from static/changelog.json."""
    import json as _j
    cl_path = os.path.join(static_dir, "changelog.json")
    try:
        with open(cl_path, "r") as f:
            return _j.load(f)
    except Exception:
        return []


@app.get("/manifest.json")
async def serve_manifest():
    return FileResponse(os.path.join(static_dir, "manifest.json"),
                        media_type="application/manifest+json")

@app.get("/icon.svg")
async def serve_icon():
    return FileResponse(os.path.join(static_dir, "icon.svg"), media_type="image/svg+xml")

@app.get("/sw.js")
async def serve_sw():
    return FileResponse(
        os.path.join(static_dir, "sw.js"),
        media_type="application/javascript",
        headers={"Service-Worker-Allowed": "/"},
    )

@app.get("/")
@app.get("/{path:path}")
async def serve_spa(path: str = ""):
    if path.startswith("api/"):
        return JSONResponse({"detail": "Not found"}, status_code=404)
    index_path = os.path.join(static_dir, "index.html")
    return FileResponse(index_path, headers={"Cache-Control": "no-store"})
