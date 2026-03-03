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
from routers import transactions, import_csv, categories, budgets, bills, tax, ai, gmail, insights, payslips, notify, crypto, investments, networth, profile, cgt, dividends, goals, score, chat, newsletter
import routers.super_tracker as super_tracker
import routers.stake_sync as stake_sync
import routers.receipts as receipts
import routers.merchants as merchants


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
                           mask_crypto_holding, mask_payslip, mask_amount, mask_description)

    # /api/transactions (list)
    if _re.fullmatch(r"/api/transactions/?", path):
        if isinstance(data, dict) and "items" in data:
            data["items"] = mask_transactions_list(data["items"])
        return data

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

    # /api/dashboard (main stats only, not /trend or /net-worth)
    if _re.fullmatch(r"/api/dashboard/?", path):
        for field in ("month_spend", "month_income", "prev_month_spend", "prev_month_income"):
            if field in data:
                data[field] = mask_amount(data[field] or 100, field)
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
        return data

    # /api/networth
    if "/networth" in path:
        if isinstance(data, list):
            for snap in data:
                for f in ("total_aud", "cash_aud", "investments_aud", "crypto_aud", "super_aud", "other_aud"):
                    if snap.get(f) is not None:
                        snap[f] = mask_amount(snap[f] or 1000, f"nw_{f}_{snap.get('id', 0)}")
        return data

    # /api/super
    if "/super" in path:
        if isinstance(data, list):
            for snap in data:
                if snap.get("balance_aud") is not None:
                    snap["balance_aud"] = mask_amount(snap["balance_aud"] or 1000, f"super_{snap.get('id', 0)}")
        return data

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

        # Only intercept JSON GET responses under /api/
        if request.method != "GET" or not path.startswith("/api/"):
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
    task = asyncio.create_task(_folder_watch_loop())
    yield
    task.cancel()


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
def dashboard(month: int = None, year: int = None):
    """Quick stats for the dashboard."""
    from datetime import date, timedelta
    from calendar import month_name, monthrange
    from sqlmodel import func
    from database import Transaction, Bill, Category

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
        def _sum(is_credit, m, y):
            return float(session.exec(
                select(func.coalesce(func.sum(Transaction.amount), 0)).where(
                    Transaction.is_credit == is_credit,
                    func.strftime("%m", Transaction.date) == f"{m:02d}",
                    func.strftime("%Y", Transaction.date) == str(y),
                )
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
            )
        ).one()

        # Uncategorised count
        uncat = session.exec(select(Category).where(Category.name == "Uncategorised")).first()
        uncategorised = 0
        if uncat:
            uncategorised = session.exec(
                select(func.count()).where(Transaction.category_id == uncat.id)
            ).one()

        # Bills due in 14 days (count + list)
        bill_cutoff = today + timedelta(days=14)
        bills_soon = session.exec(
            select(func.count()).where(
                Bill.is_active == True, Bill.next_due != None,
                Bill.next_due <= bill_cutoff,
            )
        ).one()
        upcoming_bill_rows = session.exec(
            select(Bill).where(
                Bill.is_active == True, Bill.next_due != None,
                Bill.next_due <= bill_cutoff,
            ).order_by(Bill.next_due)
        ).all()
        upcoming_bills = [
            {"name": b.name, "amount": round(b.amount_cents / 100, 2), "next_due": str(b.next_due)}
            for b in upcoming_bill_rows
        ]

        # Total transactions for selected month
        total_txns = session.exec(
            select(func.count(Transaction.id)).where(
                func.strftime("%m", Transaction.date) == f"{this_month:02d}",
                func.strftime("%Y", Transaction.date) == str(this_year),
            )
        ).one()

        # Spend & income by category
        all_txns = session.exec(
            select(Transaction).where(
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
            else:
                by_category[cat_name] = round(by_category.get(cat_name, 0) + t.amount, 2)

        # Top 5 spend transactions this month
        top_txn_rows = session.exec(
            select(Transaction).where(
                Transaction.is_credit == False,
                func.strftime("%m", Transaction.date) == f"{this_month:02d}",
                func.strftime("%Y", Transaction.date) == str(this_year),
            ).order_by(Transaction.amount.desc()).limit(5)
        ).all()
        top_transactions = []
        for t in top_txn_rows:
            cat = session.get(Category, t.category_id) if t.category_id else None
            top_transactions.append({
                "id": t.id,
                "description": t.description,
                "amount": float(t.amount),
                "date": str(t.date),
                "category_name": cat.name if cat else "Uncategorised",
            })

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
    }


@app.get("/api/dashboard/trend")
def dashboard_trend(months: int = 6, start: str = None, end: str = None):
    """Spend and income totals by month. Use months=N for last N months, or start/end as YYYY-MM."""
    from datetime import date
    from calendar import month_abbr
    from sqlmodel import func
    from database import Transaction

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
        for m, y in month_list:
            spend = float(session.exec(
                select(func.coalesce(func.sum(Transaction.amount), 0)).where(
                    Transaction.is_credit == False,
                    func.strftime("%m", Transaction.date) == f"{m:02d}",
                    func.strftime("%Y", Transaction.date) == str(y),
                )
            ).one())
            income = float(session.exec(
                select(func.coalesce(func.sum(Transaction.amount), 0)).where(
                    Transaction.is_credit == True,
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


# Serve SPA for all non-API routes
static_dir = os.path.join(os.path.dirname(__file__), "static")

@app.get("/")
@app.get("/{path:path}")
async def serve_spa(path: str = ""):
    if path.startswith("api/"):
        return JSONResponse({"detail": "Not found"}, status_code=404)
    index_path = os.path.join(static_dir, "index.html")
    return FileResponse(index_path, headers={"Cache-Control": "no-store"})
