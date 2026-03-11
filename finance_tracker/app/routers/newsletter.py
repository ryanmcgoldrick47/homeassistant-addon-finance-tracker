from __future__ import annotations

"""
Weekly Finance Newsletter — builds and sends an HTML email digest.
"""

import json
import smtplib
import ssl
from datetime import date, timedelta
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from typing import Optional

from fastapi import APIRouter, Depends, HTTPException, Request
from sqlmodel import Session, select, func

from database import (
    get_session, engine, Transaction, Category, Budget, Bill, BillPayment,
    Goal, Setting, MerchantEnrichment, User,
    PaperPortfolio, PaperHolding, PaperTrade, PaperAnalysis,
)
from deps import get_setting, get_current_user

router = APIRouter(prefix="/api/newsletter", tags=["newsletter"])


# ---------------------------------------------------------------------------
# Data gathering
# ---------------------------------------------------------------------------

def _fmt(v: float) -> str:
    """Format a dollar amount with commas."""
    return f"${v:,.2f}"


def _pct(part: float, total: float) -> str:
    if total <= 0:
        return "0%"
    return f"{part / total * 100:.1f}%"


def _gather(session: Session, user_id: int) -> dict:
    today = date.today()
    week_start = today - timedelta(days=7)
    prev_week_start = today - timedelta(days=14)

    # Current month bounds
    mtd_start = today.replace(day=1)
    from calendar import monthrange
    mtd_end = today.replace(day=monthrange(today.year, today.month)[1])

    # ── This week ──
    def _week_sum(is_credit: bool, start: date, end: date) -> float:
        return float(session.exec(
            select(func.coalesce(func.sum(Transaction.amount), 0)).where(
                Transaction.user_id == user_id,
                Transaction.is_credit == is_credit,
                Transaction.date >= start,
                Transaction.date <= end,
            )
        ).one())

    w_income  = _week_sum(True,  week_start, today)
    w_spend   = _week_sum(False, week_start, today)
    pw_income = _week_sum(True,  prev_week_start, week_start - timedelta(days=1))
    pw_spend  = _week_sum(False, prev_week_start, week_start - timedelta(days=1))

    # ── Month-to-date ──
    mtd_income = _week_sum(True,  mtd_start, today)
    mtd_spend  = _week_sum(False, mtd_start, today)
    savings_rate = round((mtd_income - mtd_spend) / mtd_income * 100, 1) if mtd_income > 0 else 0

    # ── Top 5 transactions this week ──
    top_txns_rows = session.exec(
        select(Transaction).where(
            Transaction.user_id == user_id,
            Transaction.is_credit == False,
            Transaction.date >= week_start,
            Transaction.date <= today,
        ).order_by(Transaction.amount.desc()).limit(5)
    ).all()
    # Enrich top transaction names
    top_raw_keys = {t.description.strip().upper()[:50] for t in top_txns_rows}
    enrichments = {
        e.raw_key: e for e in session.exec(
            select(MerchantEnrichment).where(MerchantEnrichment.raw_key.in_(top_raw_keys))
        ).all()
    } if top_raw_keys else {}

    top_txns = []
    for t in top_txns_rows:
        cat = session.get(Category, t.category_id) if t.category_id else None
        enrich = enrichments.get(t.description.strip().upper()[:50])
        top_txns.append({
            "date": str(t.date),
            "description": enrich.clean_name if enrich else t.description,
            "amount": float(t.amount),
            "category": cat.name if cat else "Uncategorised",
            "logo_domain": enrich.domain if enrich else None,
        })

    # ── Budget status (current month) ──
    budgets_rows = session.exec(
        select(Budget).where(
            Budget.user_id == user_id,
            Budget.month == today.month,
            Budget.year == today.year,
        )
    ).all()
    budgets = []
    for b in budgets_rows:
        cat = session.get(Category, b.category_id)
        if not cat:
            continue
        spend = float(session.exec(
            select(func.coalesce(func.sum(Transaction.amount), 0)).where(
                Transaction.user_id == user_id,
                Transaction.is_credit == False,
                Transaction.category_id == b.category_id,
                func.strftime("%m", Transaction.date) == f"{today.month:02d}",
                func.strftime("%Y", Transaction.date) == str(today.year),
            )
        ).one())
        budget_amt = b.amount_cents / 100
        pct_used = round(spend / budget_amt * 100, 1) if budget_amt > 0 else 0
        budgets.append({
            "category": cat.name,
            "colour": cat.colour,
            "budget": round(budget_amt, 2),
            "spend": round(spend, 2),
            "pct": pct_used,
            "over": spend > budget_amt,
        })
    budgets.sort(key=lambda x: x["pct"], reverse=True)

    # ── Action items ──
    uncat_cat = session.exec(select(Category).where(Category.name == "Uncategorised")).first()
    uncategorised = 0
    if uncat_cat:
        uncategorised = int(session.exec(
            select(func.count()).where(
                Transaction.user_id == user_id,
                Transaction.category_id == uncat_cat.id,
            )
        ).one())

    flagged = int(session.exec(
        select(func.count()).where(
            Transaction.user_id == user_id,
            Transaction.is_flagged == True,
            Transaction.is_reviewed == False,
        )
    ).one())

    overdue_bills = session.exec(
        select(Bill).where(
            Bill.user_id == user_id,
            Bill.is_active == True,
            Bill.next_due != None,
            Bill.next_due < today,
        )
    ).all()

    due_soon_bills = session.exec(
        select(Bill).where(
            Bill.user_id == user_id,
            Bill.is_active == True,
            Bill.next_due != None,
            Bill.next_due >= today,
            Bill.next_due <= today + timedelta(days=7),
        ).order_by(Bill.next_due)
    ).all()

    # ── Goals ──
    goals_rows = session.exec(
        select(Goal).where(
            Goal.user_id == user_id,
            Goal.is_complete == False,
        ).order_by(Goal.target_date)
    ).all()
    goals = []
    for g in goals_rows:
        pct = round(g.current_cents / g.target_cents * 100, 1) if g.target_cents > 0 else 0
        goals.append({
            "name": g.name,
            "target": round(g.target_cents / 100, 2),
            "current": round(g.current_cents / 100, 2),
            "pct": min(pct, 100),
            "target_date": str(g.target_date) if g.target_date else None,
        })

    # ── Finance score (current month) ──
    from routers.score import _compute_score
    score_data = _compute_score(session, today.month, today.year, user_id)

    # ── AI Trader portfolio ──
    paper_data = None
    try:
        portfolio = session.exec(
            select(PaperPortfolio).where(PaperPortfolio.user_id == user_id)
        ).first()
        if portfolio:
            holdings = session.exec(
                select(PaperHolding).where(PaperHolding.portfolio_id == portfolio.id)
            ).all()
            holdings_value = sum(h.value_aud for h in holdings)
            total_value = round(portfolio.cash_aud + holdings_value, 2)
            total_gain = round(total_value - portfolio.starting_cash, 2)
            return_pct = round(total_gain / portfolio.starting_cash * 100, 2) if portfolio.starting_cash > 0 else 0.0

            # Trades this week
            week_trades = session.exec(
                select(PaperTrade).where(
                    PaperTrade.portfolio_id == portfolio.id,
                    PaperTrade.executed_at >= week_start.isoformat(),
                ).order_by(PaperTrade.executed_at.desc())
            ).all()

            # Latest analysis
            latest_analysis = session.exec(
                select(PaperAnalysis).where(PaperAnalysis.portfolio_id == portfolio.id)
                .order_by(PaperAnalysis.created_at.desc())
            ).first()

            paper_data = {
                "total_value": total_value,
                "cash_aud": round(portfolio.cash_aud, 2),
                "holdings_value": round(holdings_value, 2),
                "starting_cash": portfolio.starting_cash,
                "total_gain": total_gain,
                "return_pct": return_pct,
                "holdings": [
                    {
                        "ticker": h.ticker,
                        "qty": h.qty,
                        "value_aud": round(h.value_aud, 2),
                        "gain_pct": round(h.gain_pct, 2),
                    }
                    for h in sorted(holdings, key=lambda x: x.value_aud, reverse=True)
                ],
                "week_trades": [
                    {
                        "ticker": t.ticker,
                        "side": t.side,
                        "qty": t.qty,
                        "price_aud": round(t.price_aud, 4),
                        "total_aud": round(abs(t.total_aud), 2),
                        "brokerage_aud": round(t.brokerage_aud, 2),
                        "executed_at": t.executed_at[:10],
                        "reason": (t.reason or "")[:120],
                    }
                    for t in week_trades
                ],
                "latest_analysis_text": latest_analysis.analysis_text[:600] if latest_analysis else None,
                "latest_analysis_date": latest_analysis.created_at[:10] if latest_analysis else None,
                "total_trades_all_time": session.exec(
                    select(func.count()).where(PaperTrade.portfolio_id == portfolio.id)
                ).one(),
            }
    except Exception:
        paper_data = None

    # ── Roadmap — rotating pool of genuine pending items (not yet built) ──
    _ROADMAP_POOL = [
        {
            "title": "Basiq CDR Open Banking",
            "desc": "Direct Macquarie bank feed via Consumer Data Right API — replaces manual CSV download entirely. Transactions sync automatically every morning.",
            "priority": "high",
            "chat_prompt": "Let's integrate Basiq CDR open banking for automatic daily transaction sync",
        },
        {
            "title": "Net Worth Forecast",
            "desc": "Project net worth 1, 3, and 5 years out based on current savings rate, investment return assumptions, and inflation. Chart shows confidence bands.",
            "priority": "high",
            "chat_prompt": "Let's build a net worth forecast with 1/3/5 year projections and a confidence band chart",
        },
        {
            "title": "OCR Receipt Scanner",
            "desc": "Upload a photo of a paper receipt → AI extracts merchant, date, and amount. Attaches to the matching transaction automatically.",
            "priority": "medium",
            "chat_prompt": "Let's add OCR receipt scanning — upload a photo and AI extracts the transaction details",
        },
        {
            "title": "Subscription Manager",
            "desc": "Auto-detect recurring charges from transaction history, group them as subscriptions, and alert when a price increases month-on-month.",
            "priority": "medium",
            "chat_prompt": "Let's build a subscription manager that detects recurring charges and flags price increases",
        },
        {
            "title": "Mortgage & Loan Tracker",
            "desc": "Track principal vs interest breakdown for any loan. Show payoff date, total interest saved by making extra repayments, and offset account impact.",
            "priority": "medium",
            "chat_prompt": "Let's add a mortgage/loan tracker with amortisation schedule and extra-repayment calculator",
        },
        {
            "title": "Financial Year Comparison",
            "desc": "Side-by-side FY24/25 vs FY25/26 dashboard: spend, income, savings rate, tax, super, and top categories for each year.",
            "priority": "medium",
            "chat_prompt": "Let's build a financial year comparison view for FY24/25 vs FY25/26",
        },
        {
            "title": "ATO Pre-Fill Tax Summary",
            "desc": "Generate a structured tax return summary using your payslip YTD data, deductible expenses, CGT events, and dividend income — ready to cross-check against MyGov pre-fill.",
            "priority": "medium",
            "chat_prompt": "Let's build an ATO pre-fill style tax summary using all our payslip and transaction data",
        },
        {
            "title": "Round-Up Savings Tracker",
            "desc": "Round each transaction up to the nearest dollar, accumulate the difference, and track it as an auto-contribution to a savings goal.",
            "priority": "low",
            "chat_prompt": "Let's add a round-up savings tracker that auto-feeds a chosen savings goal",
        },
        {
            "title": "Overseas Spend Analyser",
            "desc": "Break down spending by country and currency. Show total VISA foreign transaction fees paid and identify the highest-fee merchants.",
            "priority": "low",
            "chat_prompt": "Let's build an overseas spend analyser showing spend by country, currency, and total FX fees",
        },
        {
            "title": "Bill Renewal Reminders",
            "desc": "Track contract end dates for electricity, internet, and insurance. Send HA push alerts 45 days before expiry so you have time to negotiate or switch.",
            "priority": "low",
            "chat_prompt": "Let's add contract renewal reminders for utilities/insurance with HA push alerts",
        },
        {
            "title": "Salary Packaging Calculator",
            "desc": "Model FBT, salary sacrifice into super, and novated lease scenarios. Compare take-home pay under different packaging arrangements for both employers.",
            "priority": "low",
            "chat_prompt": "Let's build a salary packaging calculator for FBT and super sacrifice scenarios",
        },
        {
            "title": "Income Tax Optimiser",
            "desc": "Given your two employers and current YTD figures, model whether adjusting PAYG withholding or making deductible contributions would reduce your end-of-year tax bill.",
            "priority": "low",
            "chat_prompt": "Let's build an income tax optimiser that models PAYG adjustments and concessional contributions",
        },
    ]

    # Rotate through roadmap pool by ISO week so a different set shows each week
    _week_num = today.isocalendar()[1]
    _roadmap_start = (_week_num * 3) % len(_ROADMAP_POOL)
    _roadmap_rotated = (_ROADMAP_POOL[_roadmap_start:] + _ROADMAP_POOL[:_roadmap_start])
    ROADMAP_ITEMS = _roadmap_rotated[:4]

    # ── Feature suggestions — rotating pool from market-leading apps ──
    _SUGGESTIONS_POOL = [
        {
            "app": "Frollo",
            "feature": "Budget 80% Push Alerts",
            "desc": "HA notification the moment you hit 80% of any budget category — gives you time to course-correct before month end.",
            "chat_prompt": "Let's add real-time HA push alerts when any budget category reaches 80%",
        },
        {
            "app": "Splitwise",
            "feature": "Shared Expense Splitting",
            "desc": "Split transactions with Karina — log who paid, who owes what, and generate a monthly settlement summary.",
            "chat_prompt": "Let's add shared expense splitting between Ryan and Karina with monthly settlement",
        },
        {
            "app": "Sharesight",
            "feature": "Portfolio vs ASX 200 Benchmark",
            "desc": "Compare your share portfolio's time-weighted return against the ASX 200 or S&P 500 for any date range.",
            "chat_prompt": "Let's add benchmark comparison charts to the investments page — TWR vs ASX 200",
        },
        {
            "app": "Sharesight",
            "feature": "Tax-Loss Harvesting Alerts",
            "desc": "Flag positions sitting at an unrealised loss before 30 June — surfaces opportunities to crystallise losses and offset capital gains.",
            "chat_prompt": "Let's add a 30-June tax-loss harvesting alert that flags positions with unrealised losses",
        },
        {
            "app": "YNAB",
            "feature": "Rolling 12-Month Savings Rate",
            "desc": "Show savings rate as a 12-month rolling average — smooths out lumpy income months and gives a clearer trend.",
            "chat_prompt": "Let's add a 12-month rolling savings rate chart on the dashboard",
        },
        {
            "app": "Copilot",
            "feature": "Month-End Spend Forecast",
            "desc": "Based on spend-per-day so far, project what each category will total by the 31st. Shown as a progress bar vs budget.",
            "chat_prompt": "Let's add a month-end spend forecast that projects category totals based on daily pace",
        },
        {
            "app": "Revolut",
            "feature": "Auto Subscription Detection",
            "desc": "Automatically identify recurring payments from transaction patterns — no manual entry. Show upcoming charges and month-over-month changes.",
            "chat_prompt": "Let's auto-detect subscriptions from transaction patterns and display upcoming charges",
        },
        {
            "app": "Wise",
            "feature": "FX Rate Watchlist",
            "desc": "Set target AUD/USD (or other) rates and get an HA notification when the rate is reached — handy when planning overseas travel or transfers.",
            "chat_prompt": "Let's add an FX rate watchlist with HA alerts when target rates are reached",
        },
        {
            "app": "PocketSmith",
            "feature": "Cash Flow Calendar",
            "desc": "Show projected daily account balance for the next 90 days — plotting known bills, estimated income, and recurring expenses on a calendar.",
            "chat_prompt": "Let's build a 90-day cash flow calendar with projected daily balances",
        },
        {
            "app": "Bricklet",
            "feature": "Investment Property Tracker",
            "desc": "Track property value estimates, rental income, expenses, depreciation, and net yield — feeds into net worth and tax pages.",
            "chat_prompt": "Let's add an investment property tracker for rental income, expenses, and yield",
        },
    ]

    _suggestions_start = (_week_num * 7) % len(_SUGGESTIONS_POOL)
    _suggestions_rotated = (_SUGGESTIONS_POOL[_suggestions_start:] + _SUGGESTIONS_POOL[:_suggestions_start])
    FEATURE_SUGGESTIONS = _suggestions_rotated[:3]

    return {
        "generated_at": str(today),
        "week_label": f"{week_start.strftime('%-d %b')} – {today.strftime('%-d %b %Y')}",
        "month_label": today.strftime("%B %Y"),
        "week": {
            "income": round(w_income, 2),
            "spend": round(w_spend, 2),
            "net": round(w_income - w_spend, 2),
            "prev_income": round(pw_income, 2),
            "prev_spend": round(pw_spend, 2),
            "prev_net": round(pw_income - pw_spend, 2),
        },
        "mtd": {
            "income": round(mtd_income, 2),
            "spend": round(mtd_spend, 2),
            "net": round(mtd_income - mtd_spend, 2),
            "savings_rate": savings_rate,
        },
        "top_txns": top_txns,
        "budgets": budgets,
        "goals": goals,
        "action_items": {
            "uncategorised": uncategorised,
            "flagged": flagged,
            "overdue_bills": [{"name": b.name, "amount": b.amount_cents / 100, "due": str(b.next_due)} for b in overdue_bills],
            "due_soon_bills": [{"name": b.name, "amount": b.amount_cents / 100, "due": str(b.next_due)} for b in due_soon_bills],
        },
        "score": score_data,
        "roadmap": ROADMAP_ITEMS[:4],          # top 4 items in email
        "feature_suggestions": FEATURE_SUGGESTIONS[:3],  # top 3 suggestions
        "paper_trader": paper_data,
    }


# ---------------------------------------------------------------------------
# AI insights paragraph
# ---------------------------------------------------------------------------

async def _generate_insights(data: dict, api_key: str) -> str:
    try:
        import anthropic
        client = anthropic.Anthropic(api_key=api_key)
        w = data["week"]
        mtd = data["mtd"]
        score = data["score"]
        over_budget = [b for b in data["budgets"] if b["over"]]
        pt = data.get("paper_trader")
        trader_line = ""
        if pt:
            sign = "+" if pt["return_pct"] >= 0 else ""
            trader_line = (
                f"\n- AI Trader portfolio: ${pt['total_value']:,.2f} total value, "
                f"{sign}{pt['return_pct']:.2f}% return since inception, "
                f"{len(pt['week_trades'])} trade(s) this week"
            )
        prompt = f"""Write a concise 3-4 sentence weekly finance summary for Ryan.

Data:
- This week: spent ${w['spend']:,.2f}, earned ${w['income']:,.2f}, net ${w['net']:+,.2f}
- Last week: spent ${w['prev_spend']:,.2f}, net ${w['prev_net']:+,.2f}
- Month-to-date: spent ${mtd['spend']:,.2f}, earned ${mtd['income']:,.2f}, savings rate {mtd['savings_rate']:.1f}%
- Finance score: {score['score']}/100
- Over-budget categories: {', '.join(b['category'] for b in over_budget) or 'none'}
- Action items: {data['action_items']['uncategorised']} uncategorised, {data['action_items']['flagged']} flagged
- Active goals: {len(data['goals'])}{trader_line}

Write in second person ("You spent…"). Be encouraging but honest. Highlight one key win and one area to watch.
If the AI Trader data is present, include a brief note on its performance. No bullet points."""

        response = client.messages.create(
            model="claude-haiku-4-5-20251001",
            max_tokens=256,
            messages=[{"role": "user", "content": prompt}],
        )
        return response.content[0].text.strip()
    except Exception:
        return ""


# ---------------------------------------------------------------------------
# HTML email builder
# ---------------------------------------------------------------------------

def _colour_bar(pct: float, colour: str, over: bool) -> str:
    bar_colour = "#ef4444" if over else colour
    width = min(int(pct), 100)
    return (
        f'<div style="background:#1e293b;border-radius:4px;height:6px;margin-top:4px;">'
        f'<div style="background:{bar_colour};width:{width}%;height:6px;border-radius:4px;"></div>'
        f'</div>'
    )


def _build_html(data: dict, app_url: str, insights: str) -> str:
    w    = data["week"]
    mtd  = data["mtd"]
    ai   = data["action_items"]
    score = data["score"]
    base  = app_url.rstrip("/") if app_url else "#"

    # ── Header ──
    net_colour  = "#22c55e" if w["net"] >= 0 else "#ef4444"
    net_sign    = "+" if w["net"] >= 0 else ""
    score_colour = "#22c55e" if score["score"] >= 80 else "#f59e0b" if score["score"] >= 60 else "#ef4444"

    html = f"""<!DOCTYPE html>
<html lang="en">
<head><meta charset="UTF-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>Finance Weekly: {data['week_label']}</title></head>
<body style="margin:0;padding:0;background:#0f172a;font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',sans-serif;color:#e2e8f0;">
<table width="100%" cellpadding="0" cellspacing="0" style="background:#0f172a;">
<tr><td align="center" style="padding:24px 16px;">
<table width="600" cellpadding="0" cellspacing="0" style="max-width:600px;width:100%;">

<!-- Header -->
<tr><td style="background:#1e3a5f;border-radius:16px 16px 0 0;padding:28px 32px;text-align:center;">
  <div style="font-size:28px;margin-bottom:4px;">💰</div>
  <h1 style="margin:0;font-size:22px;font-weight:800;color:#fff;">Finance Weekly</h1>
  <p style="margin:6px 0 0;font-size:13px;color:#94a3b8;">{data['week_label']} &nbsp;·&nbsp; {data['month_label']}</p>
</td></tr>

<!-- Score + MTD snapshot -->
<tr><td style="background:#1a2e4a;padding:20px 32px;">
  <table width="100%" cellpadding="0" cellspacing="0">
  <tr>
    <td width="25%" align="center" style="padding:8px;">
      <div style="font-size:11px;color:#94a3b8;text-transform:uppercase;letter-spacing:.06em;margin-bottom:4px;">Finance Score</div>
      <div style="font-size:26px;font-weight:800;color:{score_colour};">{score['score']}</div>
      <div style="font-size:10px;color:#64748b;">/ 100</div>
    </td>
    <td width="25%" align="center" style="padding:8px;">
      <div style="font-size:11px;color:#94a3b8;text-transform:uppercase;letter-spacing:.06em;margin-bottom:4px;">MTD Income</div>
      <div style="font-size:18px;font-weight:700;color:#22c55e;">{_fmt(mtd['income'])}</div>
    </td>
    <td width="25%" align="center" style="padding:8px;">
      <div style="font-size:11px;color:#94a3b8;text-transform:uppercase;letter-spacing:.06em;margin-bottom:4px;">MTD Spend</div>
      <div style="font-size:18px;font-weight:700;color:#ef4444;">{_fmt(mtd['spend'])}</div>
    </td>
    <td width="25%" align="center" style="padding:8px;">
      <div style="font-size:11px;color:#94a3b8;text-transform:uppercase;letter-spacing:.06em;margin-bottom:4px;">Savings Rate</div>
      <div style="font-size:18px;font-weight:700;color:{'#22c55e' if mtd['savings_rate']>0 else '#ef4444'};">{mtd['savings_rate']:.1f}%</div>
    </td>
  </tr>
  </table>
</td></tr>

<!-- This week summary -->
<tr><td style="background:#162032;padding:20px 32px 16px;">
  <h2 style="margin:0 0 14px;font-size:14px;font-weight:700;color:#e2e8f0;text-transform:uppercase;letter-spacing:.06em;">This Week</h2>
  <table width="100%" cellpadding="0" cellspacing="0">
  <tr>
    <td style="padding:8px 12px;background:#1e293b;border-radius:10px;text-align:center;">
      <div style="font-size:11px;color:#94a3b8;margin-bottom:3px;">Income</div>
      <div style="font-size:20px;font-weight:700;color:#22c55e;">{_fmt(w['income'])}</div>
      <div style="font-size:11px;color:#64748b;margin-top:2px;">prev {_fmt(w['prev_income'])}</div>
    </td>
    <td style="width:12px;"></td>
    <td style="padding:8px 12px;background:#1e293b;border-radius:10px;text-align:center;">
      <div style="font-size:11px;color:#94a3b8;margin-bottom:3px;">Spend</div>
      <div style="font-size:20px;font-weight:700;color:#ef4444;">{_fmt(w['spend'])}</div>
      <div style="font-size:11px;color:#64748b;margin-top:2px;">prev {_fmt(w['prev_spend'])}</div>
    </td>
    <td style="width:12px;"></td>
    <td style="padding:8px 12px;background:#1e293b;border-radius:10px;text-align:center;">
      <div style="font-size:11px;color:#94a3b8;margin-bottom:3px;">Net</div>
      <div style="font-size:20px;font-weight:700;color:{net_colour};">{net_sign}{_fmt(w['net'])}</div>
      <div style="font-size:11px;color:#64748b;margin-top:2px;">prev {'+' if w['prev_net']>=0 else ''}{_fmt(w['prev_net'])}</div>
    </td>
  </tr>
  </table>
</td></tr>
"""

    # ── AI Insights ──
    if insights:
        html += f"""
<tr><td style="background:#162032;padding:0 32px 20px;">
  <div style="background:#1e3a5f;border-left:3px solid #38bdf8;border-radius:0 10px 10px 0;padding:14px 16px;">
    <div style="font-size:11px;color:#38bdf8;font-weight:600;text-transform:uppercase;letter-spacing:.06em;margin-bottom:6px;">Weekly Insights</div>
    <p style="margin:0;font-size:13px;line-height:1.6;color:#cbd5e1;">{insights}</p>
  </div>
</td></tr>
"""

    # ── Action items ──
    action_count = (ai['uncategorised'] + ai['flagged'] +
                    len(ai['overdue_bills']) + len(ai['due_soon_bills']))
    if action_count > 0:
        html += f"""
<tr><td style="background:#162032;padding:0 32px 20px;">
  <h2 style="margin:0 0 12px;font-size:14px;font-weight:700;color:#e2e8f0;text-transform:uppercase;letter-spacing:.06em;">Action Items</h2>
  <table width="100%" cellpadding="0" cellspacing="0" style="border-collapse:collapse;">
"""
        if ai['uncategorised'] > 0:
            html += f"""  <tr>
    <td style="padding:10px 14px;background:#1e293b;border-radius:8px;margin-bottom:6px;">
      <table width="100%"><tr>
        <td><span style="font-size:18px;">❓</span> <span style="font-size:13px;color:#e2e8f0;">{ai['uncategorised']} transaction{'s' if ai['uncategorised']!=1 else ''} need categorising</span></td>
        <td align="right"><a href="{base}#transactions" style="font-size:11px;color:#38bdf8;text-decoration:none;border:1px solid #38bdf8;padding:4px 10px;border-radius:99px;">Review →</a></td>
      </tr></table>
    </td>
  </tr>
  <tr><td style="height:6px;"></td></tr>
"""
        if ai['flagged'] > 0:
            html += f"""  <tr>
    <td style="padding:10px 14px;background:#1e293b;border-radius:8px;">
      <table width="100%"><tr>
        <td><span style="font-size:18px;">🚩</span> <span style="font-size:13px;color:#e2e8f0;">{ai['flagged']} flagged transaction{'s' if ai['flagged']!=1 else ''} awaiting review</span></td>
        <td align="right"><a href="{base}#flags" style="font-size:11px;color:#38bdf8;text-decoration:none;border:1px solid #38bdf8;padding:4px 10px;border-radius:99px;">Review →</a></td>
      </tr></table>
    </td>
  </tr>
  <tr><td style="height:6px;"></td></tr>
"""
        for b in ai['overdue_bills']:
            html += f"""  <tr>
    <td style="padding:10px 14px;background:#2d1b1b;border-radius:8px;border:1px solid #ef4444;">
      <table width="100%"><tr>
        <td><span style="font-size:18px;">⚠️</span> <span style="font-size:13px;color:#fca5a5;"><strong>{b['name']}</strong> overdue (was due {b['due']}, ${b['amount']:,.2f})</span></td>
        <td align="right"><a href="{base}#bills" style="font-size:11px;color:#ef4444;text-decoration:none;border:1px solid #ef4444;padding:4px 10px;border-radius:99px;">Pay →</a></td>
      </tr></table>
    </td>
  </tr>
  <tr><td style="height:6px;"></td></tr>
"""
        for b in ai['due_soon_bills']:
            html += f"""  <tr>
    <td style="padding:10px 14px;background:#1e293b;border-radius:8px;">
      <table width="100%"><tr>
        <td><span style="font-size:18px;">📅</span> <span style="font-size:13px;color:#e2e8f0;"><strong>{b['name']}</strong> due {b['due']} — ${b['amount']:,.2f}</span></td>
        <td align="right"><a href="{base}#bills" style="font-size:11px;color:#38bdf8;text-decoration:none;border:1px solid #38bdf8;padding:4px 10px;border-radius:99px;">View →</a></td>
      </tr></table>
    </td>
  </tr>
  <tr><td style="height:6px;"></td></tr>
"""
        html += "  </table>\n</td></tr>\n"

    # ── Top transactions ──
    if data["top_txns"]:
        html += f"""
<tr><td style="background:#162032;padding:0 32px 20px;">
  <h2 style="margin:0 0 12px;font-size:14px;font-weight:700;color:#e2e8f0;text-transform:uppercase;letter-spacing:.06em;">Top Expenses This Week</h2>
  <table width="100%" cellpadding="0" cellspacing="0">
"""
        for t in data["top_txns"]:
            logo_html = ""
            if t.get("logo_domain"):
                logo_html = (
                    f'<img src="https://www.google.com/s2/favicons?domain={t["logo_domain"]}&sz=32" '
                    f'style="width:20px;height:20px;border-radius:4px;vertical-align:middle;'
                    f'margin-right:8px;background:#1e293b;" />'
                )
            html += f"""  <tr>
    <td style="padding:8px 0;border-bottom:1px solid #1e293b;">
      <table width="100%"><tr>
        <td>
          <div style="font-size:13px;color:#e2e8f0;">{logo_html}{t['description']}</div>
          <div style="font-size:11px;color:#64748b;margin-top:2px;">{t['date']} &nbsp;·&nbsp; {t['category']}</div>
        </td>
        <td align="right" style="font-size:14px;font-weight:600;color:#ef4444;white-space:nowrap;">${t['amount']:,.2f}</td>
      </tr></table>
    </td>
  </tr>
"""
        html += "  </table>\n</td></tr>\n"

    # ── Budget status ──
    over_budget = [b for b in data["budgets"] if b["over"]]
    near_budget = [b for b in data["budgets"] if not b["over"] and b["pct"] >= 70]
    budget_items = over_budget + near_budget
    if budget_items:
        html += f"""
<tr><td style="background:#162032;padding:0 32px 20px;">
  <h2 style="margin:0 0 12px;font-size:14px;font-weight:700;color:#e2e8f0;text-transform:uppercase;letter-spacing:.06em;"><a href="{base}#budgets" style="color:#e2e8f0;text-decoration:none;">Budget Status — {data['month_label']} →</a></h2>
  <table width="100%" cellpadding="0" cellspacing="0">
"""
        for b in budget_items[:6]:
            status_colour = "#ef4444" if b["over"] else "#f59e0b"
            label = "OVER" if b["over"] else f"{b['pct']:.0f}%"
            html += f"""  <tr>
    <td style="padding:7px 0;border-bottom:1px solid #1e293b;">
      <table width="100%"><tr>
        <td style="font-size:13px;color:#e2e8f0;" width="45%">{b['category']}</td>
        <td style="font-size:12px;color:#94a3b8;" width="35%">${b['spend']:,.0f} / ${b['budget']:,.0f}</td>
        <td align="right" style="font-size:12px;font-weight:700;color:{status_colour};" width="20%">{label}</td>
      </tr></table>
      <div style="background:#1e293b;border-radius:3px;height:4px;margin-top:4px;"><div style="background:{status_colour};width:{min(int(b['pct']),100)}%;height:4px;border-radius:3px;"></div></div>
    </td>
  </tr>
"""
        html += "  </table>\n</td></tr>\n"

    # ── Goals ──
    if data["goals"]:
        html += f"""
<tr><td style="background:#162032;padding:0 32px 20px;">
  <h2 style="margin:0 0 12px;font-size:14px;font-weight:700;color:#e2e8f0;text-transform:uppercase;letter-spacing:.06em;">Savings Goals</h2>
  <table width="100%" cellpadding="0" cellspacing="0">
"""
        for g in data["goals"][:4]:
            bar_colour = "#22c55e" if g["pct"] >= 100 else "#38bdf8" if g["pct"] >= 75 else "#f59e0b"
            date_str = f" · by {g['target_date']}" if g["target_date"] else ""
            html += f"""  <tr>
    <td style="padding:8px 0;border-bottom:1px solid #1e293b;">
      <a href="{base}#goals" style="text-decoration:none;">
      <table width="100%"><tr>
        <td style="font-size:13px;color:#e2e8f0;">{g['name']}<span style="font-size:11px;color:#64748b;">{date_str}</span></td>
        <td align="right" style="font-size:12px;color:#94a3b8;white-space:nowrap;">${g['current']:,.0f} / ${g['target']:,.0f}</td>
      </tr></table>
      <div style="background:#1e293b;border-radius:3px;height:5px;margin-top:5px;"><div style="background:{bar_colour};width:{int(g['pct'])}%;height:5px;border-radius:3px;"></div></div>
      </a>
    </td>
  </tr>
"""
        html += "  </table>\n</td></tr>\n"

    # ── AI Trader ──
    pt = data.get("paper_trader")
    if pt:
        ret_colour = "#22c55e" if pt["return_pct"] >= 0 else "#ef4444"
        ret_sign   = "+" if pt["return_pct"] >= 0 else ""
        html += f"""
<tr><td style="background:#162032;padding:0 32px 20px;">
  <h2 style="margin:0 0 12px;font-size:14px;font-weight:700;color:#e2e8f0;text-transform:uppercase;letter-spacing:.06em;">
    <a href="{base}#paper" style="color:#e2e8f0;text-decoration:none;">🤖 AI Trader Portfolio →</a>
  </h2>
  <!-- Stats row -->
  <table width="100%" cellpadding="0" cellspacing="0" style="margin-bottom:12px;">
  <tr>
    <td width="25%" align="center" style="padding:8px 6px;background:#1e293b;border-radius:8px;">
      <div style="font-size:10px;color:#94a3b8;text-transform:uppercase;letter-spacing:.05em;margin-bottom:3px;">Portfolio</div>
      <div style="font-size:17px;font-weight:700;color:#e2e8f0;">{_fmt(pt['total_value'])}</div>
    </td>
    <td width="4px"></td>
    <td width="25%" align="center" style="padding:8px 6px;background:#1e293b;border-radius:8px;">
      <div style="font-size:10px;color:#94a3b8;text-transform:uppercase;letter-spacing:.05em;margin-bottom:3px;">Return</div>
      <div style="font-size:17px;font-weight:700;color:{ret_colour};">{ret_sign}{pt['return_pct']:.2f}%</div>
    </td>
    <td width="4px"></td>
    <td width="25%" align="center" style="padding:8px 6px;background:#1e293b;border-radius:8px;">
      <div style="font-size:10px;color:#94a3b8;text-transform:uppercase;letter-spacing:.05em;margin-bottom:3px;">Cash</div>
      <div style="font-size:17px;font-weight:700;color:#e2e8f0;">{_fmt(pt['cash_aud'])}</div>
    </td>
    <td width="4px"></td>
    <td width="21%" align="center" style="padding:8px 6px;background:#1e293b;border-radius:8px;">
      <div style="font-size:10px;color:#94a3b8;text-transform:uppercase;letter-spacing:.05em;margin-bottom:3px;">P&amp;L</div>
      <div style="font-size:17px;font-weight:700;color:{ret_colour};">{'+' if pt['total_gain']>=0 else ''}{_fmt(pt['total_gain'])}</div>
    </td>
  </tr>
  </table>
"""
        # Current holdings (top 5)
        if pt["holdings"]:
            html += """  <div style="font-size:11px;color:#64748b;text-transform:uppercase;letter-spacing:.05em;margin-bottom:6px;">Current Positions</div>
  <table width="100%" cellpadding="0" cellspacing="0" style="margin-bottom:12px;">
"""
            for h in pt["holdings"][:5]:
                h_colour = "#22c55e" if h["gain_pct"] >= 0 else "#ef4444"
                h_sign   = "+" if h["gain_pct"] >= 0 else ""
                html += f"""  <tr>
    <td style="padding:5px 0;border-bottom:1px solid #1e293b;">
      <table width="100%"><tr>
        <td style="font-size:12px;font-weight:600;color:#e2e8f0;">{h['ticker']}</td>
        <td style="font-size:12px;color:#94a3b8;text-align:center;">{h['qty']} units</td>
        <td style="font-size:12px;font-weight:600;color:#e2e8f0;text-align:right;">{_fmt(h['value_aud'])}</td>
        <td style="font-size:12px;color:{h_colour};text-align:right;padding-left:8px;">{h_sign}{h['gain_pct']:.1f}%</td>
      </tr></table>
    </td>
  </tr>
"""
            html += "  </table>\n"

        # This week's trades
        if pt["week_trades"]:
            html += f"""  <div style="font-size:11px;color:#64748b;text-transform:uppercase;letter-spacing:.05em;margin-bottom:6px;">Trades This Week ({len(pt['week_trades'])})</div>
  <table width="100%" cellpadding="0" cellspacing="0" style="margin-bottom:12px;">
"""
            for t in pt["week_trades"][:6]:
                t_colour = "#22c55e" if t["side"] == "SELL" else "#ef4444"
                html += f"""  <tr>
    <td style="padding:6px 10px;background:#1a2535;border-radius:6px;margin-bottom:4px;">
      <table width="100%"><tr>
        <td>
          <span style="background:{'rgba(34,197,94,.15)' if t['side']=='BUY' else 'rgba(239,68,68,.15)'};color:{'#22c55e' if t['side']=='BUY' else '#ef4444'};font-size:10px;font-weight:700;padding:2px 6px;border-radius:4px;">{t['side']}</span>
          <span style="font-size:12px;font-weight:600;color:#e2e8f0;margin-left:6px;">{t['qty']} × {t['ticker']}</span>
          <span style="font-size:11px;color:#94a3b8;margin-left:4px;">@ {_fmt(t['price_aud'])}</span>
        </td>
        <td align="right" style="font-size:12px;font-weight:600;color:{t_colour};white-space:nowrap;">{_fmt(t['total_aud'])}</td>
      </tr></table>
      {'<div style="font-size:11px;color:#64748b;margin-top:3px;padding-left:2px;">' + t["reason"] + '</div>' if t["reason"] else ''}
    </td>
  </tr>
  <tr><td style="height:4px;"></td></tr>
"""
            html += "  </table>\n"
        elif pt.get("total_trades_all_time", 0) == 0:
            html += """  <p style="font-size:12px;color:#64748b;font-style:italic;margin:0 0 8px;">No trades executed yet — click "Run Analysis" in the AI Trader tab to start.</p>\n"""
        else:
            html += """  <p style="font-size:12px;color:#64748b;font-style:italic;margin:0 0 8px;">No trades this week — portfolio held steady.</p>\n"""

        # Latest analysis excerpt
        if pt.get("latest_analysis_text"):
            safe_text = pt["latest_analysis_text"].replace("<", "&lt;").replace(">", "&gt;")
            html += f"""  <div style="font-size:11px;color:#64748b;text-transform:uppercase;letter-spacing:.05em;margin-bottom:6px;">Latest Analysis ({pt['latest_analysis_date']})</div>
  <div style="background:#1a2535;border-left:3px solid #818cf8;border-radius:0 8px 8px 0;padding:10px 14px;font-size:12px;color:#94a3b8;line-height:1.6;">{safe_text}{'…' if len(pt['latest_analysis_text']) >= 600 else ''}</div>
  <div style="margin-top:6px;text-align:right;"><a href="{base}#paper" style="font-size:11px;color:#818cf8;text-decoration:none;">View full analysis in app →</a></div>
"""

        html += "</td></tr>\n"

    # ── Chatbot prompt ──
    html += f"""
<tr><td style="background:#162032;padding:0 32px 20px;">
  <div style="background:#1e3a5f;border-radius:10px;padding:16px 20px;text-align:center;">
    <div style="font-size:20px;margin-bottom:6px;">💬</div>
    <p style="margin:0 0 10px;font-size:13px;color:#cbd5e1;line-height:1.5;">
      Have questions about your finances? The AI assistant can answer anything about your spending, goals, investments, or tax.
    </p>
    <a href="{base}#dashboard" style="display:inline-block;background:#38bdf8;color:#0f172a;font-size:13px;font-weight:700;padding:10px 24px;border-radius:99px;text-decoration:none;">Open Finance Tracker →</a>
  </div>
</td></tr>
"""

    # ── App Development Roadmap ──
    if data.get("roadmap"):
        html += f"""
<tr><td style="background:#0f1f35;padding:20px 32px 16px;">
  <h2 style="margin:0 0 6px;font-size:14px;font-weight:700;color:#e2e8f0;text-transform:uppercase;letter-spacing:.06em;">App Development Roadmap</h2>
  <p style="margin:0 0 14px;font-size:12px;color:#64748b;">Click any item to start a development conversation in the app's AI assistant.</p>
  <table width="100%" cellpadding="0" cellspacing="0">
"""
        priority_colours = {"high": "#ef4444", "medium": "#f59e0b", "low": "#22c55e"}
        for item in data["roadmap"]:
            pc = priority_colours.get(item["priority"], "#64748b")
            html += f"""  <tr>
    <td style="padding:10px 14px;background:#1e293b;border-radius:8px;margin-bottom:6px;">
      <table width="100%"><tr>
        <td>
          <div style="display:inline-block;background:{pc}22;color:{pc};font-size:10px;font-weight:700;padding:2px 8px;border-radius:99px;text-transform:uppercase;letter-spacing:.05em;margin-bottom:5px;">{item['priority']}</div>
          <div style="font-size:13px;font-weight:600;color:#e2e8f0;margin-bottom:3px;">{item['title']}</div>
          <div style="font-size:12px;color:#94a3b8;line-height:1.4;">{item['desc']}</div>
        </td>
        <td align="right" style="padding-left:12px;vertical-align:middle;">
          <a href="{base}#dashboard" style="display:block;font-size:11px;color:#a78bfa;text-decoration:none;border:1px solid #a78bfa;padding:5px 10px;border-radius:99px;white-space:nowrap;">Build it →</a>
        </td>
      </tr></table>
    </td>
  </tr>
  <tr><td style="height:6px;"></td></tr>
"""
        html += "  </table>\n</td></tr>\n"

    # ── Feature Suggestions ──
    if data.get("feature_suggestions"):
        html += f"""
<tr><td style="background:#0f1f35;padding:0 32px 20px;">
  <h2 style="margin:0 0 6px;font-size:14px;font-weight:700;color:#e2e8f0;text-transform:uppercase;letter-spacing:.06em;">Feature Ideas — Inspired by Leading Apps</h2>
  <p style="margin:0 0 14px;font-size:12px;color:#64748b;">Features from top finance apps worth adding. Click to discuss in the AI assistant.</p>
  <table width="100%" cellpadding="0" cellspacing="0">
"""
        for sug in data["feature_suggestions"]:
            html += f"""  <tr>
    <td style="padding:10px 14px;background:#1e293b;border-radius:8px;">
      <table width="100%"><tr>
        <td>
          <div style="font-size:10px;color:#38bdf8;font-weight:600;text-transform:uppercase;letter-spacing:.05em;margin-bottom:4px;">inspired by {sug['app']}</div>
          <div style="font-size:13px;font-weight:600;color:#e2e8f0;margin-bottom:3px;">{sug['feature']}</div>
          <div style="font-size:12px;color:#94a3b8;line-height:1.4;">{sug['desc']}</div>
        </td>
        <td align="right" style="padding-left:12px;vertical-align:middle;">
          <a href="{base}#dashboard" style="display:block;font-size:11px;color:#38bdf8;text-decoration:none;border:1px solid #38bdf8;padding:5px 10px;border-radius:99px;white-space:nowrap;">Discuss →</a>
        </td>
      </tr></table>
    </td>
  </tr>
  <tr><td style="height:6px;"></td></tr>
"""
        html += "  </table>\n</td></tr>\n"

    # ── Dev CTA ──
    html += f"""
<tr><td style="background:#0f1f35;padding:0 32px 20px;">
  <div style="background:#1a1035;border:1px solid #a78bfa44;border-radius:10px;padding:16px 20px;text-align:center;">
    <div style="font-size:20px;margin-bottom:6px;">🛠️</div>
    <p style="margin:0 0 10px;font-size:13px;color:#cbd5e1;line-height:1.5;">
      Want to start building a feature? Open the AI assistant in Finance Tracker and describe what you need — it can scope, plan, and implement new features.
    </p>
    <a href="{base}#dashboard" style="display:inline-block;background:#a78bfa;color:#0f172a;font-size:13px;font-weight:700;padding:10px 24px;border-radius:99px;text-decoration:none;">Open AI Assistant →</a>
  </div>
</td></tr>

<!-- Footer -->
<tr><td style="background:#0f172a;border-radius:0 0 16px 16px;padding:20px 32px;text-align:center;">
  <p style="margin:0;font-size:11px;color:#475569;">
    Finance Tracker &nbsp;·&nbsp; Generated {data['generated_at']} &nbsp;·&nbsp; Wollongong, NSW
  </p>
  <p style="margin:6px 0 0;font-size:11px;color:#334155;">
    <a href="{base}#dashboard" style="color:#38bdf8;text-decoration:none;">Open app</a>
    &nbsp;·&nbsp; <a href="{base}#transactions" style="color:#38bdf8;text-decoration:none;">Transactions</a>
    &nbsp;·&nbsp; <a href="{base}#bills" style="color:#38bdf8;text-decoration:none;">Bills</a>
    &nbsp;·&nbsp; <a href="{base}#budgets" style="color:#38bdf8;text-decoration:none;">Budget</a>
    &nbsp;·&nbsp; <a href="{base}#goals" style="color:#38bdf8;text-decoration:none;">Goals</a>
    &nbsp;·&nbsp; <a href="{base}#tax" style="color:#38bdf8;text-decoration:none;">Tax</a>
  </p>
</td></tr>

</table>
</td></tr>
</table>
</body>
</html>"""

    return html


# ---------------------------------------------------------------------------
# Endpoints
# ---------------------------------------------------------------------------

@router.get("/preview")
def preview_newsletter(
    session: Session = Depends(get_session),
    current_user: User = Depends(get_current_user),
):
    """Return newsletter data as JSON (no email sent)."""
    return _gather(session, current_user.id)


@router.post("/send")
async def send_newsletter(
    request: Request,
    session: Session = Depends(get_session),
    current_user: User = Depends(get_current_user),
):
    """Build and send the weekly newsletter email."""
    if get_setting(session, "ai_newsletter_enabled") == "0":
        raise HTTPException(403, "Newsletter AI is disabled. Enable it in Settings → AI Features.")
    gmail_address    = get_setting(session, "gmail_address", "")
    gmail_password   = get_setting(session, "gmail_app_password", "")
    newsletter_email = get_setting(session, "newsletter_email", "") or gmail_address
    app_url          = get_setting(session, "app_url", "")
    api_key          = get_setting(session, "anthropic_api_key", "") or \
                       __import__("os").environ.get("ANTHROPIC_API_KEY", "")

    # Auto-detect app URL if not configured in Settings.
    # Build from the forwarded Host header so Nabu Casa / reverse-proxy URLs work.
    if not app_url:
        fwd_host  = request.headers.get("X-Forwarded-Host", "")
        fwd_proto = request.headers.get("X-Forwarded-Proto", "http")
        if fwd_host:
            app_url = f"{fwd_proto}://{fwd_host}/api/finance_proxy/"
        else:
            # Fallback: local HA address
            app_url = "http://homeassistant.local:8123/api/finance_proxy/"

    if not gmail_address or not gmail_password:
        raise HTTPException(400, "Gmail address and app password must be configured in Settings.")
    if not newsletter_email:
        raise HTTPException(400, "Newsletter recipient email not configured in Settings.")

    data     = _gather(session, current_user.id)
    insights = await _generate_insights(data, api_key) if api_key else ""
    html     = _build_html(data, app_url, insights)

    subject = f"Finance Weekly: {data['week_label']} — Score {data['score']['score']}/100"
    if data["action_items"]["uncategorised"] > 0 or data["action_items"]["flagged"] > 0 or data["action_items"]["overdue_bills"]:
        subject += " ⚠"

    # Send via Gmail SMTP
    msg = MIMEMultipart("alternative")
    msg["Subject"] = subject
    msg["From"]    = f"Finance Tracker <{gmail_address}>"
    msg["To"]      = newsletter_email
    msg.attach(MIMEText(html, "html"))

    try:
        ctx = ssl.create_default_context()
        with smtplib.SMTP_SSL("smtp.gmail.com", 465, context=ctx) as smtp:
            smtp.login(gmail_address, gmail_password)
            smtp.sendmail(gmail_address, newsletter_email, msg.as_string())
    except smtplib.SMTPAuthenticationError:
        raise HTTPException(400, "Gmail authentication failed. Check your app password in Settings.")
    except Exception as e:
        raise HTTPException(500, f"Failed to send email: {e}")

    return {
        "ok": True,
        "to": newsletter_email,
        "subject": subject,
        "action_items": sum([
            data["action_items"]["uncategorised"] > 0,
            data["action_items"]["flagged"] > 0,
            len(data["action_items"]["overdue_bills"]) > 0,
            len(data["action_items"]["due_soon_bills"]) > 0,
        ]),
    }
