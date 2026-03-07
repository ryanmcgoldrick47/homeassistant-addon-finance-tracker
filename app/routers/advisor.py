"""Financial Advisor — AI-powered personalised financial recommendations (Australian context)."""
from __future__ import annotations

import json
from datetime import date, datetime
from typing import Optional

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel
from sqlmodel import Session, select

from database import (
    AdvisorSession, Category, Goal, Loan,
    NetWorthSnapshot, Payslip, Transaction, get_session, User,
)
from deps import get_current_user, get_setting, set_setting

router = APIRouter(prefix="/api/advisor", tags=["advisor"])


# ── Request schemas ────────────────────────────────────────────────────────

class GenerateRequest(BaseModel):
    user_context: Optional[str] = None


class ChatRequest(BaseModel):
    question: str
    session_id: Optional[int] = None


# ── Data gathering ─────────────────────────────────────────────────────────

def _gather_data(session: Session, user_id: int) -> dict:
    """Gather all financial data needed for the AI advisor prompt."""
    today = date.today()

    # ── 6-month spend/income trend ─────────────────────────────────────────
    txns = session.exec(
        select(Transaction)
        .where(Transaction.user_id == user_id, Transaction.is_reimbursable == False)
        .order_by(Transaction.date.desc())
    ).all()

    monthly: dict[str, dict] = {}
    for t in txns:
        key = t.date.strftime("%Y-%m")
        if key not in monthly:
            monthly[key] = {"income": 0.0, "spend": 0.0, "categories": {}}
        if t.is_credit:
            monthly[key]["income"] += t.amount
        else:
            monthly[key]["spend"] += t.amount
            cat_id = str(t.category_id or "Uncategorised")
            monthly[key]["categories"][cat_id] = monthly[key]["categories"].get(cat_id, 0) + t.amount

    # Keep last 6 months
    trend = [(k, monthly[k]) for k in sorted(monthly.keys(), reverse=True)[:6]]
    trend.reverse()

    # Category names (no user_id on Category model — shared across users)
    categories = {str(c.id): c.name for c in session.exec(select(Category)).all()}

    # ── Loans ──────────────────────────────────────────────────────────────
    loans = session.exec(
        select(Loan).where(Loan.user_id == user_id, Loan.is_active == True)
    ).all()

    # ── Net worth ──────────────────────────────────────────────────────────
    nw = session.exec(
        select(NetWorthSnapshot).where(NetWorthSnapshot.user_id == user_id)
        .order_by(NetWorthSnapshot.snapshot_date.desc())
    ).first()

    # ── Payslips (last 12 for income context) ─────────────────────────────
    payslips = session.exec(
        select(Payslip).where(Payslip.user_id == user_id)
        .order_by(Payslip.pay_date.desc())
        .limit(12)
    ).all()

    # ── Goals ─────────────────────────────────────────────────────────────
    goals = session.exec(
        select(Goal).where(Goal.user_id == user_id)
    ).all()

    return {
        "today": today.isoformat(),
        "trend": trend,
        "categories": categories,
        "loans": loans,
        "net_worth": nw,
        "payslips": payslips,
        "goals": goals,
    }


def _build_prompt(data: dict, rba_rate: float, market_summary: str, user_context: str = "") -> str:
    today = data["today"]
    trend = data["trend"]
    categories = data["categories"]
    loans = data["loans"]
    nw = data["net_worth"]
    payslips = data["payslips"]
    goals = data["goals"]

    # Build trend section
    trend_lines = []
    for key, m in trend:
        trend_lines.append(
            f"  {key}: Income ${m['income']:,.0f} | Spend ${m['spend']:,.0f} | "
            f"Net ${m['income'] - m['spend']:+,.0f}"
        )
    # Top categories (average across months)
    cat_totals: dict[str, float] = {}
    for _, m in trend:
        for cid, amt in m["categories"].items():
            cat_totals[cid] = cat_totals.get(cid, 0) + amt
    months_count = max(len(trend), 1)
    top_cats = sorted(cat_totals.items(), key=lambda x: x[1], reverse=True)[:8]
    cat_lines = [
        f"  {categories.get(cid, cid)}: ${amt/months_count:,.0f}/month avg"
        for cid, amt in top_cats
    ]

    # Savings rate (average over trend)
    avg_income = sum(m["income"] for _, m in trend) / months_count
    avg_spend = sum(m["spend"] for _, m in trend) / months_count
    avg_savings_rate = ((avg_income - avg_spend) / avg_income * 100) if avg_income > 0 else 0

    # Payslip income — summarise by employer (may have multiple)
    if payslips:
        employer_map: dict[str, dict] = {}
        for p in payslips:
            emp = p.employer or "Unknown employer"
            if emp not in employer_map:
                employer_map[emp] = {"count": 0, "total_gross": 0.0, "latest_date": str(p.pay_date)}
            employer_map[emp]["count"] += 1
            employer_map[emp]["total_gross"] += (p.gross_pay_cents or 0) / 100
        emp_lines = []
        for emp, info in employer_map.items():
            avg = info["total_gross"] / info["count"]
            emp_lines.append(f"  {emp}: {info['count']} payslips, avg ${avg:,.2f}/payslip, latest {info['latest_date']}")
        # Latest payslip detail
        latest = payslips[0]
        gross_pay = (latest.gross_pay_cents or 0) / 100
        net_pay   = (latest.net_pay_cents  or 0) / 100
        ytd_gross = (latest.ytd_gross_cents or 0) / 100
        gross_line = (
            f"Payslips on file: {len(payslips)} (last 12 months)\n"
            + "\n".join(emp_lines)
            + f"\nMost recent payslip ({latest.pay_date}): ${gross_pay:,.2f} gross, ${net_pay:,.2f} net | YTD gross: ${ytd_gross:,.2f}"
        )
    else:
        gross_line = "No payslip data on file."

    # Loans section
    from routers.loans import _amortise, _loan_summary
    loan_lines = []
    for l in loans:
        sched = _amortise(l.outstanding_cents, l.interest_rate, l.term_months,
                          l.monthly_repayment_cents, l.offset_cents)
        summary = _loan_summary(l, sched)
        effective_balance = max(0, l.outstanding_cents - l.offset_cents) / 100
        monthly_rate = l.interest_rate / 100 / 12
        monthly_interest_cost = effective_balance * monthly_rate
        loan_lines.append(
            f"  {l.name} ({l.loan_type}): ${l.outstanding_cents/100:,.0f} outstanding @ {l.interest_rate}% p.a.\n"
            f"    Monthly repayment: ${l.monthly_repayment_cents/100:,.0f} | Offset: ${l.offset_cents/100:,.0f}\n"
            f"    Effective balance (after offset): ${effective_balance:,.0f}\n"
            f"    Monthly interest cost: ~${monthly_interest_cost:,.0f}\n"
            f"    Payoff date: {summary['payoff_date'] or 'N/A'}\n"
            f"    Total interest remaining (est.): ${summary['total_interest_remaining']:,.0f}"
        )

    # Net worth
    if nw:
        nw_line = (
            f"${nw.net_worth:,.0f} (as of {nw.snapshot_date}) — "
            f"Assets: ${nw.total_assets:,.0f} | Liabilities: ${nw.total_liabilities:,.0f}"
        )
    else:
        nw_line = "No snapshot recorded yet"

    # Goals
    goal_lines = []
    for g in goals:
        pct = (g.current_cents / g.target_cents * 100) if g.target_cents > 0 else 0
        goal_lines.append(f"  {g.name}: ${g.current_cents/100:,.0f} / ${g.target_cents/100:,.0f} ({pct:.0f}%)")

    user_context_section = f"""
## USER-PROVIDED CONTEXT & CLARIFICATIONS
The user has provided the following notes to help you interpret their data correctly:
{user_context.strip()}
""" if user_context and user_context.strip() else ""

    prompt = f"""You are a qualified Australian financial planner providing general financial advice (not personal financial product advice). Today is {today}. The user lives in Wollongong, NSW, Australia.
{user_context_section}
## IMPORTANT DATA QUALITY NOTES
Before analysing, be aware of these common data limitations:
- **Transfers between accounts** (e.g. paying offset, moving savings) appear as both income AND spending in transaction data. Do NOT treat high "transfers" or "account-to-account" categories as discretionary spending or as evidence of a deficit — they are likely internal movements.
- **Income in transactions may be incomplete** — the user may have multiple income sources not fully captured. Use payslip data as the primary income reference where available.
- **Payslip data shows all employers** — do not assume reduced employment from a single recent payslip if multiple employers are shown.
- **Make conservative, charitable assumptions** — if data is ambiguous, note the ambiguity and ask the user to clarify rather than drawing negative conclusions.

## USER'S FINANCIAL DATA

### Income & Spending (6-month trend)
{chr(10).join(trend_lines) if trend_lines else "  No transaction data available."}

Average monthly income (transactions): ${avg_income:,.0f}
Average monthly spending (transactions, may include transfers): ${avg_spend:,.0f}
Note: figures above include all transaction categories — internal transfers may inflate both income and spending.

### Top Spending Categories (monthly average)
{chr(10).join(cat_lines) if cat_lines else "  No category data."}

### Employment / Payslip Income
{gross_line}

### Mortgage & Loans
{chr(10).join(loan_lines) if loan_lines else "  No loans recorded."}

### Net Worth
{nw_line}

### Financial Goals
{chr(10).join(goal_lines) if goal_lines else "  No goals set."}

## CURRENT MARKET CONTEXT (Australia)

RBA Cash Rate: {rba_rate:.2f}% p.a.
{market_summary}

---

## YOUR TASK

Provide a comprehensive, personalised financial review. Use the payslip data for income analysis (more reliable than raw transaction totals). Flag any data ambiguities rather than making assumptions. Use Australian terminology and AUD throughout.

## Executive Summary
2–3 sentences on their overall financial position based on reliable data. Note any key data gaps.

## Priority Recommendations
3–5 numbered recommendations. For each:
- **What**: The specific action
- **Why**: Reasoning using their actual numbers
- **Impact**: Estimated AUD benefit/year or over the loan term

## Offset Account vs Extra Repayments vs Investing
Quantified comparison of these three strategies given their mortgage rate ({', '.join(f'{l.interest_rate}%' for l in loans) or 'N/A'}), offset balance, and payslip income. Which gives the best risk-adjusted return right now?

## Budget & Spending Insights
Based on non-transfer categories only — which are healthy, which need attention?

## 12-Month Outlook
Current trajectory vs following your recommendations: net worth change, loan balance, estimated savings.

---
General advice only — recommend they consult a licensed adviser for personal product advice.
"""
    return prompt


# ── AI helper ──────────────────────────────────────────────────────────────

def _call_ai(session: Session, prompt: str) -> str:
    """Call configured AI provider and return response text."""
    provider = get_setting(session, "ai_provider", "gemini")
    if provider == "gemini":
        api_key = get_setting(session, "gemini_api_key", "")
        if not api_key:
            raise HTTPException(400, "Gemini API key not set. Configure it in Settings → AI Provider.")
        try:
            from google import genai as google_genai
            client = google_genai.Client(api_key=api_key)
            response = client.models.generate_content(
                model="gemini-2.5-flash",
                contents=prompt,
            )
            return response.text
        except Exception as e:
            raise HTTPException(500, f"Gemini error: {e}")
    else:
        api_key = get_setting(session, "anthropic_api_key", "")
        if not api_key:
            raise HTTPException(400, "Anthropic API key not set. Configure it in Settings → AI Provider.")
        try:
            import anthropic
            client = anthropic.Anthropic(api_key=api_key)
            response = client.messages.create(
                model="claude-haiku-4-5-20251001",
                max_tokens=4096,
                messages=[{"role": "user", "content": prompt}],
            )
            return response.content[0].text
        except Exception as e:
            raise HTTPException(500, f"Anthropic error: {e}")


# ── Endpoints ──────────────────────────────────────────────────────────────

@router.get("/report")
def get_report(
    session: Session = Depends(get_session),
    current_user: User = Depends(get_current_user),
):
    """Return the most recent advisor session (report + chat messages)."""
    adv = session.exec(
        select(AdvisorSession)
        .where(AdvisorSession.user_id == current_user.id)
        .order_by(AdvisorSession.created_at.desc())
    ).first()
    if not adv:
        return {"report": None, "generated_at": None, "session_id": None, "chat_messages": []}
    return {
        "report": adv.report_text,
        "generated_at": adv.created_at.isoformat(),
        "session_id": adv.id,
        "chat_messages": json.loads(adv.chat_messages or "[]"),
        "user_context": adv.user_context or "",
    }


@router.post("/generate")
async def generate_report(
    req: GenerateRequest,
    session: Session = Depends(get_session),
    current_user: User = Depends(get_current_user),
):
    """Generate a fresh AI-powered financial advisory report."""
    import urllib.request

    # Gather financial data
    data = _gather_data(session, current_user.id)

    # Get RBA cash rate
    rba_rate = 4.35  # default
    try:
        url = "https://data.api.rba.gov.au/v1/series/FIRMMCRTD/data/last/1/?format=json"
        with urllib.request.urlopen(url, timeout=5) as resp:
            rba_data = json.loads(resp.read())
            rba_rate = float(rba_data["dataSets"][0]["series"]["0"]["observations"]["0"][0])
    except Exception:
        pass

    # Get market summary
    market_summary = ""
    try:
        from routers.market_pulse import _fetch_rba_data
        rba_info = _fetch_rba_data()
        market_summary = f"Last RBA change: {rba_info.get('last_change_bps', 0):+d} bps on {rba_info.get('last_change_date', 'N/A')}"
    except Exception:
        market_summary = f"RBA cash rate is {rba_rate:.2f}% as of {date.today().year}"

    # Build prompt and call AI
    prompt = _build_prompt(data, rba_rate, market_summary, req.user_context or "")
    report_text = _call_ai(session, prompt)

    # Save as a new AdvisorSession
    adv = AdvisorSession(
        user_id=current_user.id,
        created_at=datetime.utcnow(),
        report_text=report_text,
        user_context=req.user_context or None,
        chat_messages="[]",
    )
    session.add(adv)
    session.commit()
    session.refresh(adv)

    # Also cache in settings for backwards compat
    set_setting(session, f"advisor_report_{current_user.id}", report_text)
    set_setting(session, f"advisor_report_date_{current_user.id}", adv.created_at.isoformat())

    return {
        "report": report_text,
        "generated_at": adv.created_at.isoformat(),
        "session_id": adv.id,
        "chat_messages": [],
    }


@router.post("/chat")
async def advisor_chat(
    req: ChatRequest,
    session: Session = Depends(get_session),
    current_user: User = Depends(get_current_user),
):
    """Ask a follow-up question about the advisor report."""
    # Load the target session
    if req.session_id:
        adv = session.get(AdvisorSession, req.session_id)
        if not adv or adv.user_id != current_user.id:
            raise HTTPException(404, "Session not found")
    else:
        adv = session.exec(
            select(AdvisorSession)
            .where(AdvisorSession.user_id == current_user.id)
            .order_by(AdvisorSession.created_at.desc())
        ).first()

    if not adv:
        raise HTTPException(400, "No advisor report found. Generate a report first.")

    prior_chat = json.loads(adv.chat_messages or "[]")

    # Build prompt incorporating the report and prior chat
    chat_context = "\n".join(
        f"{m['role'].upper()}: {m['content']}" for m in prior_chat[-10:]  # last 10 messages
    ) if prior_chat else "No prior questions."

    full_prompt = f"""You are an Australian financial advisor assistant. The user previously received a personalised financial advisory report. Answer their follow-up question concisely but thoroughly. Use Australian terminology and AUD throughout. Keep answers focused and practical.

## FINANCIAL ADVISORY REPORT (context)
{adv.report_text}

## PRIOR CONVERSATION
{chat_context}

## USER'S QUESTION
{req.question}

Answer the question directly. If referencing numbers from the report, be specific. Keep your response under 400 words unless the question requires more detail."""

    answer = _call_ai(session, full_prompt)

    # Append Q&A to chat history
    now = datetime.utcnow().isoformat()
    prior_chat.append({"role": "user", "content": req.question, "ts": now})
    prior_chat.append({"role": "assistant", "content": answer, "ts": now})
    adv.chat_messages = json.dumps(prior_chat)
    session.add(adv)
    session.commit()

    return {"answer": answer, "session_id": adv.id, "chat_messages": prior_chat}


@router.get("/history")
def get_history(
    session: Session = Depends(get_session),
    current_user: User = Depends(get_current_user),
):
    """Return list of past advisor sessions (newest first)."""
    sessions = session.exec(
        select(AdvisorSession)
        .where(AdvisorSession.user_id == current_user.id)
        .order_by(AdvisorSession.created_at.desc())
        .limit(20)
    ).all()
    return [
        {
            "id": s.id,
            "created_at": s.created_at.isoformat(),
            "preview": (s.report_text or "")[:250].replace("\n", " "),
            "has_chat": bool(s.chat_messages and s.chat_messages not in ("[]", "")),
            "user_context": s.user_context or "",
        }
        for s in sessions
    ]


@router.get("/history/{session_id}")
def get_history_session(
    session_id: int,
    session: Session = Depends(get_session),
    current_user: User = Depends(get_current_user),
):
    """Return a specific past advisor session in full."""
    adv = session.get(AdvisorSession, session_id)
    if not adv or adv.user_id != current_user.id:
        raise HTTPException(404, "Session not found")
    return {
        "id": adv.id,
        "created_at": adv.created_at.isoformat(),
        "report": adv.report_text,
        "user_context": adv.user_context or "",
        "chat_messages": json.loads(adv.chat_messages or "[]"),
    }
