from __future__ import annotations

import json
import os
from datetime import date, timedelta
from calendar import month_name, month_abbr

from fastapi import APIRouter, Depends, HTTPException
from sqlmodel import Session, select, func

from database import Transaction, Category, Budget, Bill, Setting, get_session
from deps import get_setting

router = APIRouter(prefix="/api/insights", tags=["insights"])


# ---------------------------------------------------------------------------
# Data gathering
# ---------------------------------------------------------------------------

def _month_totals(session: Session, m: int, y: int):
    """Return (spend, income) for a given month/year."""
    def _sum(is_credit):
        return float(session.exec(
            select(func.coalesce(func.sum(Transaction.amount), 0)).where(
                Transaction.is_credit == is_credit,
                func.strftime("%m", Transaction.date) == f"{m:02d}",
                func.strftime("%Y", Transaction.date) == str(y),
            )
        ).one())
    return _sum(False), _sum(True)


def _spend_by_category(session: Session, m: int, y: int) -> dict[str, float]:
    txns = session.exec(
        select(Transaction).where(
            Transaction.is_credit == False,
            func.strftime("%m", Transaction.date) == f"{m:02d}",
            func.strftime("%Y", Transaction.date) == str(y),
        )
    ).all()
    result: dict[str, float] = {}
    for t in txns:
        cat = session.get(Category, t.category_id) if t.category_id else None
        name = cat.name if cat else "Uncategorised"
        result[name] = round(result.get(name, 0) + t.amount, 2)
    return result


def _build_context(session: Session) -> str:
    today = date.today()

    # Collect last 3 full months + current month
    months = []
    for i in range(3, -1, -1):
        m = today.month - i
        y = today.year
        if m <= 0:
            m += 12
            y -= 1
        months.append((m, y))

    monthly_rows = []
    for m, y in months:
        spend, income = _month_totals(session, m, y)
        net = income - spend
        savings_rate = round(net / income * 100, 1) if income > 0 else 0
        label = f"{month_abbr[m]} {y}"
        monthly_rows.append(
            f"  {label}: spend=${spend:,.2f}, income=${income:,.2f}, "
            f"net={'+'if net>=0 else ''}{net:,.2f}, savings_rate={savings_rate}%"
        )

    # Current month detail
    cm, cy = today.month, today.year
    cur_spend, cur_income = _month_totals(session, cm, cy)
    cur_by_cat = _spend_by_category(session, cm, cy)

    # Previous month for comparison
    pm = cm - 1 or 12
    py = cy if cm > 1 else cy - 1
    prev_by_cat = _spend_by_category(session, pm, py)

    # Category breakdown with MoM delta
    cat_lines = []
    all_cats = sorted(set(list(cur_by_cat.keys()) + list(prev_by_cat.keys())),
                      key=lambda c: cur_by_cat.get(c, 0), reverse=True)
    for cat in all_cats[:15]:
        cur_v = cur_by_cat.get(cat, 0)
        prev_v = prev_by_cat.get(cat, 0)
        if cur_v == 0 and prev_v == 0:
            continue
        delta = ""
        if prev_v > 0:
            pct = round((cur_v - prev_v) / prev_v * 100, 1)
            delta = f" (prev ${prev_v:,.2f}, {'+' if pct >= 0 else ''}{pct}% MoM)"
        cat_lines.append(f"  {cat}: ${cur_v:,.2f}{delta}")

    # Budgets
    budgets = session.exec(select(Budget)).all()
    budget_lines = []
    for b in budgets:
        cat = session.get(Category, b.category_id) if b.category_id else None
        cat_name = cat.name if cat else "?"
        budget_amt = b.amount_cents / 100
        actual = cur_by_cat.get(cat_name, 0)
        pct = round(actual / budget_amt * 100) if budget_amt > 0 else 0
        status = "OVER" if pct > 100 else ("AT RISK" if pct > 80 else "OK")
        budget_lines.append(
            f"  {cat_name}: ${actual:,.2f} / ${budget_amt:,.2f} ({pct}% — {status})"
        )

    # Bills
    bills = session.exec(select(Bill).where(Bill.is_active == True)).all()
    monthly_bill_total = 0
    bill_lines = []
    for b in bills:
        amt = b.amount_cents / 100
        # Normalise to monthly equivalent
        freq_map = {"weekly": 52/12, "fortnightly": 26/12, "monthly": 1,
                    "quarterly": 1/3, "annual": 1/12}
        monthly_bill_total += amt * freq_map.get(b.frequency, 1)
        due_str = f"next due {b.next_due}" if b.next_due else "no due date"
        bill_lines.append(f"  {b.name}: ${amt:,.2f} ({b.frequency}, {due_str})")

    # Top transactions this month
    top_txns = session.exec(
        select(Transaction).where(
            Transaction.is_credit == False,
            func.strftime("%m", Transaction.date) == f"{cm:02d}",
            func.strftime("%Y", Transaction.date) == str(cy),
        ).order_by(Transaction.amount.desc()).limit(8)
    ).all()
    top_lines = []
    for t in top_txns:
        cat = session.get(Category, t.category_id) if t.category_id else None
        top_lines.append(
            f"  ${t.amount:,.2f} — {t.description} ({cat.name if cat else 'Uncategorised'}, {t.date})"
        )

    # Uncategorised count
    uncat = session.exec(select(Category).where(Category.name == "Uncategorised")).first()
    uncat_count = 0
    if uncat:
        uncat_count = session.exec(
            select(func.count()).where(Transaction.category_id == uncat.id)
        ).one()

    return f"""== FINANCIAL SNAPSHOT ==
Location: Wollongong, NSW, Australia | Currency: AUD
Report date: {today}

MONTHLY SUMMARY (last 4 months):
{chr(10).join(monthly_rows)}

CURRENT MONTH ({month_name[cm]} {cy}) SPEND BY CATEGORY:
{chr(10).join(cat_lines) if cat_lines else '  No transactions yet'}

BUDGET STATUS ({month_name[cm]} {cy}):
{chr(10).join(budget_lines) if budget_lines else '  No budgets configured'}

RECURRING BILLS (monthly equivalent total: ${monthly_bill_total:,.2f}/month):
{chr(10).join(bill_lines) if bill_lines else '  No bills configured'}

TOP TRANSACTIONS THIS MONTH:
{chr(10).join(top_lines) if top_lines else '  No transactions'}

UNCATEGORISED TRANSACTIONS: {uncat_count}
"""


def _build_prompt(context: str) -> str:
    return f"""You are a friendly, encouraging personal finance advisor for an Australian household.

Analyse the financial data below and return a JSON object with insightful, specific, and constructive observations.

Rules:
- Be warm, specific, and use the actual numbers from the data
- Celebrate wins genuinely — don't manufacture positives that aren't there
- Frame concerns constructively: "worth watching" not "you're failing"
- Give actionable Australian-relevant suggestions where appropriate
- Reference specific categories, amounts, and trends by name
- Do not be generic — every insight must reference specific data points

Return ONLY this JSON structure, no markdown, no preamble:
{{
  "summary": "2-3 sentence overall financial health summary for the period, specific to their data",
  "score": <integer 0-100>,
  "score_label": "<one of: Needs Work | Getting There | On Track | Doing Well | Excellent>",
  "insights": [
    {{
      "type": "<one of: win | heads_up | tip | pattern>",
      "icon": "<single emoji: 🎉 💡 ⚠️ 📊 🎯 💰 🔄 📈 🛒 🍽️ 💳 🏠 🚗>",
      "title": "<concise title, max 7 words>",
      "body": "<2-3 sentences, specific and actionable, references actual amounts>",
      "metric": "<optional: key stat to highlight, e.g. '$234 under budget' or null>"
    }}
  ]
}}

Provide 5-8 insights. Mix of types — at least 2 wins, at least 1 tip. Score reflects overall financial health (savings rate, budget adherence, bill coverage, categorisation completeness).

{context}"""


# ---------------------------------------------------------------------------
# AI call helpers (reuse provider logic from ai.py)
# ---------------------------------------------------------------------------

async def _call_ai(prompt: str, session: Session) -> str:
    provider = get_setting(session, "ai_provider") or "gemini"

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
        return response.text
    else:
        api_key = get_setting(session, "anthropic_api_key") or os.environ.get("ANTHROPIC_API_KEY", "")
        if not api_key:
            raise HTTPException(400, "Anthropic API key not configured.")
        import anthropic
        client = anthropic.Anthropic(api_key=api_key)
        msg = client.messages.create(
            model="claude-sonnet-4-6",
            max_tokens=3000,
            messages=[{"role": "user", "content": prompt}],
        )
        return msg.content[0].text


def _parse_json(raw: str) -> dict:
    raw = raw.strip()
    if raw.startswith("```"):
        raw = raw.split("```")[1]
        if raw.startswith("json"):
            raw = raw[4:]
    return json.loads(raw.strip())


# ---------------------------------------------------------------------------
# Endpoints
# ---------------------------------------------------------------------------

@router.get("")
def get_insights(session: Session = Depends(get_session)):
    """Return cached insights if available."""
    cached = get_setting(session, "insights_cache")
    generated_at = get_setting(session, "insights_generated_at")
    if cached:
        try:
            return {"data": json.loads(cached), "generated_at": generated_at, "cached": True}
        except Exception:
            pass
    return {"data": None, "generated_at": None, "cached": False}


@router.post("/generate")
async def generate_insights(session: Session = Depends(get_session)):
    """Generate fresh AI insights and cache them."""
    context = _build_context(session)
    prompt = _build_prompt(context)

    try:
        raw = await _call_ai(prompt, session)
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(500, f"AI error: {e}")

    try:
        data = _parse_json(raw)
    except Exception:
        # Try to extract JSON from response
        try:
            start = raw.index("{")
            end = raw.rindex("}") + 1
            data = json.loads(raw[start:end])
        except Exception:
            raise HTTPException(500, f"Could not parse AI response: {raw[:300]}")

    # Cache in settings
    from datetime import datetime
    now = datetime.now().isoformat(timespec="seconds")
    for key, val in [("insights_cache", json.dumps(data)), ("insights_generated_at", now)]:
        s = session.get(Setting, key)
        if s:
            s.value = val
        else:
            s = Setting(key=key, value=val)
        session.add(s)
    session.commit()

    return {"data": data, "generated_at": now, "cached": False}
