from __future__ import annotations

"""
AI chat assistant for Finance Tracker.
Uses Claude with tool_use to answer natural-language questions about financial data.
"""

import json
import re
import sqlite3
from datetime import date
from typing import Optional

import anthropic
from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel
from sqlmodel import Session

from sqlmodel import select
from database import get_session, engine, User, ChatHistory, ChatConversation
from deps import get_setting, get_current_user

router = APIRouter()

MODEL = "claude-haiku-4-5-20251001"   # fast + cheap for conversational queries
MAX_TOOL_ROUNDS = 6                    # prevent infinite loops
SQL_ROW_LIMIT = 200


# ---------------------------------------------------------------------------
# System prompt
# ---------------------------------------------------------------------------

def _system_prompt() -> str:
    today = date.today().strftime("%-d %B %Y")
    return f"""You are a personal finance assistant for an Australian household based in Wollongong, NSW.
Today's date is {today}. Australian financial year runs 1 July – 30 June.

CRITICAL RULE: You have FULL direct database access. NEVER say you cannot read transactions, categories, budgets, or any other data. ALWAYS call a tool to fetch the data you need before answering. If a pre-built tool doesn't cover the exact query, use run_sql.

You have full read access to the user's financial data via these tools:
- get_dashboard: monthly income/spend/net savings + top transactions + spend by category
- get_budget_vs_spend: existing budget limits vs actual spend per category for a month
- get_categories: all transaction categories with their IDs
- get_goals: savings goals and progress
- get_score: monthly finance score breakdown
- get_investments: share + crypto portfolio
- get_net_worth: net worth history
- get_tax_summary: deductibles, CGT, dividends for a financial year
- run_sql: ANY custom SELECT query against the database (use freely for analysis)

KEY DATABASE SCHEMA (use with run_sql):
- transaction(id, user_id, date, description, amount, is_credit, category_id, tax_deductible, merchant)
- category(id, user_id, name, color, is_income, is_system)
- budget(id, user_id, category_id, month, year, amount_cents)
- bill(id, user_id, name, amount_cents, frequency, next_due, is_active)
- shareholding(id, user_id, ticker, name, qty, avg_cost_aud, price_aud, value_aud, gain_aud)
- cryptoholding(id, user_id, symbol, qty, price_aud, value_aud)
- networthsnapshot(id, user_id, snapshot_date, total_assets, total_liabilities, net_worth)
- supersnapshot(id, user_id, snapshot_date, balance_aud, fund_name)
- payslip(id, user_id, pay_date, gross_income, tax_withheld, net_pay, super_amount)
- goal(id, user_id, name, target_cents, current_cents, target_date, is_complete, goal_type)

ALWAYS filter by user_id=:uid. For date filtering: strftime('%m', date)=:m, strftime('%Y', date)=:y.

WORKFLOW RULES:
1. ALWAYS fetch data with tools before answering numerical questions — never guess or say you don't have access.
2. For budget suggestions: first call get_categories to see existing categories, then run_sql to get 3-month average spend per category, then get_dashboard for income. Build recommendations from actual data.
3. For questions about spending patterns: run_sql against transaction table — you can query any date range.
4. For mortgage/insurance/bills: check the bill table and transaction categories — don't ask the user for data you can query.
5. Chain multiple tool calls as needed.

Guidelines:
- Be concise and specific. Lead with the key number or answer.
- Format amounts as $X,XXX (round to dollars for summaries, show cents for exact figures).
- When asked about "this month" use the current calendar month.
- Use ATO/Australian terminology: superannuation, franking credits, CGT discount, PAYG withholding, etc.
- Keep replies to 3–5 lines unless a detailed breakdown is requested.
- For budget suggestions: compare the user's actual 3-month averages to ABS 2019-20 HES weekly benchmarks (converted to monthly).
"""


# ---------------------------------------------------------------------------
# Tool definitions
# ---------------------------------------------------------------------------

TOOLS = [
    {
        "name": "get_dashboard",
        "description": (
            "Get income, spending, net savings, top transactions, and category breakdown "
            "for a given month. Use this for questions about monthly spending, income, savings rate."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "month": {"type": "integer", "description": "1–12, defaults to current month"},
                "year":  {"type": "integer", "description": "4-digit year, defaults to current year"},
            },
        },
    },
    {
        "name": "get_budget_vs_spend",
        "description": "Get budget vs actual spend per category for a month, with over/under status.",
        "input_schema": {
            "type": "object",
            "properties": {
                "month": {"type": "integer"},
                "year":  {"type": "integer"},
            },
        },
    },
    {
        "name": "get_categories",
        "description": "Get all transaction categories with their IDs, names, and whether they are income or expense categories. Use this first when building budget suggestions or analysing spending by category.",
        "input_schema": {"type": "object", "properties": {}},
    },
    {
        "name": "get_goals",
        "description": "Get all savings goals with progress (current vs target), contributions, and completion status.",
        "input_schema": {"type": "object", "properties": {}},
    },
    {
        "name": "get_score",
        "description": (
            "Get the Finance Score (0–100) with breakdown: savings rate score, budget adherence score, "
            "categorisation score, bills-on-time score."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "month": {"type": "integer"},
                "year":  {"type": "integer"},
            },
        },
    },
    {
        "name": "get_investments",
        "description": "Get share holdings, crypto holdings, portfolio total value, cost basis, and unrealised gain/loss.",
        "input_schema": {"type": "object", "properties": {}},
    },
    {
        "name": "get_net_worth",
        "description": "Get latest net worth snapshot (assets, liabilities, net worth) and recent history.",
        "input_schema": {"type": "object", "properties": {}},
    },
    {
        "name": "get_tax_summary",
        "description": (
            "Get tax deductible transactions, CGT summary (gains/discount/net), "
            "and dividend/franking credit totals for a financial year."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "fy": {"type": "integer", "description": "FY end year, e.g. 2025 for FY 2024-25. Defaults to current FY."},
            },
        },
    },
    {
        "name": "run_sql",
        "description": (
            "Execute a read-only SQL SELECT query against the finance database for custom analysis. "
            "Available tables: transaction, category, account, budget, bill, billpayment, "
            "goal, goalcontribution, cryptoholding, shareholding, networthsnapshot, "
            "supersnapshot, supercontribution, payslip, acquisitionlot, disposal, dividend, "
            "challenge, achievement. "
            "IMPORTANT: Always filter by user_id=:uid in every query. "
            "Use strftime('%m', date) for month, strftime('%Y', date) for year filtering. "
            "Limit results to avoid large payloads."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "query": {"type": "string", "description": "SQL SELECT statement (read-only). Must include user_id=:uid filter."},
            },
            "required": ["query"],
        },
    },
]


# ---------------------------------------------------------------------------
# Tool execution
# ---------------------------------------------------------------------------

def _current_fy() -> int:
    today = date.today()
    return today.year if today.month >= 7 else today.year - 1


def _run_tool(name: str, inputs: dict, user_id: int) -> str:
    """Execute a tool and return a JSON string result."""
    try:
        if name == "get_dashboard":
            return _tool_dashboard(inputs, user_id)
        elif name == "get_budget_vs_spend":
            return _tool_budget(inputs, user_id)
        elif name == "get_categories":
            return _tool_categories(user_id)
        elif name == "get_goals":
            return _tool_goals(user_id)
        elif name == "get_score":
            return _tool_score(inputs, user_id)
        elif name == "get_investments":
            return _tool_investments(user_id)
        elif name == "get_net_worth":
            return _tool_networth(user_id)
        elif name == "get_tax_summary":
            return _tool_tax(inputs, user_id)
        elif name == "run_sql":
            return _tool_sql(inputs.get("query", ""), user_id)
        else:
            return json.dumps({"error": f"Unknown tool: {name}"})
    except Exception as e:
        return json.dumps({"error": str(e)})


def _db_query(sql: str, params: dict = {}) -> list[dict]:
    """Run a read-only query using the SQLAlchemy engine's underlying connection."""
    with engine.connect() as conn:
        result = conn.execute(__import__("sqlalchemy").text(sql), params)
        cols = list(result.keys())
        return [dict(zip(cols, row)) for row in result.fetchall()]


def _tool_dashboard(inputs: dict, user_id: int) -> str:
    today = date.today()
    m = inputs.get("month") or today.month
    y = inputs.get("year") or today.year
    mm = f"{m:02d}"

    income = _db_query(
        "SELECT COALESCE(SUM(amount),0) as total FROM transaction "
        "WHERE user_id=:uid AND is_credit=1 AND strftime('%m',date)=:m AND strftime('%Y',date)=:y",
        {"uid": user_id, "m": mm, "y": str(y)}
    )[0]["total"]

    spend = _db_query(
        "SELECT COALESCE(SUM(amount),0) as total FROM transaction "
        "WHERE user_id=:uid AND is_credit=0 AND strftime('%m',date)=:m AND strftime('%Y',date)=:y",
        {"uid": user_id, "m": mm, "y": str(y)}
    )[0]["total"]

    by_cat = _db_query(
        "SELECT c.name, ROUND(SUM(t.amount),2) as total "
        "FROM transaction t LEFT JOIN category c ON t.category_id=c.id "
        "WHERE t.user_id=:uid AND t.is_credit=0 AND strftime('%m',t.date)=:m AND strftime('%Y',t.date)=:y "
        "GROUP BY c.name ORDER BY total DESC LIMIT 10",
        {"uid": user_id, "m": mm, "y": str(y)}
    )

    top_txns = _db_query(
        "SELECT t.date, t.description, t.amount, c.name as category "
        "FROM transaction t LEFT JOIN category c ON t.category_id=c.id "
        "WHERE t.user_id=:uid AND t.is_credit=0 AND strftime('%m',t.date)=:m AND strftime('%Y',t.date)=:y "
        "ORDER BY t.amount DESC LIMIT 5",
        {"uid": user_id, "m": mm, "y": str(y)}
    )

    return json.dumps({
        "month": f"{y}-{mm}",
        "income": round(float(income), 2),
        "spend": round(float(spend), 2),
        "net": round(float(income) - float(spend), 2),
        "savings_rate_pct": round((float(income) - float(spend)) / float(income) * 100, 1) if float(income) > 0 else 0,
        "spend_by_category": by_cat,
        "top_transactions": top_txns,
    })


def _tool_budget(inputs: dict, user_id: int) -> str:
    today = date.today()
    m = inputs.get("month") or today.month
    y = inputs.get("year") or today.year
    mm = f"{m:02d}"

    budgets = _db_query(
        "SELECT b.id, b.amount_cents, c.name FROM budget b JOIN category c ON b.category_id=c.id "
        "WHERE b.user_id=:uid AND b.month=:m AND b.year=:y",
        {"uid": user_id, "m": m, "y": y}
    )
    result = []
    for b in budgets:
        spend = _db_query(
            "SELECT COALESCE(SUM(t.amount),0) as total FROM transaction t "
            "JOIN category c ON t.category_id=c.id "
            "WHERE t.user_id=:uid AND c.name=:name AND t.is_credit=0 "
            "AND strftime('%m',t.date)=:mm AND strftime('%Y',t.date)=:y",
            {"uid": user_id, "name": b["name"], "mm": mm, "y": str(y)}
        )[0]["total"]
        budget_amt = b["amount_cents"] / 100
        result.append({
            "category": b["name"],
            "budget": round(budget_amt, 2),
            "spend": round(float(spend), 2),
            "remaining": round(budget_amt - float(spend), 2),
            "pct_used": round(float(spend) / budget_amt * 100, 1) if budget_amt > 0 else 0,
            "status": "over" if float(spend) > budget_amt else "ok",
        })
    result.sort(key=lambda x: x["pct_used"], reverse=True)
    return json.dumps({"month": f"{y}-{mm}", "budgets": result})


def _tool_categories(user_id: int) -> str:
    cats = _db_query(
        "SELECT id, name, color, is_income, is_system FROM category "
        "WHERE user_id=:uid OR is_system=1 ORDER BY is_income DESC, name",
        {"uid": user_id}
    )
    return json.dumps({"categories": cats, "count": len(cats)})


def _tool_goals(user_id: int) -> str:
    goals = _db_query(
        "SELECT g.id, g.name, g.target_cents, g.current_cents, g.target_date, g.is_complete, g.notes "
        "FROM goal g WHERE g.user_id=:uid ORDER BY g.is_complete, g.target_date",
        {"uid": user_id}
    )
    result = []
    for g in goals:
        target = g["target_cents"] / 100
        current = g["current_cents"] / 100
        result.append({
            "name": g["name"],
            "target": round(target, 2),
            "current": round(current, 2),
            "remaining": round(target - current, 2),
            "pct": round(current / target * 100, 1) if target > 0 else 0,
            "target_date": g["target_date"],
            "is_complete": bool(g["is_complete"]),
        })
    return json.dumps({"goals": result})


def _tool_score(inputs: dict, user_id: int) -> str:
    today = date.today()
    m = inputs.get("month") or today.month
    y = inputs.get("year") or today.year

    # Import and call compute function directly
    from routers.score import _compute_score
    with Session(engine) as session:
        data = _compute_score(session, m, y, user_id)
    return json.dumps(data)


def _tool_investments(user_id: int) -> str:
    shares = _db_query(
        "SELECT ticker, name, qty, avg_cost_aud, price_aud, value_aud, gain_aud, gain_pct, broker "
        "FROM shareholding WHERE user_id=:uid ORDER BY value_aud DESC",
        {"uid": user_id}
    )
    crypto = _db_query(
        "SELECT symbol, qty, price_aud, value_aud, source FROM cryptoholding "
        "WHERE user_id=:uid ORDER BY value_aud DESC",
        {"uid": user_id}
    )
    total_shares = sum(float(r["value_aud"] or 0) for r in shares)
    total_crypto = sum(float(r["value_aud"] or 0) for r in crypto)
    total_cost = _db_query(
        "SELECT COALESCE(SUM(cost_basis_aud),0) as t FROM shareholding WHERE user_id=:uid",
        {"uid": user_id}
    )[0]["t"]
    total_gain = _db_query(
        "SELECT COALESCE(SUM(gain_aud),0) as t FROM shareholding WHERE user_id=:uid",
        {"uid": user_id}
    )[0]["t"]
    return json.dumps({
        "shares": shares,
        "crypto": crypto,
        "total_shares_value": round(total_shares, 2),
        "total_crypto_value": round(total_crypto, 2),
        "total_portfolio": round(total_shares + total_crypto, 2),
        "total_cost_basis": round(float(total_cost), 2),
        "total_unrealised_gain": round(float(total_gain), 2),
    })


def _tool_networth(user_id: int) -> str:
    latest = _db_query(
        "SELECT * FROM networthsnapshot WHERE user_id=:uid ORDER BY snapshot_date DESC LIMIT 1",
        {"uid": user_id}
    )
    history = _db_query(
        "SELECT snapshot_date, net_worth, total_assets, total_liabilities "
        "FROM networthsnapshot WHERE user_id=:uid ORDER BY snapshot_date DESC LIMIT 12",
        {"uid": user_id}
    )
    super_latest = _db_query(
        "SELECT balance_aud, snapshot_date FROM supersnapshot "
        "WHERE user_id=:uid ORDER BY snapshot_date DESC LIMIT 1",
        {"uid": user_id}
    )
    return json.dumps({
        "latest": latest[0] if latest else None,
        "history": history,
        "super_latest": super_latest[0] if super_latest else None,
    })


def _tool_tax(inputs: dict, user_id: int) -> str:
    fy = inputs.get("fy") or _current_fy()
    fy_start = f"{fy-1}-07-01"
    fy_end   = f"{fy}-06-30"

    deductibles = _db_query(
        "SELECT t.date, t.description, t.amount, c.name as category, t.tax_category "
        "FROM transaction t LEFT JOIN category c ON t.category_id=c.id "
        "WHERE t.user_id=:uid AND t.tax_deductible=1 AND t.date>=:s AND t.date<=:e "
        "ORDER BY t.amount DESC",
        {"uid": user_id, "s": fy_start, "e": fy_end}
    )
    total_deductible = sum(float(r["amount"]) for r in deductibles)

    cgt = _db_query(
        "SELECT d.ticker, d.disposed_date, d.qty, d.gain_aud, d.discount_eligible "
        "FROM disposal d WHERE d.user_id=:uid AND d.disposed_date>=:s AND d.disposed_date<=:e",
        {"uid": user_id, "s": fy_start, "e": fy_end}
    )
    cgt_gains = sum(float(r["gain_aud"]) for r in cgt if float(r["gain_aud"]) > 0)
    cgt_losses = sum(abs(float(r["gain_aud"])) for r in cgt if float(r["gain_aud"]) < 0)
    long_gains = sum(float(r["gain_aud"]) for r in cgt if float(r["gain_aud"]) > 0 and r["discount_eligible"])
    discount = long_gains * 0.5

    divs = _db_query(
        "SELECT ticker, SUM(amount_aud) as cash, SUM(franking_credits_aud) as franking "
        "FROM dividend WHERE user_id=:uid AND pay_date>=:s AND pay_date<=:e GROUP BY ticker",
        {"uid": user_id, "s": fy_start, "e": fy_end}
    )
    total_div_cash = sum(float(r["cash"]) for r in divs)
    total_franking = sum(float(r["franking"]) for r in divs)

    return json.dumps({
        "fy": f"{fy-1}-{str(fy)[2:]}",
        "deductible_total": round(total_deductible, 2),
        "deductible_count": len(deductibles),
        "deductibles_top10": deductibles[:10],
        "cgt_disposals": cgt,
        "cgt_gains": round(cgt_gains, 2),
        "cgt_losses": round(cgt_losses, 2),
        "cgt_discount": round(discount, 2),
        "cgt_net_gain": round(max(0, cgt_gains - discount - cgt_losses), 2),
        "dividends_by_ticker": divs,
        "total_dividend_cash": round(total_div_cash, 2),
        "total_franking_credits": round(total_franking, 2),
        "total_grossed_up": round(total_div_cash + total_franking, 2),
    })


def _tool_sql(query: str, user_id: int) -> str:
    """Execute a read-only SELECT query with safety checks."""
    q = query.strip().rstrip(";")
    # Safety: only allow SELECT
    if not re.match(r"(?i)^\s*SELECT\b", q):
        return json.dumps({"error": "Only SELECT statements are permitted"})
    # Block dangerous keywords
    for bad in ("INSERT", "UPDATE", "DELETE", "DROP", "ALTER", "ATTACH", "PRAGMA", "CREATE"):
        if re.search(rf"(?i)\b{bad}\b", q):
            return json.dumps({"error": f"Keyword '{bad}' is not permitted in chat queries"})

    # Add LIMIT if not present
    if not re.search(r"(?i)\bLIMIT\b", q):
        q += f" LIMIT {SQL_ROW_LIMIT}"

    try:
        rows = _db_query(q, {"uid": user_id})
        return json.dumps({"rows": rows, "count": len(rows)})
    except Exception as e:
        return json.dumps({"error": str(e)})


# ---------------------------------------------------------------------------
# Chat endpoint
# ---------------------------------------------------------------------------

class ChatMessage(BaseModel):
    role: str    # "user" | "assistant"
    content: str


class ChatRequest(BaseModel):
    message: str
    history: list[ChatMessage] = []
    conversation_id: Optional[int] = None   # None = start new conversation


@router.post("/api/chat")
async def chat(
    body: ChatRequest,
    session: Session = Depends(get_session),
    current_user: User = Depends(get_current_user),
):
    api_key = get_setting(session, "anthropic_api_key") or __import__("os").environ.get("ANTHROPIC_API_KEY", "")
    if not api_key:
        raise HTTPException(400, "Anthropic API key not configured. Add it in Settings.")

    client = anthropic.Anthropic(api_key=api_key)

    # Resolve or create conversation
    conv_id = body.conversation_id
    if conv_id:
        conv = session.get(ChatConversation, conv_id)
        if not conv or conv.user_id != current_user.id:
            conv_id = None  # invalid — will create new

    if not conv_id:
        # New conversation — title from first 80 chars of the message
        title = body.message[:80].strip()
        conv = ChatConversation(user_id=current_user.id, title=title, message_count=0)
        session.add(conv)
        session.commit()
        session.refresh(conv)
        conv_id = conv.id

    # Load persisted history for this conversation (last 20 turns)
    db_history = session.exec(
        select(ChatHistory)
        .where(ChatHistory.user_id == current_user.id,
               ChatHistory.conversation_id == conv_id)
        .order_by(ChatHistory.id.desc())
        .limit(20)
    ).all()
    db_history = list(reversed(db_history))

    # Build message list from DB history
    messages: list[dict] = [{"role": h.role, "content": h.content} for h in db_history]
    messages.append({"role": "user", "content": body.message})

    # Tool-use loop
    reply = None
    for _round in range(MAX_TOOL_ROUNDS):
        response = client.messages.create(
            model=MODEL,
            max_tokens=2048,
            system=_system_prompt(),
            tools=TOOLS,
            messages=messages,
        )

        tool_uses = [b for b in response.content if b.type == "tool_use"]
        text_blocks = [b for b in response.content if b.type == "text"]

        if not tool_uses:
            reply = "\n".join(b.text for b in text_blocks).strip()
            now = __import__("datetime").datetime.now().isoformat(timespec="seconds")
            session.add(ChatHistory(user_id=current_user.id, conversation_id=conv_id, role="user", content=body.message))
            session.add(ChatHistory(user_id=current_user.id, conversation_id=conv_id, role="assistant", content=reply))
            # Update conversation metadata
            conv = session.get(ChatConversation, conv_id)
            if conv:
                conv.updated_at = now
                conv.message_count = (conv.message_count or 0) + 2
                session.add(conv)
            session.commit()
            return {"reply": reply, "conversation_id": conv_id}

        messages.append({"role": "assistant", "content": response.content})
        tool_results = []
        for tu in tool_uses:
            result_str = _run_tool(tu.name, tu.input, current_user.id)
            tool_results.append({"type": "tool_result", "tool_use_id": tu.id, "content": result_str})
        messages.append({"role": "user", "content": tool_results})

    return {"reply": "I ran into an issue processing your request. Please try again.", "conversation_id": conv_id}


@router.get("/api/chat/history")
def get_chat_history(
    limit: int = 40,
    conversation_id: Optional[int] = None,
    session: Session = Depends(get_session),
    current_user: User = Depends(get_current_user),
):
    """Return persisted chat history for the current user (optionally filtered by conversation)."""
    stmt = select(ChatHistory).where(ChatHistory.user_id == current_user.id)
    if conversation_id is not None:
        stmt = stmt.where(ChatHistory.conversation_id == conversation_id)
    stmt = stmt.order_by(ChatHistory.id.asc()).limit(limit)
    rows = session.exec(stmt).all()
    return {"history": [{"role": r.role, "content": r.content, "id": r.id} for r in rows]}


@router.delete("/api/chat/history")
def clear_chat_history(
    session: Session = Depends(get_session),
    current_user: User = Depends(get_current_user),
):
    """Clear all chat history for the current user."""
    rows = session.exec(
        select(ChatHistory).where(ChatHistory.user_id == current_user.id)
    ).all()
    for r in rows:
        session.delete(r)
    session.commit()
    return {"ok": True, "deleted": len(rows)}


@router.get("/api/chat/conversations")
def list_conversations(
    search: str = "",
    session: Session = Depends(get_session),
    current_user: User = Depends(get_current_user),
):
    """List all chat conversations for the user, newest first."""
    convs = session.exec(
        select(ChatConversation)
        .where(ChatConversation.user_id == current_user.id)
        .order_by(ChatConversation.updated_at.desc())
    ).all()

    result = []
    for c in convs:
        if search and search.lower() not in c.title.lower():
            # Also check first assistant reply for search match
            first_reply = session.exec(
                select(ChatHistory)
                .where(ChatHistory.conversation_id == c.id,
                       ChatHistory.role == "assistant")
                .order_by(ChatHistory.id.asc())
                .limit(1)
            ).first()
            if not first_reply or search.lower() not in first_reply.content.lower():
                continue
        # Get first assistant message as preview
        preview_row = session.exec(
            select(ChatHistory)
            .where(ChatHistory.conversation_id == c.id,
                   ChatHistory.role == "assistant")
            .order_by(ChatHistory.id.asc())
            .limit(1)
        ).first()
        preview = (preview_row.content[:120] + "…") if preview_row and len(preview_row.content) > 120 else (preview_row.content if preview_row else "")
        result.append({
            "id": c.id,
            "title": c.title,
            "created_at": c.created_at,
            "updated_at": c.updated_at,
            "message_count": c.message_count,
            "preview": preview,
        })
    return result


@router.delete("/api/chat/conversations/{conv_id}")
def delete_conversation(
    conv_id: int,
    session: Session = Depends(get_session),
    current_user: User = Depends(get_current_user),
):
    conv = session.get(ChatConversation, conv_id)
    if not conv or conv.user_id != current_user.id:
        raise HTTPException(404, "Not found")
    msgs = session.exec(
        select(ChatHistory).where(ChatHistory.conversation_id == conv_id)
    ).all()
    for m in msgs:
        session.delete(m)
    session.delete(conv)
    session.commit()
    return {"ok": True}
