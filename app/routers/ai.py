from __future__ import annotations

import json
import os

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel
from sqlmodel import Session, select

from database import Transaction, Category, Setting, get_session
from deps import get_setting

router = APIRouter(prefix="/api/ai", tags=["ai"])

CATEGORY_LIST = [
    "Salary / Wages", "Investment Income", "Government Payments", "Other Income",
    "Groceries", "Dining & Takeaway", "Coffee & Snacks", "Transport", "Fuel",
    "Health & Medical", "Pharmacy", "Personal Care", "Entertainment",
    "Shopping & Clothing", "Home & Garden", "Rent / Mortgage", "Utilities",
    "Internet & Phone", "Insurance", "Subscriptions",
    "Work-related Travel", "Work Equipment", "Work Clothing / PPE",
    "Self-education", "Investment Fees", "Donations", "Sole Trader Expenses",
    "ATM / Cash", "Transfers", "Uncategorised",
]

ATO_CATEGORIES = [
    "Work-related car expenses",
    "Work-related travel expenses",
    "Work-related clothing expenses",
    "Work-related self-education",
    "Other work-related deductions",
    "Investment income deductions",
    "Donations",
    "Sole trader business expenses",
    None,
]


class CategoriseRequest(BaseModel):
    transaction_ids: list[int]
    force: bool = False  # re-categorise even if already categorised


def _build_prompt(txns: list, count: int) -> str:
    txn_lines = "\n".join(
        f'{i+1}. Date: {t.date}, Description: "{t.description}", '
        f'Amount: ${t.amount:.2f}, Type: {"credit" if t.is_credit else "debit"}'
        for i, t in enumerate(txns)
    )
    return f"""You are a personal finance assistant for an Australian user.
Categorise each transaction below using ONLY categories from this list:
{json.dumps(CATEGORY_LIST, indent=2)}

For each transaction also determine:
- is_tax_deductible: true/false (Australian tax deduction eligibility)
- tax_category: one of {json.dumps(ATO_CATEGORIES)} or null
- confidence: 0.0–1.0
- flag_unusual: true if the transaction seems unusual (very large, suspicious merchant, etc.)

Return a JSON array with exactly {count} objects in this format:
[
  {{
    "index": 1,
    "category": "Groceries",
    "is_tax_deductible": false,
    "tax_category": null,
    "confidence": 0.95,
    "flag_unusual": false
  }}
]

Transactions:
{txn_lines}

Return ONLY the JSON array, no other text."""


def _parse_ai_response(raw: str) -> list:
    """Extract JSON array from AI response, tolerating preamble/postamble text."""
    raw = raw.strip()
    start = raw.find("[")
    end = raw.rfind("]") + 1
    if start < 0 or end <= 0:
        raise json.JSONDecodeError("No JSON array found", raw, 0)
    return json.loads(raw[start:end])


async def _call_gemini(api_key: str, prompt: str) -> str:
    import asyncio
    from google import genai
    client = genai.Client(api_key=api_key)
    def _sync():
        return client.models.generate_content(model="gemini-2.5-flash", contents=prompt).text
    return await asyncio.to_thread(_sync)


async def _call_anthropic(api_key: str, prompt: str) -> str:
    import asyncio
    import anthropic
    client = anthropic.Anthropic(api_key=api_key)
    def _sync():
        return client.messages.create(
            model="claude-haiku-4-5-20251001",
            max_tokens=4096,
            messages=[{"role": "user", "content": prompt}],
        ).content[0].text
    return await asyncio.to_thread(_sync)


@router.post("/categorise")
async def categorise_transactions(
    body: CategoriseRequest,
    session: Session = Depends(get_session),
):
    provider = get_setting(session, "ai_provider") or "gemini"

    if provider == "gemini":
        api_key = get_setting(session, "gemini_api_key") or os.environ.get("GEMINI_API_KEY", "")
        if not api_key:
            raise HTTPException(status_code=400, detail="Gemini API key not configured. Add it in Settings.")
    else:
        api_key = get_setting(session, "anthropic_api_key") or os.environ.get("ANTHROPIC_API_KEY", "")
        if not api_key:
            raise HTTPException(status_code=400, detail="Anthropic API key not configured. Add it in Settings.")

    # Fetch transactions
    txns = []
    for txn_id in body.transaction_ids:
        t = session.get(Transaction, txn_id)
        if t:
            cat = session.get(Category, t.category_id) if t.category_id else None
            if not body.force and cat and cat.name != "Uncategorised":
                continue
            txns.append(t)

    if not txns:
        return {"categorised": 0, "results": []}

    batch = txns[:50]
    prompt = _build_prompt(batch, len(batch))

    try:
        if provider == "gemini":
            raw = await _call_gemini(api_key, prompt)
        else:
            raw = await _call_anthropic(api_key, prompt)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"AI API error: {e}")

    try:
        results = _parse_ai_response(raw)
    except json.JSONDecodeError:
        raise HTTPException(status_code=500, detail=f"AI returned invalid JSON: {raw[:200]}")

    # Apply results to DB
    categories = session.exec(select(Category)).all()
    cat_map = {c.name.lower(): c for c in categories}

    applied = []
    for item in results:
        idx = item.get("index", 1) - 1
        if idx < 0 or idx >= len(batch):
            continue
        t = batch[idx]
        cat_name = item.get("category", "Uncategorised")
        cat = cat_map.get(cat_name.lower())
        if cat:
            t.category_id = cat.id
        t.tax_deductible = item.get("is_tax_deductible", False)
        t.tax_category = item.get("tax_category")
        if item.get("flag_unusual"):
            t.is_flagged = True
        session.add(t)
        applied.append({
            "id": t.id,
            "category": cat_name,
            "tax_deductible": t.tax_deductible,
            "flagged": t.is_flagged,
        })

    session.commit()
    return {"categorised": len(applied), "results": applied}


@router.post("/categorise-all-uncategorised")
async def categorise_all_uncategorised(
    session: Session = Depends(get_session),
):
    """Find ALL Uncategorised transactions and batch-categorise them in loops of 50."""
    uncategorised_cat = session.exec(
        select(Category).where(Category.name == "Uncategorised")
    ).first()

    if not uncategorised_cat:
        return {"categorised": 0}

    # Fetch all uncategorised IDs upfront to avoid infinite loop if AI re-assigns "Uncategorised"
    all_ids = [
        t.id for t in session.exec(
            select(Transaction).where(Transaction.category_id == uncategorised_cat.id)
        ).all()
    ]

    if not all_ids:
        return {"categorised": 0}

    total_categorised = 0
    BATCH = 50

    for i in range(0, len(all_ids), BATCH):
        batch_ids = all_ids[i:i + BATCH]
        req = CategoriseRequest(transaction_ids=batch_ids, force=True)
        result = await categorise_transactions(body=req, session=session)
        total_categorised += result.get("categorised", 0)

    return {"categorised": total_categorised}
