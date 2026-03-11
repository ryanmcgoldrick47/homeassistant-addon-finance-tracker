from __future__ import annotations

"""Merchant name enrichment — clean raw bank descriptions and resolve logo domains."""

import json
from typing import Optional

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel
from sqlmodel import Session, select

from database import MerchantEnrichment, Transaction, get_session
from deps import get_setting

router = APIRouter(prefix="/api/merchants", tags=["merchants"])


class EnrichmentUpdate(BaseModel):
    clean_name: str
    domain: Optional[str] = None


@router.get("/enrichment")
def list_enrichments(session: Session = Depends(get_session)):
    return session.exec(select(MerchantEnrichment)).all()


@router.patch("/enrichment/{raw_key:path}")
def update_enrichment(
    raw_key: str, body: EnrichmentUpdate, session: Session = Depends(get_session)
):
    e = session.get(MerchantEnrichment, raw_key)
    if e:
        e.clean_name = body.clean_name
        e.domain = body.domain
    else:
        e = MerchantEnrichment(raw_key=raw_key, clean_name=body.clean_name, domain=body.domain)
    session.add(e)
    session.commit()
    session.refresh(e)
    return e


@router.post("/enrich-batch")
def enrich_batch(session: Session = Depends(get_session)):
    """Use Claude Haiku to clean transaction descriptions not yet enriched."""
    api_key = get_setting(session, "anthropic_api_key")
    if not api_key:
        raise HTTPException(status_code=400, detail="No Anthropic API key configured")

    # All unique raw keys in the transactions table
    descs = session.exec(select(Transaction.description)).all()
    all_keys = {d.strip().upper()[:50] for d in descs if d}

    # Keys already covered
    covered = {e.raw_key for e in session.exec(select(MerchantEnrichment)).all()}
    to_enrich = sorted(all_keys - covered)

    if not to_enrich:
        return {"enriched": 0, "message": "All merchants already enriched"}

    import anthropic
    client = anthropic.Anthropic(api_key=api_key)

    total_enriched = 0
    BATCH = 30
    for i in range(0, len(to_enrich), BATCH):
        batch = to_enrich[i : i + BATCH]
        prompt = (
            "Clean these Australian bank transaction descriptions into proper merchant names.\n"
            "For each, return:\n"
            "- clean_name: proper display name (e.g. 'Woolworths', 'Netflix', 'Shell')\n"
            "- domain: best-guess website domain for logo (e.g. 'woolworths.com.au', 'netflix.com'), "
            "or null if unknown\n\n"
            "Return ONLY a JSON array (no markdown):\n"
            '[{"raw": "...", "clean_name": "...", "domain": "..."}, ...]\n\n'
            "Descriptions:\n" + "\n".join(f"- {r}" for r in batch)
        )
        try:
            msg = client.messages.create(
                model="claude-haiku-4-5-20251001",
                max_tokens=2000,
                messages=[{"role": "user", "content": prompt}],
            )
            text = msg.content[0].text.strip()
            start = text.find("[")
            end = text.rfind("]") + 1
            if start < 0 or end <= 0:
                continue
            results = json.loads(text[start:end])
            for item in results:
                raw_key = str(item.get("raw", "")).strip().upper()[:50]
                if not raw_key:
                    continue
                clean = item.get("clean_name") or raw_key.title()
                domain = item.get("domain") or None
                e = MerchantEnrichment(raw_key=raw_key, clean_name=clean, domain=domain)
                session.merge(e)
                total_enriched += 1
            session.commit()
        except Exception:
            continue

    return {"enriched": total_enriched, "total_unique_merchants": len(to_enrich)}
