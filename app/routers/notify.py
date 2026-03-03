from __future__ import annotations

import os
from datetime import date, timedelta
from typing import Optional

import httpx
from fastapi import APIRouter, Depends
from pydantic import BaseModel
from sqlmodel import Session, select, func

import json as _json
from database import Bill, Budget, Transaction, Category, Setting, get_session
from deps import get_setting

router = APIRouter(prefix="/api/notify", tags=["notify"])

FREQ_PER_YEAR = {
    "weekly": 52,
    "fortnightly": 26,
    "monthly": 12,
    "quarterly": 4,
    "annual": 1,
}


def _get_notify_config(session: Session) -> tuple[str, str, list[str]]:
    """Return (ha_url, token, target_list). Uses SUPERVISOR_TOKEN env var if available."""
    ha_url = get_setting(session, "ha_url", "http://hassio/core")
    token = os.environ.get("SUPERVISOR_TOKEN", "") or get_setting(session, "ha_token", "")
    targets_str = get_setting(session, "ha_notify_targets", "mobile_app_ryans_iphone")
    targets = [t.strip() for t in targets_str.split(",") if t.strip()]
    return ha_url, token, targets


async def _send_notification(ha_url: str, token: str, targets: list[str],
                              title: str, message: str) -> list[dict]:
    sent = []
    async with httpx.AsyncClient(timeout=10) as client:
        for target in targets:
            try:
                resp = await client.post(
                    f"{ha_url}/api/services/notify/{target}",
                    headers={"Authorization": f"Bearer {token}"},
                    json={"title": title, "message": message},
                )
                sent.append({"target": target, "status": resp.status_code})
            except Exception as e:
                sent.append({"target": target, "error": str(e)})
    return sent


class TestBody(BaseModel):
    message: Optional[str] = "Finance Tracker notifications are working!"


@router.post("/test")
async def test_notification(body: TestBody, session: Session = Depends(get_session)):
    """Send a test notification to all configured HA notify targets."""
    ha_url, token, targets = _get_notify_config(session)
    if not token:
        return {"ok": False, "reason": "No HA token configured. Add a Long-Lived Access Token in Settings."}
    if not targets:
        return {"ok": False, "reason": "No notify targets configured."}
    sent = await _send_notification(ha_url, token, targets, "Finance Tracker", body.message)
    return {"ok": True, "sent": sent}


@router.post("/check")
async def check_and_notify(session: Session = Depends(get_session)):
    """
    Evaluate all notification conditions and send HA alerts as needed.
    Call this daily (e.g. from an HA automation or shell cron).
    """
    ha_url, token, targets = _get_notify_config(session)
    today = date.today()
    notifications_sent = []
    skipped_reason = None

    if not token or not targets:
        skipped_reason = "No ha_token or notify targets configured"

    # ── 1. Bills due today or tomorrow ──
    due_soon = session.exec(
        select(Bill).where(
            Bill.is_active == True,
            Bill.next_due != None,
            Bill.next_due >= today,
            Bill.next_due <= today + timedelta(days=1),
        ).order_by(Bill.next_due)
    ).all()

    if due_soon:
        lines = []
        for b in due_soon:
            when = "today" if b.next_due == today else "tomorrow"
            lines.append(f"• {b.name}: ${b.amount_cents/100:.2f} due {when}")
        msg = "\n".join(lines)
        if token and targets:
            sent = await _send_notification(ha_url, token, targets,
                                            f"{len(due_soon)} Bill{'s' if len(due_soon)>1 else ''} Due Soon",
                                            msg)
            notifications_sent.append({"type": "bills_due", "count": len(due_soon), "sent": sent})
        else:
            notifications_sent.append({"type": "bills_due", "count": len(due_soon), "skipped": skipped_reason})

    # ── 2. Budget categories over 100% + approaching threshold ──
    this_month = today.month
    this_year = today.year
    budgets = session.exec(
        select(Budget).where(Budget.month == this_month, Budget.year == this_year)
    ).all()

    # Load alert deduplication log (keyed by "month_year_catid_level")
    alert_threshold = float(get_setting(session, "budget_alert_threshold", "80"))
    alerts_enabled = get_setting(session, "budget_alerts_enabled", "1") == "1"
    try:
        alert_log = _json.loads(get_setting(session, "budget_alert_log", "{}"))
    except Exception:
        alert_log = {}
    # Prune entries from previous months
    period = f"{this_month}_{this_year}"
    alert_log = {k: v for k, v in alert_log.items() if k.startswith(period + "_")}

    over_budget = []
    approaching = []
    for budget in budgets:
        cat = session.get(Category, budget.category_id)
        if not cat:
            continue
        spend = float(session.exec(
            select(func.coalesce(func.sum(Transaction.amount), 0)).where(
                Transaction.is_credit == False,
                Transaction.category_id == budget.category_id,
                func.strftime("%m", Transaction.date) == f"{this_month:02d}",
                func.strftime("%Y", Transaction.date) == str(this_year),
            )
        ).one())
        budget_amt = budget.amount_cents / 100
        if budget_amt <= 0:
            continue
        pct = spend / budget_amt * 100

        if spend > budget_amt:
            over_budget.append(f"• {cat.name}: ${spend:.2f} / ${budget_amt:.2f}")
        elif alerts_enabled and pct >= alert_threshold:
            key = f"{period}_{budget.category_id}_{int(alert_threshold)}"
            if key not in alert_log:
                approaching.append(f"• {cat.name}: {pct:.0f}% used (${spend:.0f} / ${budget_amt:.0f})")
                alert_log[key] = str(today)

    # Save updated dedup log
    s_log = session.get(Setting, "budget_alert_log")
    if s_log:
        s_log.value = _json.dumps(alert_log)
        session.add(s_log)
        session.commit()

    if over_budget:
        msg = "Categories over budget this month:\n" + "\n".join(over_budget)
        if token and targets:
            sent = await _send_notification(ha_url, token, targets,
                                            f"{len(over_budget)} Budget{'s' if len(over_budget)>1 else ''} Exceeded",
                                            msg)
            notifications_sent.append({"type": "budget_exceeded", "count": len(over_budget), "sent": sent})
        else:
            notifications_sent.append({"type": "budget_exceeded", "count": len(over_budget), "skipped": skipped_reason})

    if approaching:
        msg = f"Approaching {alert_threshold:.0f}% of budget:\n" + "\n".join(approaching)
        if token and targets:
            sent = await _send_notification(ha_url, token, targets,
                                            f"{len(approaching)} Budget{'s' if len(approaching)>1 else ''} at {alert_threshold:.0f}%",
                                            msg)
            notifications_sent.append({"type": "budget_approaching", "count": len(approaching), "sent": sent})
        else:
            notifications_sent.append({"type": "budget_approaching", "count": len(approaching), "skipped": skipped_reason})

    # ── 3. Spending pace alerts ──
    pace_enabled = get_setting(session, "pace_alerts_enabled", "1") == "1"
    if pace_enabled and today.day > 1:
        from calendar import monthrange
        days_in_month = monthrange(today.year, today.month)[1]
        days_elapsed = today.day

        pace_threshold = float(get_setting(session, "pace_alert_threshold", "100"))
        try:
            pace_log = _json.loads(get_setting(session, "pace_alert_log", "{}"))
        except Exception:
            pace_log = {}
        pace_log = {k: v for k, v in pace_log.items() if k.startswith(period + "_")}

        on_pace_to_overspend = []
        for budget in budgets:
            cat = session.get(Category, budget.category_id)
            if not cat:
                continue
            spend = float(session.exec(
                select(func.coalesce(func.sum(Transaction.amount), 0)).where(
                    Transaction.is_credit == False,
                    Transaction.category_id == budget.category_id,
                    func.strftime("%m", Transaction.date) == f"{this_month:02d}",
                    func.strftime("%Y", Transaction.date) == str(this_year),
                )
            ).one())
            budget_amt = budget.amount_cents / 100
            if budget_amt <= 0 or spend <= 0:
                continue
            projected_pct = (spend / days_elapsed * days_in_month) / budget_amt * 100
            pkey = f"{period}_{budget.category_id}_pace"
            if projected_pct >= pace_threshold and spend < budget_amt and pkey not in pace_log:
                on_pace_to_overspend.append(
                    f"• {cat.name}: on pace for {projected_pct:.0f}% (${spend:.0f} spent, "
                    f"${spend / days_elapsed * days_in_month:.0f} projected)"
                )
                pace_log[pkey] = str(today)

        p_log_s = session.get(Setting, "pace_alert_log")
        if p_log_s:
            p_log_s.value = _json.dumps(pace_log)
            session.add(p_log_s)
            session.commit()

        if on_pace_to_overspend:
            msg = f"Spending pace exceeds budget (projected >{pace_threshold:.0f}%):\n" + "\n".join(on_pace_to_overspend)
            if token and targets:
                sent = await _send_notification(ha_url, token, targets,
                                                f"{len(on_pace_to_overspend)} Budget{'s' if len(on_pace_to_overspend)>1 else ''} On Track to Overspend",
                                                msg)
                notifications_sent.append({"type": "spend_pace", "count": len(on_pace_to_overspend), "sent": sent})
            else:
                notifications_sent.append({"type": "spend_pace", "count": len(on_pace_to_overspend), "skipped": skipped_reason})

    # ── 4. Flagged transactions needing review ──
    flagged_count = session.exec(
        select(func.count()).where(
            Transaction.is_flagged == True,
            Transaction.is_reviewed == False,
        )
    ).one()

    if flagged_count > 0:
        msg = f"You have {flagged_count} transaction{'s' if flagged_count > 1 else ''} flagged for review in Finance Tracker."
        if token and targets:
            sent = await _send_notification(ha_url, token, targets,
                                            f"{flagged_count} Transaction{'s' if flagged_count>1 else ''} Need Review",
                                            msg)
            notifications_sent.append({"type": "flagged", "count": int(flagged_count), "sent": sent})
        else:
            notifications_sent.append({"type": "flagged", "count": int(flagged_count), "skipped": skipped_reason})

    # ── 4. Monthly summary (send on 1st of month) ──
    if today.day == 1:
        # Previous month stats
        if today.month == 1:
            prev_m, prev_y = 12, today.year - 1
        else:
            prev_m, prev_y = today.month - 1, today.year

        prev_spend = float(session.exec(
            select(func.coalesce(func.sum(Transaction.amount), 0)).where(
                Transaction.is_credit == False,
                func.strftime("%m", Transaction.date) == f"{prev_m:02d}",
                func.strftime("%Y", Transaction.date) == str(prev_y),
            )
        ).one())
        prev_income = float(session.exec(
            select(func.coalesce(func.sum(Transaction.amount), 0)).where(
                Transaction.is_credit == True,
                func.strftime("%m", Transaction.date) == f"{prev_m:02d}",
                func.strftime("%Y", Transaction.date) == str(prev_y),
            )
        ).one())
        net = prev_income - prev_spend
        from calendar import month_name
        month_label = f"{month_name[prev_m]} {prev_y}"
        savings_pct = round(net / prev_income * 100, 1) if prev_income > 0 else 0
        msg = (
            f"Income: ${prev_income:,.2f}\n"
            f"Spend: ${prev_spend:,.2f}\n"
            f"Net: ${net:+,.2f} ({savings_pct:+.1f}% saved)"
        )
        if token and targets:
            sent = await _send_notification(ha_url, token, targets,
                                            f"{month_label} Summary",
                                            msg)
            notifications_sent.append({"type": "monthly_summary", "month": month_label, "sent": sent})
        else:
            notifications_sent.append({"type": "monthly_summary", "month": month_label, "skipped": skipped_reason})

    return {
        "checked_at": str(today),
        "notifications": notifications_sent,
        "total": len(notifications_sent),
    }
