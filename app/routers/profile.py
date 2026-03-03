from __future__ import annotations

from datetime import date

from fastapi import APIRouter, Depends
from sqlmodel import Session, select

from database import Transaction, Category, get_session
from deps import get_setting

router = APIRouter(prefix="/api/profile", tags=["profile"])

# ---------------------------------------------------------------------------
# ABS hard-coded benchmark data
# Source: ABS 6530.0 Household Expenditure Survey 2019-20
# All figures are approximate weekly AUD amounts per household
# ---------------------------------------------------------------------------

# Weekly household expenditure by age group (AUD)
ABS_WEEKLY_SPEND = {
    "15-24": {"food": 165, "housing": 210, "transport": 115, "recreation": 85,  "health": 35,  "clothing": 42},
    "25-34": {"food": 240, "housing": 385, "transport": 195, "recreation": 120, "health": 55,  "clothing": 58},
    "35-44": {"food": 310, "housing": 420, "transport": 235, "recreation": 150, "health": 70,  "clothing": 65},
    "45-54": {"food": 290, "housing": 370, "transport": 215, "recreation": 140, "health": 90,  "clothing": 55},
    "55-64": {"food": 250, "housing": 290, "transport": 185, "recreation": 120, "health": 110, "clothing": 45},
    "65+":   {"food": 195, "housing": 195, "transport": 130, "recreation": 95,  "health": 120, "clothing": 32},
}

# Median gross weekly income by age group and gender (AUD)
ABS_WEEKLY_INCOME = {
    "15-24": {"male": 590,  "female": 510,  "non_binary": 550},
    "25-34": {"male": 1350, "female": 1050, "non_binary": 1150},
    "35-44": {"male": 1580, "female": 1050, "non_binary": 1250},
    "45-54": {"male": 1480, "female": 1020, "non_binary": 1200},
    "55-64": {"male": 1290, "female": 910,  "non_binary": 1050},
    "65+":   {"male": 760,  "female": 620,  "non_binary": 680},
}

# Median household savings rate by age group (% of gross income)
ABS_SAVINGS_RATE = {
    "15-24": 8.0,
    "25-34": 12.5,
    "35-44": 14.2,
    "45-54": 16.8,
    "55-64": 18.5,
    "65+":   11.0,
}

# App category names → ABS category keys
ABS_CATEGORY_MAP = {
    "food":       ["Groceries", "Dining & Takeaway", "Coffee & Snacks"],
    "housing":    ["Rent / Mortgage", "Utilities", "Home & Garden"],
    "transport":  ["Transport", "Fuel"],
    "recreation": ["Entertainment", "Subscriptions"],
    "health":     ["Health & Medical", "Pharmacy"],
    "clothing":   ["Shopping & Clothing", "Personal Care"],
}

ABS_CATEGORY_LABELS = {
    "food":       "Food & Groceries",
    "housing":    "Housing & Utilities",
    "transport":  "Transport",
    "recreation": "Recreation & Entertainment",
    "health":     "Health & Medical",
    "clothing":   "Clothing & Personal Care",
}


def _age_group(age: int) -> str:
    if age < 25: return "15-24"
    if age < 35: return "25-34"
    if age < 45: return "35-44"
    if age < 55: return "45-54"
    if age < 65: return "55-64"
    return "65+"


def _spending_percentile(user_weekly: float, abs_median: float) -> int:
    """Estimate % of Australians who spend MORE than the user (higher = better)."""
    if abs_median == 0:
        return 50
    ratio = user_weekly / abs_median
    if ratio <= 0.4:
        return 85
    if ratio <= 1.0:
        return int(50 + (1.0 - ratio) * 58)
    if ratio <= 1.5:
        return int(50 - (ratio - 1.0) * 40)
    return max(5, int(30 - (ratio - 1.5) * 30))


def _rate_percentile(user_rate: float, abs_median: float) -> int:
    """Estimate % of Australians with a LOWER rate than the user (higher = better)."""
    if abs_median == 0:
        return 50
    ratio = user_rate / abs_median if abs_median > 0 else 0
    if ratio >= 2.0:
        return 90
    if ratio >= 1.0:
        return int(50 + (ratio - 1.0) * 40)
    if ratio >= 0:
        return int(ratio * 50)
    return 5


@router.get("/benchmarks")
def benchmarks(fy: int, session: Session = Depends(get_session)):
    age_str = get_setting(session, "profile_age", "")
    gender = get_setting(session, "profile_gender", "") or "male"

    if not age_str or not age_str.isdigit():
        return {
            "has_profile": False,
            "fy": fy,
            "fy_label": f"{fy-1}–{fy}",
            "message": "Set your age in Settings → Personal Profile to see benchmarks.",
        }

    age = int(age_str)
    age_group = _age_group(age)
    fy_start = date(fy - 1, 7, 1)
    fy_end = date(fy, 6, 30)
    WEEKS_IN_FY = 52.18

    # Fetch all transactions for the FY
    txns = session.exec(
        select(Transaction).where(
            Transaction.date >= fy_start,
            Transaction.date <= fy_end,
        )
    ).all()

    # Aggregate spend by category name
    cat_spend: dict[str, float] = {}
    total_income = 0.0
    total_spend = 0.0
    for t in txns:
        cat = session.get(Category, t.category_id) if t.category_id else None
        cat_name = cat.name if cat else "Uncategorised"
        if t.is_credit:
            total_income += t.amount
        else:
            total_spend += t.amount
            cat_spend[cat_name] = cat_spend.get(cat_name, 0) + t.amount

    user_weekly_income = total_income / WEEKS_IN_FY
    net = total_income - total_spend
    user_savings_rate = round(net / total_income * 100, 1) if total_income > 0 else 0.0

    abs_spend = ABS_WEEKLY_SPEND.get(age_group, {})
    abs_income_row = ABS_WEEKLY_INCOME.get(age_group, {})
    abs_income_median = float(abs_income_row.get(gender, 1000))
    abs_savings_median = ABS_SAVINGS_RATE.get(age_group, 12.0)

    categories = []
    for abs_key, app_cats in ABS_CATEGORY_MAP.items():
        user_annual = sum(cat_spend.get(c, 0.0) for c in app_cats)
        user_weekly = user_annual / WEEKS_IN_FY
        abs_median = abs_spend.get(abs_key, 0)
        pct = _spending_percentile(user_weekly, abs_median)
        categories.append({
            "abs_key": abs_key,
            "label": ABS_CATEGORY_LABELS[abs_key],
            "user_weekly_aud": round(user_weekly, 2),
            "abs_median_weekly_aud": float(abs_median),
            "percentile_better_than": pct,
            "status": "below_median" if user_weekly <= abs_median else "above_median",
        })

    return {
        "has_profile": True,
        "age": age,
        "age_group": age_group,
        "gender": gender,
        "fy": fy,
        "fy_label": f"{fy-1}–{fy}",
        "period": {"start": str(fy_start), "end": str(fy_end)},
        "user_weekly_income": round(user_weekly_income, 2),
        "abs_median_weekly_income": abs_income_median,
        "income_percentile": _rate_percentile(user_weekly_income, abs_income_median),
        "user_savings_rate": user_savings_rate,
        "abs_median_savings_rate": abs_savings_median,
        "savings_percentile": _rate_percentile(user_savings_rate, abs_savings_median),
        "categories": categories,
    }
