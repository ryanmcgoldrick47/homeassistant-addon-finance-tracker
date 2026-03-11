"""Demo mode data masker — replaces real personal data with plausible fakes."""
from __future__ import annotations

import hashlib
import random

# Deterministic: same input always produces same fake output
# so navigation is consistent within a session.


def _seed(s: str) -> random.Random:
    h = int(hashlib.md5(str(s).encode()).hexdigest(), 16)
    return random.Random(h)


FAKE_MERCHANTS = [
    "Woolworths", "Coles", "ALDI", "Harris Farm Markets",
    "McDonald's", "Guzman y Gomez", "Roll'd", "Subway",
    "Officeworks", "JB Hi-Fi", "Kmart", "Big W",
    "Shell Coles Express", "BP", "Ampol",
    "Spotify", "Netflix", "Amazon Prime", "Adobe",
    "Bunnings", "Ikea", "Harvey Norman",
    "Chemist Warehouse", "Terry White", "Priceline",
    "Uber", "Uber Eats", "DoorDash",
    "Sydney Water", "Origin Energy", "AGL",
    "Telstra", "Optus", "Vodafone",
    "ANZ", "CBA Direct Debit", "ING Transfer",
    "Fitness First", "Anytime Fitness",
    "Qantas", "Jetstar", "Virgin Australia",
]

FAKE_NAMES = [
    "Alex Thompson", "Jordan Lee", "Sam Williams", "Casey Brown",
    "Morgan Davis", "Riley Wilson", "Taylor Moore", "Avery Jones",
]

FAKE_BANKS = ["Macquarie", "ANZ", "CBA", "Westpac", "NAB", "ING"]
FAKE_TICKERS = ["VAS.AX", "VGS.AX", "A200.AX", "BHP.AX", "CBA.AX", "VTI", "AAPL", "MSFT", "GOOGL"]
FAKE_CRYPTO = ["BTC", "ETH", "SOL", "LINK", "BNB"]


def mask_amount(amount: float, seed_val: str) -> float:
    r = _seed(seed_val)
    # Preserve order-of-magnitude but randomise value
    if amount < 10:
        return round(r.uniform(1, 9.99), 2)
    elif amount < 50:
        return round(r.uniform(10, 49.99), 2)
    elif amount < 200:
        return round(r.uniform(50, 199), 2)
    elif amount < 1000:
        return round(r.uniform(200, 999), 2)
    elif amount < 5000:
        return round(r.uniform(1000, 4999), 2)
    else:
        return round(r.uniform(5000, 20000), 2)


def mask_description(desc: str, txn_id) -> str:
    r = _seed(str(txn_id))
    return r.choice(FAKE_MERCHANTS)


def mask_account_name(account_id) -> str:
    r = _seed(f"acc_{account_id}")
    bank = r.choice(FAKE_BANKS)
    suffix = r.randint(1000, 9999)
    return f"{bank} ····{suffix}"


def mask_ticker(ticker: str) -> str:
    r = _seed(f"ticker_{ticker}")
    pool = FAKE_TICKERS if "." in ticker or ticker.endswith(".AX") else [t for t in FAKE_TICKERS if "." not in t]
    return r.choice(pool or FAKE_TICKERS)


def mask_crypto_symbol(sym: str) -> str:
    r = _seed(f"crypto_{sym}")
    return r.choice(FAKE_CRYPTO)


def mask_transaction(t: dict) -> dict:
    t = dict(t)
    seed = str(t.get("id", t.get("description", "")))
    fake_desc = mask_description(t.get("description", ""), seed)
    t["description"] = fake_desc
    t["clean_name"] = fake_desc   # keep consistent with masked description
    t["logo_domain"] = None       # don't leak real domain in demo mode
    t["amount"] = mask_amount(float(t.get("amount", 10)), seed)
    if t.get("notes"):
        t["notes"] = "Note hidden in demo mode"
    return t


def mask_transactions_list(items: list[dict]) -> list[dict]:
    return [mask_transaction(t) for t in items]


def mask_holding(h: dict) -> dict:
    h = dict(h)
    seed = str(h.get("id", h.get("ticker", "")))
    r = _seed(seed)
    h["ticker"] = mask_ticker(h.get("ticker", "XXX"))
    h["name"] = r.choice(["Vanguard ETF", "iShares Core", "BetaShares Fund", "Magellan Global", "Pinnacle Fund"])
    for field in ("qty", "avg_cost_aud", "price_aud", "value_aud", "cost_basis_aud", "gain_aud"):
        if h.get(field) is not None:
            h[field] = mask_amount(float(h[field]) or 500, f"{seed}_{field}")
    h["gain_pct"] = round(r.uniform(-15, 40), 2)
    return h


def mask_crypto_holding(c: dict) -> dict:
    c = dict(c)
    seed = str(c.get("id", c.get("symbol", "")))
    r = _seed(seed)
    c["symbol"] = mask_crypto_symbol(c.get("symbol", "BTC"))
    for field in ("qty", "price_aud", "value_aud", "avg_cost_aud", "cost_basis_aud", "gain_aud"):
        if c.get(field) is not None:
            c[field] = mask_amount(float(c[field]) or 500, f"{seed}_{field}")
    c["gain_pct"] = round(r.uniform(-20, 60), 2)
    return c


def mask_payslip(p: dict) -> dict:
    p = dict(p)
    seed = str(p.get("id", "pay"))
    p["employer"] = "Demo Employer Pty Ltd"
    for field in ("gross_pay_cents", "net_pay_cents", "tax_withheld_cents", "super_cents",
                  "ytd_gross_cents", "ytd_tax_cents", "ytd_super_cents"):
        if p.get(field):
            p[field] = int(mask_amount(p[field] / 100, f"{seed}_{field}") * 100)
    return p
