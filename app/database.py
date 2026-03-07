from __future__ import annotations

import os
from datetime import date, datetime
from typing import Optional, List
from sqlmodel import Field, Session, SQLModel, create_engine, select, func

_DATA_DIR = os.environ.get("FINANCE_DATA_DIR", "/data")
DATABASE_URL = os.environ.get("DATABASE_URL", f"sqlite:////{_DATA_DIR}/finance.db")
engine = create_engine(DATABASE_URL, connect_args={"check_same_thread": False})


def get_session():
    with Session(engine) as session:
        yield session


def create_db():
    SQLModel.metadata.create_all(engine)
    _migrate()
    _seed_defaults()


# ---------------------------------------------------------------------------
# Models — no back-populates (avoids SQLModel generic-list annotation issues)
# ---------------------------------------------------------------------------

class User(SQLModel, table=True):
    id:           Optional[int] = Field(default=None, primary_key=True)
    name:         str
    password_hash: str = ""          # pbkdf2_hmac sha256; empty = no password set yet
    color_hex:    str = "#6366f1"
    created_at:   str = Field(default_factory=lambda: datetime.now().isoformat(timespec="seconds"))


class UserSession(SQLModel, table=True):
    token:      str = Field(primary_key=True)
    user_id:    int = Field(foreign_key="user.id")
    created_at: str = Field(default_factory=lambda: datetime.now().isoformat(timespec="seconds"))
    expires_at: str  # ISO datetime string


class Account(SQLModel, table=True):
    id:           Optional[int] = Field(default=None, primary_key=True)
    name:         str
    bank:         str = "Macquarie"
    account_number: str = ""
    linked_loan_id: Optional[int] = None   # if set, CSV imports sync loan balance + payments
    offset_loan_id: Optional[int] = None   # if set, CSV imports sync this account balance → loan.offset_cents
    user_id:      int = Field(default=1)


class Category(SQLModel, table=True):
    id:            Optional[int] = Field(default=None, primary_key=True)
    name:          str
    colour:        str = "#6366f1"
    icon:          str = "mdi:tag"
    is_income:     bool = False
    is_tax_relevant: bool = False
    exclude_from_spend: bool = Field(default=False)  # e.g. Transfers — not personal spending
    parent_id:     Optional[int] = Field(default=None, foreign_key="category.id")


class Transaction(SQLModel, table=True):
    id:           Optional[int] = Field(default=None, primary_key=True)
    account_id:   Optional[int] = Field(default=None, foreign_key="account.id")
    date:         date
    description:  str
    amount:       float  # always positive
    is_credit:    bool = False
    category_id:  Optional[int] = Field(default=None, foreign_key="category.id")
    is_flagged:   bool = False
    is_reviewed:  bool = False
    tax_deductible: bool = False
    tax_category: Optional[str] = None
    notes:        Optional[str] = None
    receipt_path: Optional[str] = None  # path to saved receipt/email file
    raw_hash:     str = Field(index=True)  # SHA256 for dedup
    is_overseas:  bool = Field(default=False)
    currency_code: Optional[str] = None   # e.g. "USD", "THB", "VND"
    is_reimbursable: bool = Field(default=False)   # work expense to be paid back
    reimbursement_received: bool = Field(default=False)  # True once employer has paid back
    user_id:      int = Field(default=1)


class Budget(SQLModel, table=True):
    id:          Optional[int] = Field(default=None, primary_key=True)
    category_id: int = Field(foreign_key="category.id")
    month:       int  # 1–12
    year:        int
    amount_cents: int  # store as cents to avoid float issues
    user_id:     int = Field(default=1)


class Bill(SQLModel, table=True):
    id:          Optional[int] = Field(default=None, primary_key=True)
    name:        str
    amount_cents: int
    frequency:   str = "monthly"  # weekly, fortnightly, monthly, quarterly, annual
    next_due:    Optional[date] = None
    category_id: Optional[int] = Field(default=None, foreign_key="category.id")
    is_active:   bool = True
    user_id:     int = Field(default=1)


class BillPayment(SQLModel, table=True):
    id:          Optional[int] = Field(default=None, primary_key=True)
    bill_id:     int = Field(foreign_key="bill.id")
    paid_date:   date
    amount_cents: int
    notes:       Optional[str] = None
    user_id:     int = Field(default=1)


class Setting(SQLModel, table=True):
    key:   str = Field(primary_key=True)
    value: str = ""


class CryptoHolding(SQLModel, table=True):
    id:             Optional[int] = Field(default=None, primary_key=True)
    symbol:         str                          # e.g. "BTC"
    qty:            float = 0.0                  # units held
    price_aud:      float = 0.0                  # most recently fetched AUD price
    value_aud:      float = 0.0                  # qty * price_aud
    synced_at:      Optional[str] = None         # ISO datetime string
    source:         str = "binance"              # "binance" | "manual"
    # Cost basis (populated by sync-trades)
    avg_cost_aud:   float = 0.0                  # weighted avg purchase price per unit
    cost_basis_aud: float = 0.0                  # qty * avg_cost_aud
    gain_aud:       float = 0.0                  # value_aud - cost_basis_aud
    gain_pct:       float = 0.0                  # gain as % of cost basis
    user_id:        int = Field(default=1)


class CryptoTrade(SQLModel, table=True):
    """Individual trade fetched from Binance — source of truth for cost basis."""
    id:          Optional[int] = Field(default=None, primary_key=True)
    binance_id:  str = Field(index=True)     # Binance trade ID (unique per symbol)
    symbol:      str                          # base asset, e.g. "BTC"
    pair:        str = ""                     # trading pair, e.g. "BTCUSDT"
    trade_time:  str                          # ISO datetime UTC
    side:        str                          # "BUY" | "SELL"
    qty:         float = 0.0                  # units
    price_usdt:  float = 0.0                  # price in USDT (0 if AUD pair)
    price_aud:   float = 0.0                  # price in AUD at trade time
    fee_qty:     float = 0.0
    fee_asset:   str = ""
    user_id:     int = Field(default=1)


class ShareHolding(SQLModel, table=True):
    id:               Optional[int] = Field(default=None, primary_key=True)
    ticker:           str                    # e.g. "VAS.AX", "AAPL"
    name:             Optional[str] = None
    qty:              float = 0.0
    avg_cost_aud:     float = 0.0            # average cost per unit in AUD
    price_aud:        float = 0.0            # last fetched price in AUD
    value_aud:        float = 0.0            # qty * price_aud
    cost_basis_aud:   float = 0.0            # qty * avg_cost_aud
    gain_aud:         float = 0.0            # value_aud - cost_basis_aud
    gain_pct:         float = 0.0
    currency:         str = "AUD"            # quote currency from Yahoo
    price_fetched_at: Optional[str] = None
    purchase_date:    Optional[date] = None  # date of purchase (for historical FX lookup)
    purchase_fx_rate: Optional[float] = None # AUD per 1 USD at time of purchase
    broker:           str = "stake"          # "stake" | "chess" | "other"
    notes:            Optional[str] = None
    user_id:          int = Field(default=1)


class NetWorthSnapshot(SQLModel, table=True):
    id:                Optional[int] = Field(default=None, primary_key=True)
    snapshot_date:     date
    label:             Optional[str] = None
    created_at:        Optional[str] = None
    # Assets
    cash_savings:      float = 0.0
    super_balance:     float = 0.0
    property_value:    float = 0.0
    shares_value:      float = 0.0
    crypto_value:      float = 0.0
    other_assets:      float = 0.0
    # Liabilities (always positive numbers)
    mortgage_balance:  float = 0.0
    car_loan:          float = 0.0
    credit_card:       float = 0.0
    hecs_debt:         float = 0.0
    other_liabilities: float = 0.0
    # Computed totals
    total_assets:      float = 0.0
    total_liabilities: float = 0.0
    net_worth:         float = 0.0
    user_id:           int = Field(default=1)


class AcquisitionLot(SQLModel, table=True):
    """One purchase parcel of shares or crypto — the cost base for CGT."""
    id:               Optional[int] = Field(default=None, primary_key=True)
    ticker:           str                        # e.g. "VAS.AX", "BTC"
    asset_type:       str = "share"              # "share" | "crypto"
    acquired_date:    date
    qty:              float                      # units purchased
    cost_per_unit_aud: float                     # purchase price per unit in AUD
    brokerage_aud:    float = 0.0                # brokerage paid on purchase
    notes:            Optional[str] = None
    user_id:          int = Field(default=1)


class Disposal(SQLModel, table=True):
    """A sale of all or part of an AcquisitionLot."""
    id:                  Optional[int] = Field(default=None, primary_key=True)
    lot_id:              int = Field(foreign_key="acquisitionlot.id")
    ticker:              str
    asset_type:          str = "share"
    disposed_date:       date
    qty:                 float                   # units sold
    proceeds_per_unit_aud: float                 # sale price per unit in AUD
    brokerage_aud:       float = 0.0             # brokerage paid on sale
    gain_aud:            float = 0.0             # computed: (proceeds - cost - brokerage) * qty
    discount_eligible:   bool = False            # True if held > 12 months
    notes:               Optional[str] = None
    user_id:             int = Field(default=1)


class Dividend(SQLModel, table=True):
    """One dividend or distribution payment."""
    id:                  Optional[int] = Field(default=None, primary_key=True)
    ticker:              str
    pay_date:            date
    ex_date:             Optional[date] = None
    amount_aud:          float = 0.0             # cash dividend received (AUD)
    franking_credits_aud: float = 0.0            # imputation credits (AUD)
    franking_pct:        float = 0.0             # 0–100, e.g. 100 = fully franked
    notes:               Optional[str] = None
    user_id:             int = Field(default=1)


class SuperSnapshot(SQLModel, table=True):
    id:            Optional[int] = Field(default=None, primary_key=True)
    snapshot_date: date
    fund_name:     Optional[str] = None
    balance_aud:   float = 0.0
    notes:         Optional[str] = None
    created_at:    Optional[str] = None
    user_id:       int = Field(default=1)


class SuperContribution(SQLModel, table=True):
    id:                Optional[int] = Field(default=None, primary_key=True)
    snapshot_id:       Optional[int] = Field(default=None, foreign_key="supersnapshot.id")
    contribution_date: date
    amount_aud:        float = 0.0
    type:              str = "employer"   # employer | employee | voluntary
    source:            Optional[str] = None
    notes:             Optional[str] = None
    user_id:           int = Field(default=1)


class Goal(SQLModel, table=True):
    id:           Optional[int] = Field(default=None, primary_key=True)
    name:         str
    goal_type:    str = "long_term"                 # "short_term" | "long_term"
    target_cents: int                               # target amount in cents
    current_cents: int = 0                          # manually tracked progress
    target_date:  Optional[date] = None
    category_id:  Optional[int] = Field(default=None, foreign_key="category.id")
    is_complete:  bool = False
    notes:        Optional[str] = None
    created_at:   Optional[str] = None
    user_id:      int = Field(default=1)


class GoalContribution(SQLModel, table=True):
    id:               Optional[int] = Field(default=None, primary_key=True)
    goal_id:          int = Field(foreign_key="goal.id")
    contributed_date: date
    amount_cents:     int
    notes:            Optional[str] = None
    user_id:          int = Field(default=1)


class Achievement(SQLModel, table=True):
    """Unlocked achievement badges."""
    key:         str = Field(primary_key=True)   # e.g. "first_green_month" (prefixed "uid:key" for multi-user)
    unlocked_at: Optional[str] = None            # ISO datetime
    data_json:   Optional[str] = None            # extra context JSON
    user_id:     int = Field(default=1)


class Challenge(SQLModel, table=True):
    """User-set monthly spending or saving challenge."""
    id:             Optional[int] = Field(default=None, primary_key=True)
    name:           str
    challenge_type: str = "spend_limit"          # spend_limit | save_target
    category_id:    Optional[int] = Field(default=None, foreign_key="category.id")
    target_value:   float                        # AUD amount
    month:          int
    year:           int
    is_active:      bool = True
    is_complete:    bool = False
    user_id:        int = Field(default=1)


class Payslip(SQLModel, table=True):
    id:                Optional[int] = Field(default=None, primary_key=True)
    pay_date:          date
    period_start:      Optional[date] = None
    period_end:        Optional[date] = None
    employer:          Optional[str] = None
    pay_frequency:     Optional[str] = None          # fortnightly, weekly, monthly
    gross_pay_cents:   int = 0
    net_pay_cents:     int = 0
    tax_withheld_cents: int = 0
    super_cents:       int = 0
    annual_leave_hours: Optional[float] = None
    sick_leave_hours:  Optional[float] = None
    long_service_hours: Optional[float] = None
    ytd_gross_cents:   Optional[int] = None
    ytd_tax_cents:     Optional[int] = None
    ytd_super_cents:   Optional[int] = None
    hours_worked:      Optional[float] = None
    allowances_json:   Optional[str] = None        # JSON list of {name, amount}
    deductions_json:   Optional[str] = None        # JSON list of {name, amount}
    flags_json:        Optional[str] = None        # JSON list of flag strings
    raw_extracted:     Optional[str] = None        # full AI-extracted JSON
    source:            str = "upload"              # upload | gmail
    filename:          Optional[str] = None
    is_reviewed:       bool = False
    matched_txn_id:    Optional[int] = None        # soft-link to matched bank transaction
    user_id:           int = Field(default=1)


class RecurringPattern(SQLModel, table=True):
    """Detected (or manually confirmed) recurring transaction pattern."""
    id:          Optional[int] = Field(default=None, primary_key=True)
    norm_key:    str                             # normalised merchant key (upper, no numbers)
    display_name: str                            # human-readable merchant name
    avg_amount:  float = 0.0
    frequency:   str = "monthly"                # weekly | fortnightly | monthly | quarterly | annual
    occurrences: int = 0
    last_date:   Optional[date] = None
    confidence:  float = 0.0                    # 0.0–1.0
    status:      str = "suggested"              # suggested | confirmed | dismissed
    bill_id:     Optional[int] = None           # linked bill (if converted)
    user_id:     int = Field(default=1)


class MerchantEnrichment(SQLModel, table=True):
    __tablename__ = "merchant_enrichment"
    raw_key:    str = Field(primary_key=True)   # uppercase desc[:50]
    clean_name: str                              # human-readable name
    domain:     Optional[str] = Field(default=None)  # for logo lookup


class ChatConversation(SQLModel, table=True):
    __tablename__ = "chat_conversation"
    id:            Optional[int] = Field(default=None, primary_key=True)
    user_id:       int
    title:         str = ""          # auto-set from first user message (≤80 chars)
    created_at:    str = Field(default_factory=lambda: __import__("datetime").datetime.now().isoformat(timespec="seconds"))
    updated_at:    str = Field(default_factory=lambda: __import__("datetime").datetime.now().isoformat(timespec="seconds"))
    message_count: int = 0


class ChatHistory(SQLModel, table=True):
    __tablename__ = "chat_history"
    id:              Optional[int] = Field(default=None, primary_key=True)
    user_id:         int
    conversation_id: Optional[int] = Field(default=None, foreign_key="chat_conversation.id")
    role:            str   # "user" | "assistant"
    content:         str
    created_at:      str = Field(default_factory=lambda: __import__("datetime").datetime.now().isoformat(timespec="seconds"))


class Loan(SQLModel, table=True):
    id:                     Optional[int] = Field(default=None, primary_key=True)
    name:                   str
    loan_type:              str = "mortgage"   # mortgage | personal | car | other
    principal_cents:        int                # original principal (cents)
    outstanding_cents:      int                # current balance (cents)
    interest_rate:          float              # annual % e.g. 6.14
    start_date:             date
    term_months:            int
    monthly_repayment_cents: int
    offset_cents:           int = 0            # offset account balance (cents)
    is_active:              bool = True
    notes:                  Optional[str] = None
    user_id:                int = 1


class LoanPayment(SQLModel, table=True):
    """Records an actual repayment made against a loan (regular or extra)."""
    id:           Optional[int] = Field(default=None, primary_key=True)
    loan_id:      int = Field(foreign_key="loan.id")
    payment_date: date
    amount_cents: int                         # total payment amount (cents)
    principal_cents: int = 0                  # portion reducing balance
    interest_cents:  int = 0                  # portion that is interest
    notes:        Optional[str] = None        # e.g. "extra repayment", "refinance"
    user_id:      int = 1


class Trip(SQLModel, table=True):
    """Work-related travel log for ATO tax deduction purposes."""
    id:             Optional[int] = Field(default=None, primary_key=True)
    user_id:        int = Field(default=1)
    date:           date
    purpose:        str = "work"          # work | personal | medical | charity
    description:    Optional[str] = None  # e.g. "Client meeting — Parramatta"
    start_location: Optional[str] = None
    end_location:   Optional[str] = None
    km:             float = 0.0
    toll_cents:     int = 0               # toll costs in cents
    notes:          Optional[str] = None


class AdvisorSession(SQLModel, table=True):
    """Stores each AI financial advisor report and follow-up chat messages."""
    id:            Optional[int] = Field(default=None, primary_key=True)
    user_id:       int = 1
    created_at:    datetime = Field(default_factory=datetime.utcnow)
    report_text:   str = ""
    user_context:  Optional[str] = None      # user-provided notes/context
    chat_messages: Optional[str] = None      # JSON: [{role, content, ts}, ...]


# ---------------------------------------------------------------------------
# Default seed data
# ---------------------------------------------------------------------------

DEFAULT_CATEGORIES = [
    # Income
    {"name": "Salary / Wages", "colour": "#22c55e", "icon": "mdi:briefcase", "is_income": True},
    {"name": "Investment Income", "colour": "#10b981", "icon": "mdi:trending-up", "is_income": True},
    {"name": "Government Payments", "colour": "#14b8a6", "icon": "mdi:bank", "is_income": True},
    {"name": "Other Income", "colour": "#84cc16", "icon": "mdi:plus-circle", "is_income": True},
    # Everyday
    {"name": "Groceries", "colour": "#f97316", "icon": "mdi:cart"},
    {"name": "Dining & Takeaway", "colour": "#fb923c", "icon": "mdi:food"},
    {"name": "Coffee & Snacks", "colour": "#fbbf24", "icon": "mdi:coffee"},
    {"name": "Transport", "colour": "#60a5fa", "icon": "mdi:car"},
    {"name": "Fuel", "colour": "#3b82f6", "icon": "mdi:gas-station"},
    {"name": "Health & Medical", "colour": "#ec4899", "icon": "mdi:heart-pulse"},
    {"name": "Pharmacy", "colour": "#f43f5e", "icon": "mdi:pill"},
    {"name": "Personal Care", "colour": "#a78bfa", "icon": "mdi:face-woman"},
    {"name": "Entertainment", "colour": "#8b5cf6", "icon": "mdi:movie"},
    {"name": "Shopping & Clothing", "colour": "#d946ef", "icon": "mdi:shopping"},
    {"name": "Home & Garden", "colour": "#2dd4bf", "icon": "mdi:home"},
    # Bills / Fixed
    {"name": "Rent / Mortgage", "colour": "#0ea5e9", "icon": "mdi:home-city"},
    {"name": "Utilities", "colour": "#06b6d4", "icon": "mdi:flash"},
    {"name": "Internet & Phone", "colour": "#22d3ee", "icon": "mdi:wifi"},
    {"name": "Insurance", "colour": "#94a3b8", "icon": "mdi:shield-check"},
    {"name": "Subscriptions", "colour": "#64748b", "icon": "mdi:card-account-details"},
    # Tax-relevant
    {"name": "Work-related Travel", "colour": "#f59e0b", "icon": "mdi:airplane", "is_tax_relevant": True},
    {"name": "Work Equipment", "colour": "#d97706", "icon": "mdi:laptop", "is_tax_relevant": True},
    {"name": "Work Clothing / PPE", "colour": "#b45309", "icon": "mdi:tshirt-crew", "is_tax_relevant": True},
    {"name": "Self-education", "colour": "#92400e", "icon": "mdi:school", "is_tax_relevant": True},
    {"name": "Investment Fees", "colour": "#065f46", "icon": "mdi:chart-line", "is_tax_relevant": True},
    {"name": "Donations", "colour": "#7f1d1d", "icon": "mdi:hand-heart", "is_tax_relevant": True},
    {"name": "Sole Trader Expenses", "colour": "#1e3a5f", "icon": "mdi:store", "is_tax_relevant": True},
    # Other
    {"name": "ATM / Cash", "colour": "#6b7280", "icon": "mdi:cash"},
    {"name": "Transfers", "colour": "#9ca3af", "icon": "mdi:bank-transfer"},
    {"name": "Investment Transfer", "colour": "#0ea5e9", "icon": "mdi:bank-transfer-out"},
    {"name": "Uncategorised", "colour": "#d1d5db", "icon": "mdi:help-circle"},
]

DEFAULT_SETTINGS = {
    "anthropic_api_key": "",
    "financial_year": "2025",
    "ha_webhook_id": "",
    "ha_url": "http://hassio/core",
    "ha_token": "",
    "ha_notify_targets": "mobile_app_ryans_iphone",
    "binance_api_key": "",
    "binance_api_secret": "",
    "wfh_days": "0",
    "gmail_address": "",
    "gmail_app_password": "",
    "profile_age": "",
    "profile_gender": "",
    "profile_state": "",
    "profile_employment": "",
    "profile_household_size": "1",
    "super_fund_name": "",
    "super_member_number": "",
    "newsletter_email": "",
    "app_url": "",
    "stake_session_token": "",
    "folder_watch_enabled": "1",
    "demo_mode": "0",
    "budget_alert_threshold": "80",
    "budget_alerts_enabled": "1",
    "budget_alert_log": "{}",
    "forecast_return_pct": "7",
    "pace_alerts_enabled": "1",
    "pace_alert_threshold": "100",
    "pace_alert_log": "{}",
    "basiq_api_key": "",
    "basiq_user_id": "",
    "basiq_last_sync": "",
    "basiq_last_sync_count": "",
    "mcp_api_key": "",
    "payslip_watch_enabled": "1",
    "ai_ocr_enabled": "1",
    "ai_gmail_enabled": "1",
    "ai_newsletter_enabled": "1",
    "ai_categorise_enabled": "1",
    "ai_payslip_enabled": "1",
    "ai_ocr_calls_est": "10",
    "ai_gmail_calls_est": "20",
    "ai_newsletter_calls_est": "4",
    "ai_categorise_calls_est": "50",
    "ai_payslip_calls_est": "2",
}

# Tables that need a user_id column added for existing DBs
_USER_ID_TABLES = [
    "account", "transaction", "budget", "bill", "billpayment",
    "goal", "goalcontribution", "achievement", "challenge",
    "cryptoholding", "cryptotrade", "shareholding",
    "acquisitionlot", "disposal", "dividend",
    "networthsnapshot", "supersnapshot", "supercontribution", "payslip",
    "recurringpattern",
]


def _migrate():
    """Add new columns to existing tables that predate the current schema."""
    from sqlalchemy import text
    new_cols = [
        ("cryptoholding", "avg_cost_aud",   "REAL DEFAULT 0.0"),
        ("cryptoholding", "cost_basis_aud",  "REAL DEFAULT 0.0"),
        ("cryptoholding", "gain_aud",        "REAL DEFAULT 0.0"),
        ("cryptoholding", "gain_pct",        "REAL DEFAULT 0.0"),
    ]
    # Overseas transaction fields
    new_cols += [
        ("transaction", "is_overseas", "INTEGER DEFAULT 0"),
        ("transaction", "currency_code", "TEXT"),
    ]
    # Reimbursable work expense fields
    new_cols += [
        ("transaction", "is_reimbursable", "INTEGER DEFAULT 0"),
        ("transaction", "reimbursement_received", "INTEGER DEFAULT 0"),
    ]
    # Chat conversation tracking
    new_cols += [
        ("chat_history", "conversation_id", "INTEGER"),
    ]
    # Payslip-transaction matching
    new_cols += [
        ("payslip", "matched_txn_id", "INTEGER"),
    ]
    # Historical FX rate on share purchases
    new_cols += [
        ("shareholding", "purchase_date",    "TEXT"),
        ("shareholding", "purchase_fx_rate", "REAL"),
    ]
    # Goal type (short_term / long_term)
    new_cols += [
        ("goal", "goal_type", "TEXT DEFAULT 'long_term'"),
    ]
    # Category: exclude from personal spend totals (e.g. Transfers)
    new_cols += [
        ("category", "exclude_from_spend", "INTEGER DEFAULT 0"),
    ]

    # Add user_id to all tables that need it
    for tbl in _USER_ID_TABLES:
        new_cols.append((tbl, "user_id", "INTEGER DEFAULT 1 NOT NULL"))

    with Session(engine) as session:
        for table, col, typedef in new_cols:
            try:
                session.exec(text(f'ALTER TABLE "{table}" ADD COLUMN {col} {typedef}'))
                session.commit()
            except Exception:
                pass  # column already exists

        # Flag transfer categories as excluded from personal spend
        try:
            session.exec(text(
                "UPDATE category SET exclude_from_spend=1 "
                "WHERE name IN ('Transfers', 'Investment Transfer') AND exclude_from_spend=0"
            ))
            session.commit()
        except Exception:
            pass


def _seed_defaults():
    with Session(engine) as session:
        # Users — seed first user (Ryan) if none exist
        user_count = session.exec(select(func.count()).select_from(User)).one()
        if user_count == 0:
            session.add(User(id=1, name="Ryan", color_hex="#6366f1"))
            session.commit()

        # Categories
        cat_count = session.exec(select(func.count()).select_from(Category)).one()
        if cat_count == 0:
            for cat in DEFAULT_CATEGORIES:
                session.add(Category(**cat))

        # Settings
        for key, value in DEFAULT_SETTINGS.items():
            existing_setting = session.get(Setting, key)
            if existing_setting is None:
                session.add(Setting(key=key, value=value))

        # Default account
        acc_count = session.exec(select(func.count()).select_from(Account)).one()
        if acc_count == 0:
            session.add(Account(name="Macquarie Transaction", bank="Macquarie", user_id=1))

        # Ensure Investment Transfer category exists (idempotent for existing DBs)
        inv_cat = session.exec(
            select(Category).where(Category.name == "Investment Transfer")
        ).first()
        if inv_cat is None:
            session.add(Category(
                name="Investment Transfer",
                colour="#0ea5e9",
                icon="mdi:bank-transfer-out",
            ))

        session.commit()
