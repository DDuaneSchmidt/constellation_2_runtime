from __future__ import annotations

from typing import Final


CAPITAL_TYPES_V1: Final[tuple[str, ...]] = (
    "owned",
    "future",
    "speculative",
    "real_estate",
    "simulated",
)

CONTROL_TYPES_V1: Final[tuple[str, ...]] = (
    "advisor",
    "aegis",
    "passive",
    "self",
    "other",
)

BUCKET_TYPES_V1: Final[tuple[str, ...]] = (
    "income",
    "growth",
    "safety",
    "future_income",
    "speculative",
    "residence",
)

FLOW_TYPES_V1: Final[tuple[str, ...]] = (
    "contribution",
    "withdrawal",
    "transfer_in",
    "transfer_out",
    "distribution",
)

CASHFLOW_EVENT_TYPES_V1: Final[tuple[str, ...]] = (
    "invest_income",
    "social_security",
    "expense",
    "inheritance",
)

CASHFLOW_FREQUENCIES_V1: Final[tuple[str, ...]] = (
    "monthly",
    "annual",
    "one_time",
)

CASHFLOW_SCENARIOS_V1: Final[tuple[str, ...]] = (
    "base",
    "florida",
    "chile",
)

INPUT_SOURCES_V1: Final[tuple[str, ...]] = (
    "seed_capture",
    "manual_entry",
    "import",
    "broker_statement",
    "operator_adjustment",
)

CONFIDENCE_BANDS_V1: Final[tuple[str, ...]] = (
    "high",
    "medium",
    "low",
)

CONFIDENCE_MIN_V1: Final[float] = 0.0
CONFIDENCE_MAX_V1: Final[float] = 1.0
CONFIDENCE_BAND_HIGH_MIN_V1: Final[float] = 0.85
CONFIDENCE_BAND_MEDIUM_MIN_V1: Final[float] = 0.60

CAPITAL_DB_ENV_VAR_V1: Final[str] = "C2_CAPITAL_DB_PATH"
CAPITAL_DB_RELATIVE_DEFAULT_V1: Final[tuple[str, ...]] = (
    "capital_v1",
    "capital_domain.v1.sqlite3",
)

FLOW_TRUTH_OWNER_V1: Final[str] = "capital_cash_flows_v1"
FLOW_TRUTH_OWNER_DESCRIPTION_V1: Final[str] = (
    "Canonical external-capital movement truth is append-only cash flow rows; "
    "balance snapshots do not own flow truth."
)

FRESHNESS_POLICY_ID_V1: Final[str] = "capital_latest_global_snapshot_relative_v1"
FRESHNESS_POLICY_DESCRIPTION_V1: Final[str] = (
    "Included accounts are stale when their latest snapshot day is older than the latest global snapshot day "
    "for the report basis."
)

BALANCE_SEED_DATE_V1: Final[str] = "2026-04-22"

ACCOUNT_SEED_ROWS_V1: Final[tuple[dict[str, object], ...]] = (
    {
        "account_id": "schwab_brokerage",
        "account_name": "Schwab Brokerage",
        "capital_type": "owned",
        "control_type": "advisor",
        "bucket_type": "income",
        "include_in_allocation": True,
        "confidence_level": 1.00,
        "confidence_band": "high",
        "notes": "Invest-able, advisor managed",
    },
    {
        "account_id": "schwab_roth",
        "account_name": "Schwab Roth",
        "capital_type": "owned",
        "control_type": "advisor",
        "bucket_type": "growth",
        "include_in_allocation": True,
        "confidence_level": 1.00,
        "confidence_band": "high",
        "notes": "Advisor managed",
    },
    {
        "account_id": "schwab_rollover_ira",
        "account_name": "Schwab Rollover IRA",
        "capital_type": "owned",
        "control_type": "advisor",
        "bucket_type": "growth",
        "include_in_allocation": True,
        "confidence_level": 1.00,
        "confidence_band": "high",
        "notes": "Invest-able, advisor managed",
    },
    {
        "account_id": "schwab_cash",
        "account_name": "Schwab Cash",
        "capital_type": "owned",
        "control_type": "passive",
        "bucket_type": "safety",
        "include_in_allocation": True,
        "confidence_level": 1.00,
        "confidence_band": "high",
        "notes": "Cash / reserves",
    },
    {
        "account_id": "inheritance",
        "account_name": "Inheritance",
        "capital_type": "future",
        "control_type": "passive",
        "bucket_type": "growth",
        "include_in_allocation": False,
        "confidence_level": 0.50,
        "confidence_band": "low",
        "notes": "Timing uncertain (~10 yrs)",
    },
    {
        "account_id": "social_security",
        "account_name": "Social Security",
        "capital_type": "future",
        "control_type": "passive",
        "bucket_type": "future_income",
        "include_in_allocation": False,
        "confidence_level": 0.90,
        "confidence_band": "high",
        "notes": "Future income stream",
    },
    {
        "account_id": "ufb_savings",
        "account_name": "UFB Savings",
        "capital_type": "owned",
        "control_type": "passive",
        "bucket_type": "safety",
        "include_in_allocation": True,
        "confidence_level": 1.00,
        "confidence_band": "high",
        "notes": "Cash savings",
    },
    {
        "account_id": "security_benefit_annuity",
        "account_name": "Security Benefit Annuity",
        "capital_type": "owned",
        "control_type": "advisor",
        "bucket_type": "income",
        "include_in_allocation": True,
        "confidence_level": 0.95,
        "confidence_band": "high",
        "notes": "Annuity / benefit",
    },
    {
        "account_id": "catalyze_dallas",
        "account_name": "Catalyze Dallas",
        "capital_type": "speculative",
        "control_type": "passive",
        "bucket_type": "speculative",
        "include_in_allocation": False,
        "confidence_level": 0.30,
        "confidence_band": "low",
        "notes": "Speculative investment",
    },
    {
        "account_id": "aegis_paul",
        "account_name": "Aegis-Paul",
        "capital_type": "owned",
        "control_type": "aegis",
        "bucket_type": "growth",
        "include_in_allocation": True,
        "confidence_level": 1.00,
        "confidence_band": "high",
        "notes": "AEGIS trading capital",
    },
    {
        "account_id": "aegis_paper",
        "account_name": "Aegis-Paper",
        "capital_type": "simulated",
        "control_type": "aegis",
        "bucket_type": "growth",
        "include_in_allocation": False,
        "confidence_level": 1.00,
        "confidence_band": "high",
        "notes": "Paper trading (not capital)",
    },
    {
        "account_id": "zeno_ct_4742",
        "account_name": "4742 Zeno Ct",
        "capital_type": "real_estate",
        "control_type": "self",
        "bucket_type": "residence",
        "include_in_allocation": False,
        "confidence_level": 1.00,
        "confidence_band": "high",
        "notes": "Primary residence (not liquid)",
    },
)

BALANCE_SEED_ROWS_V1: Final[tuple[dict[str, object], ...]] = (
    {"account_id": "schwab_brokerage", "balance": 4055800.0},
    {"account_id": "schwab_roth", "balance": 510000.0},
    {"account_id": "schwab_rollover_ira", "balance": 265000.0},
    {"account_id": "schwab_cash", "balance": 48700.0},
    {"account_id": "inheritance", "balance": 500000.0},
    {"account_id": "social_security", "balance": 36000.0},
    {"account_id": "ufb_savings", "balance": 2000.0},
    {"account_id": "security_benefit_annuity", "balance": 615000.0},
    {"account_id": "catalyze_dallas", "balance": 175000.0},
    {"account_id": "aegis_paul", "balance": 20000.0},
    {"account_id": "aegis_paper", "balance": 0.0},
    {"account_id": "zeno_ct_4742", "balance": 1200000.0},
)

CASHFLOW_EVENT_SEED_ROWS_V1: Final[tuple[dict[str, object], ...]] = (
    {
        "event_name": "portfolio_income",
        "event_type": "invest_income",
        "scenario": "base",
        "start_date": "2026-05-01",
        "end_date": None,
        "frequency": "monthly",
        "amount": 18000.0,
        "confidence": 1.0,
        "is_deterministic": True,
        "notes": "Deterministic monthly portfolio income baseline.",
    },
    {
        "event_name": "florida_expenses",
        "event_type": "expense",
        "scenario": "florida",
        "start_date": "2026-05-01",
        "end_date": None,
        "frequency": "monthly",
        "amount": 12000.0,
        "confidence": 1.0,
        "is_deterministic": True,
        "notes": "Deterministic Florida monthly expense scenario.",
    },
    {
        "event_name": "chile_expenses",
        "event_type": "expense",
        "scenario": "chile",
        "start_date": "2026-05-01",
        "end_date": None,
        "frequency": "monthly",
        "amount": 8000.0,
        "confidence": 1.0,
        "is_deterministic": True,
        "notes": "Deterministic Chile monthly expense scenario.",
    },
    {
        "event_name": "social_security",
        "event_type": "social_security",
        "scenario": "base",
        "start_date": "2035-01-01",
        "end_date": None,
        "frequency": "monthly",
        "amount": 3000.0,
        "confidence": 0.9,
        "is_deterministic": True,
        "notes": "Expected monthly social security start date for long-range projection.",
    },
    {
        "event_name": "inheritance",
        "event_type": "inheritance",
        "scenario": "base",
        "start_date": "2036-01-01",
        "end_date": None,
        "frequency": "one_time",
        "amount": 500000.0,
        "confidence": 0.5,
        "is_deterministic": False,
        "notes": "Non-deterministic one-time inheritance scenario input.",
    },
)

EXPECTED_INVESTABLE_TOTAL_V1: Final[float] = 5516500.0
EXPECTED_CONTROL_TOTALS_V1: Final[dict[str, float]] = {
    "advisor": 5445800.0,
    "passive": 50700.0,
    "aegis": 20000.0,
}
EXPECTED_BUCKET_TOTALS_V1: Final[dict[str, float]] = {
    "income": 4670800.0,
    "growth": 795000.0,
    "safety": 50700.0,
}
