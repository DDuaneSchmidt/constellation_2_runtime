from __future__ import annotations

import json
from datetime import date
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any, Iterable

from constellation_2.common.paper_session_path_alignment_v1 import (
    resolve_paper_capital_seed_path,
    resolve_paper_capital_seed_root,
)

BLOCKER_FIXABILITY_EXTERNAL = "external/runtime"
BLOCKER_FIXABILITY_REFRESHABLE = "refreshable"
BLOCKER_FIXABILITY_UNKNOWN = "unknown"
BLOCKER_FIXABILITY_VERIFIABLE_ONLY = "verifiable-only"

DAY_READINESS_READY_FOR_DAY = "READY_FOR_DAY"
DAY_READINESS_WAITING_FOR_MARKET_DATA = "WAITING_FOR_MARKET_DATA"
DAY_READINESS_BLOCKED_EXTERNAL = "BLOCKED_EXTERNAL"
DAY_READINESS_BLOCKED_REPO_FIXABLE = "BLOCKED_REPO_FIXABLE"

DAY_READINESS_CLASSIFICATION_REFRESHABLE = "refreshable"
DAY_READINESS_CLASSIFICATION_VERIFIABLE_ONLY = "verifiable_only"
DAY_READINESS_CLASSIFICATION_EXTERNAL_TIME_BOUND = "external_time_bound"

TOMORROW_BLOCKED_EXTERNAL = DAY_READINESS_BLOCKED_EXTERNAL
TOMORROW_BLOCKED_REPO_FIXABLE = DAY_READINESS_BLOCKED_REPO_FIXABLE
TOMORROW_READY = DAY_READINESS_READY_FOR_DAY

_REFRESHABLE_OWNER_FILENAMES = {
    "ensure_paper_capital_seed_v1.py",
    "run_pre_open_materializer_v1.py",
    "run_pipeline_manifest_v1.py",
    "run_ib_api_handshake_spine_v1.py",
    "run_pointer_heads_materialize_v1.py",
    "run_global_kill_switch_v1.py",
    "run_broker_reconciliation_day_v2.py",
    "run_exposure_net_day_v1.py",
    "run_reconciled_trade_state_v1.py",
    "run_sleeve_edge_measurement_v1.py",
    "run_capital_authority_allocation_day_v1.py",
    "run_paper_startup_authorization_convergence_v1.py",
}
_EXTERNAL_REASON_CODES = {
    "BROKER_EVENTS_MISSING",
    "IB_API_HANDSHAKE_NOT_OK",
}
_TIME_BOUND_REASON_CODES = {
    "BROKER_EVENTS_MISSING",
    "STARTUP_MATERIALIZATION_MISSING_DEPENDENCY:DEFAULT_EQUITY_REFERENCE_PRICE_UNAVAILABLE",
}


def _load_json_object(path: Path) -> dict[str, Any]:
    obj = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(obj, dict):
        raise ValueError(f"top_level_not_object path={path}")
    return obj


def _require_money(value: Any, *, path: Path, field: str) -> str:
    raw = str(value or "").strip()
    if not raw:
        raise ValueError(f"missing_money field={field} path={path}")
    try:
        money = Decimal(raw)
    except (InvalidOperation, ValueError) as exc:
        raise ValueError(f"invalid_money field={field} path={path} value={raw!r}") from exc
    if money != money.quantize(Decimal("0.01")):
        raise ValueError(f"invalid_money_quantization field={field} path={path} value={raw!r}")
    return format(money, "f")


def resolve_continuity_paper_seed_usd_v1(*, operator_input_root: Path, target_day: str) -> str:
    normalized_target_day = date.fromisoformat(str(target_day).strip()).isoformat()
    seed_root = resolve_paper_capital_seed_root(operator_input_root=Path(operator_input_root).resolve())
    if not seed_root.exists() or not seed_root.is_dir():
        raise FileNotFoundError(f"paper_capital_seed_root_missing path={seed_root}")

    candidate_days = sorted(
        {
            item.name
            for item in seed_root.iterdir()
            if item.is_dir() and len(item.name) == 10 and item.name < normalized_target_day
        },
        reverse=True,
    )
    if not candidate_days:
        raise FileNotFoundError(
            f"no_prior_paper_capital_seed_found target_day={normalized_target_day} root={seed_root}"
        )

    source_day = candidate_days[0]
    source_path = resolve_paper_capital_seed_path(
        operator_input_root=Path(operator_input_root).resolve(),
        day_utc=source_day,
    )
    if not source_path.exists() or not source_path.is_file():
        raise FileNotFoundError(f"paper_capital_seed_missing_for_prior_day path={source_path}")

    payload = _load_json_object(source_path)
    observed_day = str(payload.get("day_utc") or "").strip()
    if observed_day != source_day:
        raise ValueError(
            f"paper_capital_seed_day_mismatch expected={source_day} observed={observed_day!r} path={source_path}"
        )
    environment = str(payload.get("environment") or "").strip().upper()
    if environment != "PAPER":
        raise ValueError(f"paper_capital_seed_environment_invalid environment={environment!r} path={source_path}")

    cash_total = _require_money(payload.get("cash_total"), path=source_path, field="cash_total")
    nlv_total = _require_money(payload.get("nlv_total"), path=source_path, field="nlv_total")
    if cash_total != nlv_total:
        raise ValueError(
            f"paper_capital_seed_cash_nlv_mismatch cash_total={cash_total} nlv_total={nlv_total} path={source_path}"
        )
    if Decimal(cash_total) < Decimal("0.00"):
        raise ValueError(f"paper_capital_seed_negative_not_allowed value={cash_total} path={source_path}")
    return cash_total


def classify_bootstrap_blocker_fixability_v1(
    *,
    owner_tool: str,
    blocker_class: str,
    reason_codes: Iterable[str],
) -> str:
    normalized_owner = Path(str(owner_tool or "").strip()).name
    normalized_reasons = {str(code).strip() for code in reason_codes if str(code).strip()}
    if any(code in _EXTERNAL_REASON_CODES for code in normalized_reasons):
        return BLOCKER_FIXABILITY_EXTERNAL
    if any(code.startswith("AUTHORIZATION_GATE_NOT_PASS:replay_certification_gate_v1:FAIL") for code in normalized_reasons):
        return BLOCKER_FIXABILITY_EXTERNAL
    if normalized_owner == "run_session_authority_v1.py":
        return BLOCKER_FIXABILITY_VERIFIABLE_ONLY
    if normalized_owner in _REFRESHABLE_OWNER_FILENAMES:
        return BLOCKER_FIXABILITY_REFRESHABLE
    if str(blocker_class or "").strip().upper() == "BLOCKED_STATE" and not normalized_owner:
        return BLOCKER_FIXABILITY_EXTERNAL
    return BLOCKER_FIXABILITY_UNKNOWN


def resolve_day_readiness_automation_path_v1(*, truth_root: Path, target_day: str) -> Path:
    return (
        Path(truth_root).resolve()
        / "reports"
        / "day_readiness_automation_v1"
        / str(target_day).strip()
        / "day_readiness_automation.v1.json"
    ).resolve()


def is_external_time_bound_reason_codes_v1(reason_codes: Iterable[str]) -> bool:
    normalized = {str(code).strip() for code in reason_codes if str(code).strip()}
    return any(code in _TIME_BOUND_REASON_CODES for code in normalized)


def classify_day_readiness_projection_v1(*, blocker_fixability: str, reason_codes: Iterable[str]) -> str:
    if is_external_time_bound_reason_codes_v1(reason_codes) or blocker_fixability == BLOCKER_FIXABILITY_EXTERNAL:
        return DAY_READINESS_CLASSIFICATION_EXTERNAL_TIME_BOUND
    if blocker_fixability == BLOCKER_FIXABILITY_VERIFIABLE_ONLY:
        return DAY_READINESS_CLASSIFICATION_VERIFIABLE_ONLY
    return DAY_READINESS_CLASSIFICATION_REFRESHABLE


def derive_tomorrow_readiness_state_v1(
    *,
    bootstrap_status: str,
    blocker_fixability: str,
    reason_codes: Iterable[str] = (),
    downstream_ready: bool = True,
) -> str:
    if str(bootstrap_status or "").strip().upper() == "READY" and bool(downstream_ready):
        return TOMORROW_READY
    if is_external_time_bound_reason_codes_v1(reason_codes):
        return DAY_READINESS_WAITING_FOR_MARKET_DATA
    if blocker_fixability == BLOCKER_FIXABILITY_EXTERNAL:
        return TOMORROW_BLOCKED_EXTERNAL
    return TOMORROW_BLOCKED_REPO_FIXABLE
