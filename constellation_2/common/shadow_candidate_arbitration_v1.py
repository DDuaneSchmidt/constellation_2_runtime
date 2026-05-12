from __future__ import annotations

import copy
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any

from constellation_2.phaseD.lib.canon_json_v1 import canonical_hash_for_c2_artifact_v1, canonical_json_bytes_v1
from constellation_2.phaseD.lib.validate_against_schema_v1 import validate_against_repo_schema_v1


REPO_ROOT = Path(__file__).resolve().parents[2]
SHADOW_CANDIDATE_ARBITRATION_SCHEMA_RELPATH = "governance/04_DATA/SCHEMAS/C2/REPORTS/shadow_candidate_arbitration.v1.schema.json"

PRIORITY_ORDER = [
    "C2_DEFENSIVE_TAIL_V1",
    "C2_EVENT_DISLOCATION_V1",
    "C2_MARKET_NEUTRAL_SPREAD_V1",
    "C2_MEAN_REVERSION_EQ_V1",
    "C2_CROSS_ASSET_TREND_V1",
    "C2_TREND_EQ_PRIMARY_V1",
    "C2_VOL_INCOME_DEFINED_RISK_V1",
]


class ShadowCandidateArbitrationError(ValueError):
    pass


def shadow_candidate_arbitration_path_v1(*, truth_root: Path, day_utc: str, run_id: str = "") -> Path:
    base = Path(truth_root).resolve() / "reports" / "shadow_candidate_arbitration_v1" / day_utc
    if str(run_id or "").strip():
        return base / str(run_id).strip() / "shadow_candidate_arbitration.v1.json"
    return base / "shadow_candidate_arbitration.v1.json"


def _text(value: Any) -> str:
    return str(value or "").strip()


def _upper(value: Any) -> str:
    return _text(value).upper()


def _list(value: Any) -> list[Any]:
    return list(value) if isinstance(value, list) else []


def _strings(value: Any) -> list[str]:
    return sorted({_text(item) for item in _list(value) if _text(item)})


def _decimal(value: Any) -> Decimal:
    try:
        return Decimal(str(value))
    except (InvalidOperation, ValueError):
        return Decimal("0")


def _decimal_text(value: Any) -> str:
    decimal = _decimal(value)
    return format(decimal.normalize(), "f") if decimal != 0 else "0"


def _int(value: Any, *, default: int = 0) -> int:
    if isinstance(value, bool):
        return default
    try:
        return int(str(value).strip())
    except (TypeError, ValueError):
        return default


def _priority(engine_id: str) -> int:
    try:
        return PRIORITY_ORDER.index(engine_id)
    except ValueError:
        return len(PRIORITY_ORDER)


def _canonical_scalar(value: Any) -> Any:
    if isinstance(value, float):
        return _decimal_text(value)
    if isinstance(value, dict):
        return {str(key): _canonical_scalar(value[key]) for key in sorted(value)}
    if isinstance(value, list):
        return [_canonical_scalar(item) for item in value]
    return value


def _candidate_id(candidate: dict[str, Any]) -> str:
    for key in ("candidate_id", "raw_intent_id", "intent_id"):
        value = _text(candidate.get(key))
        if value:
            return value
    return ":".join(
        part
        for part in (
            _text(candidate.get("engine_id")),
            _upper(candidate.get("symbol_or_pair") or candidate.get("symbol")),
            _text(candidate.get("lineage_hash")),
        )
        if part
    )


def _scoring_rows(portfolio_scoring: dict[str, Any] | None) -> dict[str, dict[str, Any]]:
    if not isinstance(portfolio_scoring, dict):
        return {}
    rows = portfolio_scoring.get("rankings")
    if not isinstance(rows, list):
        rows = portfolio_scoring.get("ranked_intents")
    by_id: dict[str, dict[str, Any]] = {}
    for row in rows if isinstance(rows, list) else []:
        if isinstance(row, dict) and _text(row.get("intent_id")):
            by_id[_text(row.get("intent_id"))] = row
    return by_id


def _production_selected_intent(
    *,
    selected_intent_pointer: dict[str, Any] | None,
    production_arbitration: dict[str, Any] | None,
) -> dict[str, Any]:
    selected: dict[str, Any] = {}
    if isinstance(selected_intent_pointer, dict) and isinstance(selected_intent_pointer.get("selected_intent"), dict):
        selected = dict(selected_intent_pointer["selected_intent"])
    elif isinstance(production_arbitration, dict) and isinstance(production_arbitration.get("selected_intent"), dict):
        selected = dict(production_arbitration["selected_intent"])
    return {
        "intent_id": _text(selected.get("intent_id")),
        "engine_id": _text(selected.get("engine_id") or selected.get("sleeve_id")),
        "symbol": _upper(selected.get("symbol")),
        "source_intent_path": _text(selected.get("intent_path")),
        "source_intent_hash": _text(selected.get("intent_hash")),
    }


def _is_manifest_usable(candidate_manifest: dict[str, Any] | None, *, day_utc: str, run_id: str) -> tuple[bool, str]:
    if not isinstance(candidate_manifest, dict):
        return False, "CANDIDATE_GENERATION_MANIFEST_MISSING"
    if _text(candidate_manifest.get("schema_id")) != "candidate_generation_manifest":
        return False, "CANDIDATE_GENERATION_MANIFEST_SCHEMA_INVALID"
    if _text(candidate_manifest.get("day_utc")) != day_utc:
        return False, "CANDIDATE_GENERATION_MANIFEST_DAY_MISMATCH"
    if run_id and _text(candidate_manifest.get("run_id")) != run_id:
        return False, "CANDIDATE_GENERATION_MANIFEST_RUN_MISMATCH"
    if not isinstance(candidate_manifest.get("candidate_rows"), list):
        return False, "CANDIDATE_GENERATION_MANIFEST_ROWS_MISSING"
    return True, ""


def _is_ledger_usable(sleeve_invocation_ledger: dict[str, Any] | None, *, day_utc: str, run_id: str) -> tuple[bool, str]:
    if not isinstance(sleeve_invocation_ledger, dict):
        return False, "SLEEVE_INVOCATION_LEDGER_MISSING"
    if _text(sleeve_invocation_ledger.get("schema_id")) != "sleeve_invocation_ledger":
        return False, "SLEEVE_INVOCATION_LEDGER_SCHEMA_INVALID"
    if _text(sleeve_invocation_ledger.get("day_utc")) != day_utc:
        return False, "SLEEVE_INVOCATION_LEDGER_DAY_MISMATCH"
    if run_id and _text(sleeve_invocation_ledger.get("run_id")) != run_id:
        return False, "SLEEVE_INVOCATION_LEDGER_RUN_MISMATCH"
    return True, ""


def _ranked_candidates(
    *,
    candidates: list[dict[str, Any]],
    portfolio_scoring: dict[str, Any] | None,
) -> list[dict[str, Any]]:
    scoring_by_intent = _scoring_rows(portfolio_scoring)
    rows: list[dict[str, Any]] = []
    for candidate in candidates:
        raw_intent_id = _text(candidate.get("raw_intent_id"))
        scoring = scoring_by_intent.get(raw_intent_id, {})
        candidate_status = _upper(candidate.get("status")) or "UNKNOWN"
        gate_decision = _upper(candidate.get("portfolio_gate_decision"))
        allowed_by_gate = bool(candidate.get("allowed_by_portfolio_gate"))
        scoring_executable = bool(scoring.get("executable_eligible")) if scoring else False
        executable = (
            candidate_status == "CANDIDATE_CREATED"
            and bool(raw_intent_id)
            and gate_decision == "ALLOW"
            and allowed_by_gate
            and (scoring_executable or not scoring_by_intent)
        )
        rank = _int(scoring.get("rank"), default=0)
        score_total = _decimal_text(scoring.get("score_total"))
        reason_codes = sorted(
            set(
                _strings(candidate.get("reason_codes"))
                + _strings(candidate.get("lifecycle_reason_codes"))
                + _strings(scoring.get("reason_codes"))
            )
        )
        non_executable_reason = ""
        if not executable:
            if candidate_status in {"SUPPRESSED", "SIGNAL_ONLY", "BLOCKED", "NO_SIGNAL"}:
                non_executable_reason = candidate_status
            elif not raw_intent_id:
                non_executable_reason = "RAW_INTENT_ID_MISSING"
            elif gate_decision and gate_decision != "ALLOW":
                non_executable_reason = f"PORTFOLIO_GATE_{gate_decision}"
            elif allowed_by_gate is not True:
                non_executable_reason = "PORTFOLIO_GATE_NOT_ALLOWED"
            elif scoring_by_intent and not scoring:
                non_executable_reason = "PORTFOLIO_SCORING_MISSING"
            elif scoring and not scoring_executable:
                non_executable_reason = "PORTFOLIO_SCORING_NOT_EXECUTABLE"
            else:
                non_executable_reason = _text(candidate.get("rejection_reason")) or "NOT_EXECUTABLE"
        rows.append(
            {
                "candidate_id": _candidate_id(candidate),
                "diagnostic_rank": 0,
                "shadow_executable_rank": 0,
                "executable_candidate": executable,
                "candidate_status": candidate_status,
                "engine_id": _text(candidate.get("engine_id")),
                "symbol_or_pair": _upper(candidate.get("symbol_or_pair")),
                "raw_intent_id": raw_intent_id,
                "raw_intent_path": _text(candidate.get("raw_intent_path")),
                "raw_intent_hash": _text(candidate.get("raw_intent_hash")),
                "portfolio_gate_decision": gate_decision,
                "allowed_by_portfolio_gate": allowed_by_gate,
                "portfolio_score_rank": rank,
                "portfolio_score_total": score_total,
                "portfolio_score_components": _canonical_scalar(scoring.get("score_components")) if isinstance(scoring.get("score_components"), dict) else {},
                "reason_codes": reason_codes,
                "rejection_reason": _text(candidate.get("rejection_reason")),
                "non_executable_reason": non_executable_reason,
                "lineage_hash": _text(candidate.get("lineage_hash")),
            }
        )

    executable_rows = [row for row in rows if row["executable_candidate"]]
    executable_rows.sort(
        key=lambda row: (
            _int(row.get("portfolio_score_rank"), default=999999) or 999999,
            -_decimal(row.get("portfolio_score_total")),
            _priority(_text(row.get("engine_id"))),
            _text(row.get("candidate_id")),
        )
    )
    for index, row in enumerate(executable_rows, start=1):
        row["shadow_executable_rank"] = index

    rows.sort(
        key=lambda row: (
            0 if row["executable_candidate"] else 1,
            _int(row.get("shadow_executable_rank"), default=999999) or 999999,
            _text(row.get("candidate_status")),
            _text(row.get("engine_id")),
            _text(row.get("symbol_or_pair")),
            _text(row.get("candidate_id")),
        )
    )
    for index, row in enumerate(rows, start=1):
        row["diagnostic_rank"] = index
    return rows


def _score_delta(shadow_winner: dict[str, Any], production_selected_intent: dict[str, Any], ranked_candidates: list[dict[str, Any]]) -> str:
    production_id = _text(production_selected_intent.get("intent_id"))
    if not shadow_winner or not production_id:
        return "0"
    production_row = next((row for row in ranked_candidates if _text(row.get("raw_intent_id")) == production_id), {})
    return _decimal_text(_decimal(shadow_winner.get("portfolio_score_total")) - _decimal(production_row.get("portfolio_score_total")))


def build_shadow_candidate_arbitration_v1(
    *,
    day_utc: str,
    environment: str,
    run_id: str,
    produced_at_utc: str,
    candidate_manifest: dict[str, Any] | None,
    sleeve_invocation_ledger: dict[str, Any] | None,
    portfolio_scoring: dict[str, Any] | None = None,
    production_arbitration: dict[str, Any] | None = None,
    selected_intent_pointer: dict[str, Any] | None = None,
    candidate_generation_manifest_path: str = "",
    sleeve_invocation_ledger_path: str = "",
    portfolio_scoring_path: str = "",
    production_arbitration_path: str = "",
    selected_intent_pointer_path: str = "",
) -> dict[str, Any]:
    manifest_ok, manifest_blocker = _is_manifest_usable(candidate_manifest, day_utc=day_utc, run_id=run_id)
    ledger_ok, ledger_blocker = _is_ledger_usable(sleeve_invocation_ledger, day_utc=day_utc, run_id=run_id)
    blocker = manifest_blocker or ledger_blocker
    if not manifest_ok or not ledger_ok:
        payload = {
            "schema_id": "shadow_candidate_arbitration",
            "schema_version": "v1",
            "run_id": run_id,
            "day_utc": day_utc,
            "environment": environment,
            "produced_at_utc": produced_at_utc,
            "status": "DIAGNOSTIC_UNAVAILABLE",
            "canonical_blocker": blocker,
            "candidate_generation_manifest_path": candidate_generation_manifest_path,
            "sleeve_invocation_ledger_path": sleeve_invocation_ledger_path,
            "portfolio_scoring_path": portfolio_scoring_path,
            "production_arbitration_path": production_arbitration_path,
            "selected_intent_pointer_path": selected_intent_pointer_path,
            "ranked_candidates": [],
            "shadow_winner": {},
            "production_selected_intent": _production_selected_intent(
                selected_intent_pointer=selected_intent_pointer,
                production_arbitration=production_arbitration,
            ),
            "winner_matches_production": False,
            "difference_reason": blocker,
            "comparison_diagnostics": {
                "score_delta": "0",
                "missing_candidate_reasons": [blocker],
                "gate_reasons": [],
            },
            "non_authoritative": True,
            "execution_authority_granted": False,
            "order_submission_attempted": False,
            "trading_behavior_changed": False,
            "canonical_json_hash": None,
        }
        payload["canonical_json_hash"] = canonical_hash_for_c2_artifact_v1(payload)
        return payload

    manifest = copy.deepcopy(candidate_manifest)
    ranked = _ranked_candidates(
        candidates=[row for row in _list(manifest.get("candidate_rows")) if isinstance(row, dict)],
        portfolio_scoring=copy.deepcopy(portfolio_scoring) if isinstance(portfolio_scoring, dict) else None,
    )
    shadow_winner = next((row for row in ranked if row.get("executable_candidate")), {})
    production_selected = _production_selected_intent(
        selected_intent_pointer=selected_intent_pointer,
        production_arbitration=production_arbitration,
    )
    production_id = _text(production_selected.get("intent_id"))
    shadow_id = _text(shadow_winner.get("raw_intent_id"))
    winner_matches = bool(shadow_id and production_id and shadow_id == production_id)
    if not shadow_winner:
        difference_reason = "NO_SHADOW_EXECUTABLE_CANDIDATE"
    elif not production_id:
        difference_reason = "PRODUCTION_SELECTED_INTENT_MISSING"
    elif winner_matches:
        difference_reason = "MATCH"
    else:
        difference_reason = "SHADOW_WINNER_DIFFERS_FROM_PRODUCTION"

    gate_reasons = sorted(
        {
            reason
            for row in ranked
            for reason in ([_text(row.get("non_executable_reason"))] + _strings(row.get("reason_codes")))
            if reason
        }
    )
    missing_candidate_reasons = []
    if production_id and not any(_text(row.get("raw_intent_id")) == production_id for row in ranked):
        missing_candidate_reasons.append("PRODUCTION_SELECTED_INTENT_NOT_IN_CANDIDATE_MANIFEST")

    payload = {
        "schema_id": "shadow_candidate_arbitration",
        "schema_version": "v1",
        "run_id": run_id,
        "day_utc": day_utc,
        "environment": environment,
        "produced_at_utc": produced_at_utc,
        "status": "PASS",
        "canonical_blocker": "",
        "candidate_generation_manifest_path": candidate_generation_manifest_path,
        "sleeve_invocation_ledger_path": sleeve_invocation_ledger_path,
        "portfolio_scoring_path": portfolio_scoring_path,
        "production_arbitration_path": production_arbitration_path,
        "selected_intent_pointer_path": selected_intent_pointer_path,
        "ranked_candidates": ranked,
        "shadow_winner": shadow_winner,
        "production_selected_intent": production_selected,
        "winner_matches_production": winner_matches,
        "difference_reason": difference_reason,
        "comparison_diagnostics": {
            "score_delta": _score_delta(shadow_winner, production_selected, ranked),
            "missing_candidate_reasons": missing_candidate_reasons,
            "gate_reasons": gate_reasons,
        },
        "non_authoritative": True,
        "execution_authority_granted": False,
        "order_submission_attempted": False,
        "trading_behavior_changed": False,
        "canonical_json_hash": None,
    }
    payload["canonical_json_hash"] = canonical_hash_for_c2_artifact_v1(payload)
    return payload


def validate_shadow_candidate_arbitration_v1(payload: dict[str, Any]) -> None:
    validate_against_repo_schema_v1(payload, REPO_ROOT, SHADOW_CANDIDATE_ARBITRATION_SCHEMA_RELPATH)


def write_shadow_candidate_arbitration_v1(*, truth_root: Path, payload: dict[str, Any]) -> Path:
    validate_shadow_candidate_arbitration_v1(payload)
    path = shadow_candidate_arbitration_path_v1(
        truth_root=truth_root,
        day_utc=str(payload["day_utc"]),
        run_id=str(payload.get("run_id") or ""),
    )
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(canonical_json_bytes_v1(payload) + b"\n")
    return path
