from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Iterable

from constellation_2.phaseD.lib.canon_json_v1 import (
    canonical_hash_for_c2_artifact_v1,
    canonical_json_bytes_v1,
    canonical_sha256_hex_v1,
)
from constellation_2.phaseD.lib.validate_against_schema_v1 import validate_against_repo_schema_v1
from ops.aegis.research_lab.research_validation_engine_v1 import resolve_research_promotion_eligibility_v1


REPO_ROOT = Path(__file__).resolve().parents[2]
SLEEVE_INVOCATION_SCHEMA_RELPATH = "governance/04_DATA/SCHEMAS/C2/REPORTS/sleeve_invocation_ledger.v1.schema.json"
CANDIDATE_MANIFEST_SCHEMA_RELPATH = "governance/04_DATA/SCHEMAS/C2/REPORTS/candidate_generation_manifest.v1.schema.json"


def sleeve_invocation_ledger_path_v1(*, truth_root: Path, day_utc: str, run_id: str = "") -> Path:
    base = Path(truth_root).resolve() / "reports" / "sleeve_invocation_ledger_v1" / day_utc
    if str(run_id or "").strip():
        return base / str(run_id).strip() / "sleeve_invocation_ledger.v1.json"
    return base / "sleeve_invocation_ledger.v1.json"


def candidate_generation_manifest_path_v1(*, truth_root: Path, day_utc: str, run_id: str = "") -> Path:
    base = Path(truth_root).resolve() / "reports" / "candidate_generation_manifest_v1" / day_utc
    if str(run_id or "").strip():
        return base / str(run_id).strip() / "candidate_generation_manifest.v1.json"
    return base / "candidate_generation_manifest.v1.json"


def _text(value: Any) -> str:
    return str(value or "").strip()


def _upper(value: Any) -> str:
    return _text(value).upper()


def _list(value: Any) -> list[Any]:
    return list(value) if isinstance(value, list) else []


def _strings(value: Any) -> list[str]:
    return sorted({_text(item) for item in _list(value) if _text(item)})


def _symbols_for_outcome(outcome: dict[str, Any]) -> list[str]:
    for key in ("producer_requested_symbols", "active_symbol_universe", "allowed_symbols", "registry_allowed_symbols"):
        symbols = [_upper(item) for item in _list(outcome.get(key)) if _upper(item)]
        if symbols:
            return sorted(set(symbols))
    for key in ("intent_symbol", "producer_requested_symbol", "artifact_symbol"):
        symbol = _upper(outcome.get(key))
        if symbol:
            return [symbol]
    output_intents = _list(outcome.get("output_intents"))
    symbols = [_upper(row.get("symbol")) for row in output_intents if isinstance(row, dict) and _upper(row.get("symbol"))]
    return sorted(set(symbols))


def _output_paths(outcome: dict[str, Any]) -> list[str]:
    paths = [_text(outcome.get("artifact_path")), _text(outcome.get("output_intent_path")), _text(outcome.get("intent_artifact_path"))]
    for row in _list(outcome.get("output_intents")):
        if isinstance(row, dict):
            paths.append(_text(row.get("intent_path")))
    return sorted({path for path in paths if path})


def _input_paths(outcome: dict[str, Any]) -> list[str]:
    paths = []
    for row in _list(outcome.get("input_artifacts")):
        if isinstance(row, dict):
            paths.append(_text(row.get("path")))
    manifest_check = outcome.get("market_data_manifest_check")
    if isinstance(manifest_check, dict):
        for key in ("local_manifest_path", "canonical_manifest_path"):
            paths.append(_text(manifest_check.get(key)))
    return sorted({path for path in paths if path})


def _row_lineage_hash(row: dict[str, Any]) -> str:
    return canonical_sha256_hex_v1(row)


def _candidate_id(*, day_utc: str, run_id: str, engine_id: str, symbol_or_pair: str, intent_id: str, status: str) -> str:
    return canonical_sha256_hex_v1(
        {
            "day_utc": day_utc,
            "engine_id": engine_id,
            "intent_id": intent_id,
            "run_id": run_id,
            "status": status,
            "symbol_or_pair": symbol_or_pair,
        }
    )[:24]


def _status_from_outcome(outcome: dict[str, Any]) -> str:
    status = _upper(outcome.get("status"))
    if status == "INTENT_CREATED":
        return "CANDIDATE_CREATED"
    if status == "NO_INTENT":
        return "NO_SIGNAL"
    if status == "BLOCKED":
        return "BLOCKED"
    if status in {"DISABLED", "FILTERED_OUT"}:
        return "SUPPRESSED"
    return status or "UNKNOWN"



def _candidate_mode_fields(run_mode: str) -> dict[str, Any]:
    mode = _upper(run_mode) or "INTRADAY_OPERATIONAL"
    intraday = mode == "INTRADAY_OPERATIONAL"
    return {
        "candidate_data_status": "PROVISIONAL_CANDIDATE" if intraday else "FINAL_EOD_CERTIFIED_CANDIDATE",
        "source_data_mode": mode,
        "final_eod_certification_status": "PENDING" if intraday else "PASS",
        "final_eod_certification_pending": intraday,
        "manual_capture_eligible": not intraday,
    }


def _market_data_lineage_fields(truth_root: Path, day_utc: str, run_mode: str) -> dict[str, Any]:
    path = Path(truth_root).resolve() / "reports" / "aegis_market_data_v1" / day_utc / "market_data.v1.json"
    payload: dict[str, Any] = {}
    if path.exists():
        try:
            loaded = json.loads(path.read_text(encoding="utf-8"))
            payload = loaded if isinstance(loaded, dict) else {}
        except Exception:
            payload = {}
    mode = _upper(run_mode) or "INTRADAY_OPERATIONAL"
    certification_state = _upper(payload.get("certification_state"))
    final_status = _upper(payload.get("final_eod_certification_status"))
    if not certification_state:
        certification_state = "CERTIFIED" if mode == "FINAL_EOD_CERTIFIED" or final_status in {"VALID", "PASS", "CERTIFIED"} else "CERTIFICATION_PENDING"
    candidate_lane = _upper(payload.get("candidate_lane")) or ("CERTIFIED" if certification_state == "CERTIFIED" else "PROVISIONAL")
    snapshot_ids = payload.get("input_market_data_snapshot_ids") if isinstance(payload.get("input_market_data_snapshot_ids"), list) else []
    if not snapshot_ids and str(payload.get("market_data_snapshot_id") or "").strip():
        snapshot_ids = [str(payload.get("market_data_snapshot_id"))]
    return {
        "candidate_lane": candidate_lane,
        "candidate_visibility_lane": candidate_lane,
        "certification_state": certification_state,
        "certification_label": "CERTIFIED" if candidate_lane == "CERTIFIED" and certification_state == "CERTIFIED" else "NON_CERTIFIED",
        "input_market_data_snapshot_ids": [str(item) for item in snapshot_ids if str(item or "").strip()],
        "market_data_snapshot_path": str(payload.get("market_data_snapshot_path") or ""),
        "market_data_snapshot_hash": str(payload.get("market_data_snapshot_hash") or ""),
        "execution_eligible": candidate_lane == "CERTIFIED" and certification_state == "CERTIFIED",
    }


def _apply_candidate_lane_fields(rows: list[dict[str, Any]], fields: dict[str, Any]) -> list[dict[str, Any]]:
    enriched: list[dict[str, Any]] = []
    execution_eligible = bool(fields.get("execution_eligible") is True)
    for row in rows:
        next_row = dict(row)
        next_row["candidate_lane"] = str(fields.get("candidate_lane") or "PROVISIONAL")
        next_row["certification_state"] = str(fields.get("certification_state") or "CERTIFICATION_PENDING")
        next_row["certification_label"] = str(fields.get("certification_label") or "NON_CERTIFIED")
        next_row["execution_eligible"] = execution_eligible
        next_row["read_only"] = not execution_eligible
        next_row["input_market_data_snapshot_ids"] = list(fields.get("input_market_data_snapshot_ids") or [])
        next_row["manual_capture_eligible"] = execution_eligible
        next_row["lineage_hash"] = _row_lineage_hash({key: value for key, value in next_row.items() if key != "lineage_hash"})
        enriched.append(next_row)
    return enriched

def _candidate_rows_from_outcome(*, day_utc: str, run_id: str, outcome: dict[str, Any], truth_root: Path, run_mode: str = "") -> list[dict[str, Any]]:
    engine_id = _text(outcome.get("engine_id") or outcome.get("sleeve_id"))
    output_intents = [row for row in _list(outcome.get("output_intents")) if isinstance(row, dict)]
    reason_codes = _strings(outcome.get("reason_codes"))
    lifecycle_reason_codes = _strings(outcome.get("lifecycle_reason_codes"))
    input_paths = _input_paths(outcome)
    output_paths = _output_paths(outcome)
    research_gate = _research_validation_fields_for_outcome(truth_root=truth_root, day_utc=day_utc, outcome=outcome)
    if output_intents:
        rows: list[dict[str, Any]] = []
        for intent in output_intents:
            symbol = _upper(intent.get("symbol") or outcome.get("intent_symbol"))
            intent_id = _text(intent.get("intent_id"))
            row = {
                "candidate_id": _candidate_id(day_utc=day_utc, run_id=run_id, engine_id=engine_id, symbol_or_pair=symbol, intent_id=intent_id, status="CANDIDATE_CREATED"),
                "engine_id": engine_id,
                "symbol_or_pair": symbol,
                "status": "CANDIDATE_CREATED",
                "reason_codes": reason_codes,
                "rejection_reason": "",
                "raw_intent_id": intent_id,
                "raw_intent_path": _text(intent.get("intent_path")),
                "raw_intent_hash": _text(intent.get("intent_hash")),
                "lifecycle_decision": _text(outcome.get("lifecycle_decision")),
                "lifecycle_reason_codes": lifecycle_reason_codes,
                "portfolio_gate_decision": "",
                "allowed_by_portfolio_gate": False,
                "input_artifact_paths": input_paths,
                "output_artifact_paths": output_paths,
                **research_gate,
                **_candidate_mode_fields(run_mode),
            }
            row = _apply_research_validation_to_candidate_row(row)
            row["lineage_hash"] = _row_lineage_hash(row)
            rows.append(row)
        return rows

    symbols = _symbols_for_outcome(outcome) or [""]
    status = _status_from_outcome(outcome)
    rows = []
    for symbol in symbols:
        row = {
            "candidate_id": _candidate_id(day_utc=day_utc, run_id=run_id, engine_id=engine_id, symbol_or_pair=symbol, intent_id="", status=status),
            "engine_id": engine_id,
            "symbol_or_pair": symbol,
            "status": status,
            "reason_codes": reason_codes,
            "rejection_reason": _text(outcome.get("canonical_blocker")),
            "raw_intent_id": "",
            "raw_intent_path": "",
            "raw_intent_hash": "",
            "lifecycle_decision": _text(outcome.get("lifecycle_decision")),
            "lifecycle_reason_codes": lifecycle_reason_codes,
            "portfolio_gate_decision": "",
            "allowed_by_portfolio_gate": False,
            "input_artifact_paths": input_paths,
            "output_artifact_paths": output_paths,
            **research_gate,
            **_candidate_mode_fields(run_mode),
        }
        row = _apply_research_validation_to_candidate_row(row)
        row["lineage_hash"] = _row_lineage_hash(row)
        rows.append(row)
    return rows



def _research_hypothesis_id_from_outcome(outcome: dict[str, Any]) -> str:
    for key in ("hypothesis_id", "source_hypothesis_id", "research_hypothesis_id"):
        text = _text(outcome.get(key))
        if text:
            return text
    for row in _list(outcome.get("output_intents")):
        if isinstance(row, dict):
            for key in ("hypothesis_id", "source_hypothesis_id", "research_hypothesis_id"):
                text = _text(row.get(key))
                if text:
                    return text
    return ""


def _research_validation_fields_for_outcome(*, truth_root: Path, day_utc: str, outcome: dict[str, Any]) -> dict[str, Any]:
    hypothesis_id = _research_hypothesis_id_from_outcome(outcome)
    hypothesis_version = _text(outcome.get("hypothesis_version")) or "v1"
    if not hypothesis_id:
        return {
            "research_validation_required": False,
            "hypothesis_id": "",
            "hypothesis_version": "",
            "research_validation_result_id": "",
            "research_promotion_gate_status": "NOT_RESEARCH_DERIVED",
            "research_promotion_gate_hash": "",
            "research_validation_enforcement_status": "NOT_RESEARCH_DERIVED",
        }
    return resolve_research_promotion_eligibility_v1(
        truth_root=truth_root,
        day_utc=day_utc,
        hypothesis_id=hypothesis_id,
        hypothesis_version=hypothesis_version,
    )


def _apply_research_validation_to_candidate_row(row: dict[str, Any]) -> dict[str, Any]:
    if not row.get("research_validation_required"):
        return row
    if row.get("research_validation_enforcement_status") == "PASS":
        return row
    next_row = dict(row)
    next_row["status"] = "SUPPRESSED"
    reason = _text(next_row.get("research_validation_enforcement_status")) or "REJECTED_RESEARCH_VALIDATION_MISSING"
    next_row["rejection_reason"] = reason
    next_row["reason_codes"] = sorted(set(_strings(next_row.get("reason_codes")) + [reason]))
    next_row["manual_capture_eligible"] = False
    next_row["execution_eligible"] = False
    next_row["read_only"] = True
    return next_row

def _apply_portfolio_gate(candidate_rows: list[dict[str, Any]], portfolio_gate: dict[str, Any] | None) -> list[dict[str, Any]]:
    if not isinstance(portfolio_gate, dict):
        return candidate_rows
    decisions = [row for row in _list(portfolio_gate.get("decisions")) if isinstance(row, dict)]
    by_intent_id = {_text(row.get("raw_intent_id")): row for row in decisions if _text(row.get("raw_intent_id"))}
    by_engine_symbol = {
        (_text(row.get("sleeve_id")), _upper(row.get("raw_intent_symbol"))): row
        for row in decisions
        if _text(row.get("sleeve_id"))
    }
    enriched: list[dict[str, Any]] = []
    for row in candidate_rows:
        decision = by_intent_id.get(_text(row.get("raw_intent_id"))) or by_engine_symbol.get((_text(row.get("engine_id")), _upper(row.get("symbol_or_pair"))))
        next_row = dict(row)
        if decision:
            gate_decision = _upper(decision.get("portfolio_gate_decision"))
            next_row["portfolio_gate_decision"] = gate_decision
            next_row["allowed_by_portfolio_gate"] = bool(decision.get("allowed_by_portfolio_gate"))
            next_row["reason_codes"] = sorted(set(_strings(next_row.get("reason_codes")) + _strings(decision.get("reason_codes"))))
            if gate_decision == "SIGNAL_ONLY":
                next_row["status"] = "SIGNAL_ONLY"
                next_row["rejection_reason"] = "PORTFOLIO_GATE_SIGNAL_ONLY"
            elif gate_decision == "SUPPRESS" and _text(next_row.get("raw_intent_id")):
                next_row["status"] = "SUPPRESSED"
                next_row["rejection_reason"] = "PORTFOLIO_GATE_SUPPRESSED"
            elif gate_decision == "DEGRADED":
                next_row["status"] = "BLOCKED"
                next_row["rejection_reason"] = "PORTFOLIO_GATE_DEGRADED"
            elif gate_decision == "ALLOW" and _text(next_row.get("raw_intent_id")):
                next_row["status"] = "CANDIDATE_CREATED"
                next_row["rejection_reason"] = ""
        next_row["lineage_hash"] = _row_lineage_hash({key: value for key, value in next_row.items() if key != "lineage_hash"})
        enriched.append(next_row)
    return enriched


def build_sleeve_invocation_ledger_v1(
    *,
    day_utc: str,
    environment: str,
    truth_root: Path,
    outcomes: Iterable[dict[str, Any]],
    run_id: str,
    scheduled_run_at_utc: str,
    produced_at_utc: str,
) -> dict[str, Any]:
    rows: list[dict[str, Any]] = []
    for outcome in outcomes:
        if not isinstance(outcome, dict):
            continue
        engine_id = _text(outcome.get("engine_id") or outcome.get("sleeve_id"))
        symbols = _symbols_for_outcome(outcome) or [""]
        for symbol in symbols:
            row = {
                "run_id": run_id,
                "day_utc": day_utc,
                "scheduled_run_at_utc": scheduled_run_at_utc,
                "engine_id": engine_id,
                "symbol_or_pair": symbol,
                "status": _upper(outcome.get("status")) or "UNKNOWN",
                "reason_codes": _strings(outcome.get("reason_codes")),
                "input_artifact_paths": _input_paths(outcome),
                "output_artifact_paths": _output_paths(outcome),
                "produced_at_utc": produced_at_utc,
            }
            row["lineage_hash"] = _row_lineage_hash(row)
            rows.append(row)
    rows.sort(key=lambda row: (str(row["engine_id"]), str(row["symbol_or_pair"]), str(row["status"])))
    payload = {
        "schema_id": "sleeve_invocation_ledger",
        "schema_version": "v1",
        "run_id": run_id,
        "day_utc": day_utc,
        "environment": environment,
        "scheduled_run_at_utc": scheduled_run_at_utc,
        "produced_at_utc": produced_at_utc,
        "invocations": rows,
        "invocation_count": len(rows),
        "execution_authority_granted": False,
        "order_submission_attempted": False,
        "trading_behavior_changed": False,
        "canonical_json_hash": None,
    }
    payload["canonical_json_hash"] = canonical_hash_for_c2_artifact_v1(payload)
    return payload


def build_candidate_generation_manifest_v1(
    *,
    day_utc: str,
    environment: str,
    truth_root: Path,
    outcomes: Iterable[dict[str, Any]],
    run_id: str,
    produced_at_utc: str,
    source_rollup_path: str = "",
    portfolio_gate: dict[str, Any] | None = None,
    run_mode: str = "INTRADAY_OPERATIONAL",
) -> dict[str, Any]:
    rows: list[dict[str, Any]] = []
    for outcome in outcomes:
        if isinstance(outcome, dict):
            rows.extend(_candidate_rows_from_outcome(day_utc=day_utc, run_id=run_id, outcome=outcome, truth_root=Path(truth_root), run_mode=run_mode))
    rows = _apply_portfolio_gate(rows, portfolio_gate)
    lineage_fields = _market_data_lineage_fields(Path(truth_root), day_utc, run_mode)
    rows = _apply_candidate_lane_fields(rows, lineage_fields)
    rows.sort(key=lambda row: (str(row["engine_id"]), str(row["symbol_or_pair"]), str(row["candidate_id"])))
    status_counts: dict[str, int] = {}
    for row in rows:
        status = _text(row.get("status")) or "UNKNOWN"
        status_counts[status] = status_counts.get(status, 0) + 1
    payload = {
        "schema_id": "candidate_generation_manifest",
        "schema_version": "v1",
        "run_id": run_id,
        "day_utc": day_utc,
        "environment": environment,
        "run_mode": _upper(run_mode) or "INTRADAY_OPERATIONAL",
        "market_data_mode": _upper(run_mode) or "INTRADAY_OPERATIONAL",
        "candidate_generation_label": "FINAL_EOD_CERTIFIED" if lineage_fields["execution_eligible"] else "NON_CERTIFIED",
        "candidate_lane": str(lineage_fields.get("candidate_lane") or "PROVISIONAL"),
        "candidate_visibility_lane": str(lineage_fields.get("candidate_visibility_lane") or "PROVISIONAL"),
        "certification_state": str(lineage_fields.get("certification_state") or "CERTIFICATION_PENDING"),
        "certification_label": str(lineage_fields.get("certification_label") or "NON_CERTIFIED"),
        "input_market_data_snapshot_ids": list(lineage_fields.get("input_market_data_snapshot_ids") or []),
        "market_data_snapshot_path": str(lineage_fields.get("market_data_snapshot_path") or ""),
        "market_data_snapshot_hash": str(lineage_fields.get("market_data_snapshot_hash") or ""),
        "execution_eligible": bool(lineage_fields.get("execution_eligible") is True),
        "read_only": not bool(lineage_fields.get("execution_eligible") is True),
        "final_eod_certification_status": "PASS" if lineage_fields["execution_eligible"] else "PENDING",
        "produced_at_utc": produced_at_utc,
        "source_rollup_path": source_rollup_path,
        "portfolio_activation_gate_path": _text(portfolio_gate.get("artifact_path")) if isinstance(portfolio_gate, dict) else "",
        "candidate_rows": rows,
        "summary": {
            "candidate_count": len(rows),
            "status_counts": {key: status_counts[key] for key in sorted(status_counts)},
        },
        "selected_intent_pointer_authoritative": True,
        "execution_authority_granted": False,
        "order_submission_attempted": False,
        "trading_behavior_changed": False,
        "certified_candidate_promotion_required": not bool(lineage_fields.get("execution_eligible") is True),
        "execution_firewall_required": True,
        "canonical_json_hash": None,
    }
    payload["canonical_json_hash"] = canonical_hash_for_c2_artifact_v1(payload)
    return payload


def validate_sleeve_invocation_ledger_v1(payload: dict[str, Any]) -> None:
    validate_against_repo_schema_v1(payload, REPO_ROOT, SLEEVE_INVOCATION_SCHEMA_RELPATH)


def validate_candidate_generation_manifest_v1(payload: dict[str, Any]) -> None:
    validate_against_repo_schema_v1(payload, REPO_ROOT, CANDIDATE_MANIFEST_SCHEMA_RELPATH)


def write_sleeve_invocation_ledger_v1(*, truth_root: Path, payload: dict[str, Any]) -> Path:
    validate_sleeve_invocation_ledger_v1(payload)
    path = sleeve_invocation_ledger_path_v1(truth_root=truth_root, day_utc=str(payload["day_utc"]), run_id=str(payload.get("run_id") or ""))
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(canonical_json_bytes_v1(payload) + b"\n")
    return path


def write_candidate_generation_manifest_v1(*, truth_root: Path, payload: dict[str, Any]) -> Path:
    validate_candidate_generation_manifest_v1(payload)
    path = candidate_generation_manifest_path_v1(truth_root=truth_root, day_utc=str(payload["day_utc"]), run_id=str(payload.get("run_id") or ""))
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(canonical_json_bytes_v1(payload) + b"\n")
    return path
