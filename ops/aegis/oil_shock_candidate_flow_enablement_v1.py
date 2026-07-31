from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Mapping

from ops.aegis.intelligence_common_v1 import read_json_v1, write_json_v1
from ops.aegis.oil_shock_candidate_flow_v1 import OIL_HYPOTHESIS_ID, OIL_NAME, oil_shock_candidate_flow_path_v1
from ops.aegis.oil_shock_candidate_producer_v1 import ENGINE_ID, oil_shock_candidate_producer_path_v1
from ops.aegis.oil_shock_candidate_construction_v1 import oil_shock_candidate_construction_path_v1
from ops.aegis.research_mapping_rules_v1 import file_hash_v1, report_path_v1, stable_hash_v1, text_v1

FAMILY = "aegis_oil_shock_candidate_flow_enablement_v1"
FILENAME = "oil_shock_candidate_flow_enablement.v1.json"
POLICY_VERSION = "AEGIS_OIL_SHOCK_CANDIDATE_FLOW_ENABLEMENT_V1"

SAFETY = {
    "research_only": True,
    "no_broker_execution": True,
    "no_trade_advice": True,
    "no_live_trading": True,
    "no_real_capital": True,
    "no_autonomous_execution": True,
    "no_order_management": True,
    "no_forced_candidate_creation": True,
    "no_candidate_contract_bypass": True,
    "no_entry_price_certification_bypass": True,
    "no_paper_lifecycle_bypass": True,
    "no_outcome_validation_bypass": True,
    "candidate_created_by_this_artifact": False,
    "paper_observation_created_by_this_artifact": False,
    "broker_execution_allowed": False,
    "trade_advice_allowed": False,
    "live_trading_allowed": False,
    "real_capital_allowed": False,
    "autonomous_execution_allowed": False,
    "safety_gates_changed": False,
}


def oil_shock_candidate_flow_enablement_path_v1(*, truth_root: Path | str, day_utc: str) -> Path:
    return report_path_v1(truth_root, FAMILY, str(day_utc), FILENAME)


def build_oil_shock_candidate_flow_enablement_v1(*, truth_root: Path | str, repo_root: Path | str, day_utc: str) -> dict[str, Any]:
    root = Path(truth_root).expanduser().resolve()
    repo = Path(repo_root).expanduser().resolve()
    paths = _paths(root, repo, str(day_utc))
    payloads = {key: read_json_v1(path) for key, path in paths.items() if path.suffix == ".json"}
    producer_tool = repo / "ops/tools/build_aegis_oil_shock_candidate_producer_v1.py"
    producer_registered = producer_tool.exists()
    producer = payloads.get("producer", {}) if isinstance(payloads.get("producer"), Mapping) else {}
    flow = payloads.get("flow", {}) if isinstance(payloads.get("flow"), Mapping) else {}
    candidate_contracts = payloads.get("candidate_contracts", {}) if isinstance(payloads.get("candidate_contracts"), Mapping) else {}
    flow_row = flow.get("oil_shock") if isinstance(flow.get("oil_shock"), Mapping) else {}
    raw_signal_count = len(producer.get("output_intents") or [])
    candidate_count = _oil_shock_candidate_contract_count(candidate_contracts, flow_row)
    required_fields = [str(item) for item in producer.get("required_evidence_fields") or flow_row.get("required_evidence_fields") or []]
    available_fields = [str(item) for item in producer.get("available_evidence_fields") or flow_row.get("available_evidence_fields") or []]
    missing_fields = [str(item) for item in producer.get("missing_evidence_fields") or flow_row.get("missing_evidence_fields") or []]
    construction = payloads.get("construction", {}) if isinstance(payloads.get("construction"), Mapping) else {}
    missing_construction_fields = [str(item) for item in producer.get("missing_construction_fields") or construction.get("missing_construction_fields") or flow_row.get("missing_construction_fields") or [] if str(item)]
    missing_market_symbols = [str(item).upper() for item in producer.get("missing_market_symbols") or [] if str(item)]
    producer_status = text_v1(producer.get("producer_status") or flow_row.get("candidate_producer_status"))
    exact_blocker = text_v1(producer.get("exact_blocker") or flow_row.get("exact_blocker"))

    classification, blocker_code, owner, next_step, message = _classification(
        producer_registered=producer_registered,
        producer=producer,
        producer_status=producer_status,
        exact_blocker=exact_blocker,
        candidate_count=candidate_count,
        raw_signal_count=raw_signal_count,
        missing_fields=missing_fields,
        missing_market_symbols=missing_market_symbols,
    )
    row = {
        "hypothesis_id": text_v1(producer.get("hypothesis_id") or flow_row.get("hypothesis_id") or OIL_HYPOTHESIS_ID),
        "hypothesis_name": text_v1(producer.get("hypothesis_name") or flow_row.get("hypothesis_name") or OIL_NAME),
        "producer_status": producer_status or ("REGISTERED" if producer_registered else "PRODUCER_MISSING"),
        "producer_registered": producer_registered,
        "producer_run_status": "RAN" if producer else "NOT_RUN",
        "candidate_flow_status": classification,
        "candidate_count": candidate_count,
        "raw_signal_count": raw_signal_count,
        "required_evidence_fields": required_fields,
        "available_evidence_fields": sorted(set(available_fields)),
        "missing_evidence_fields": sorted(set(missing_fields)),
        "missing_construction_fields": sorted(set(missing_construction_fields)),
        "candidate_construction_status": text_v1(construction.get("candidate_construction_status") or producer.get("candidate_construction_status") or flow_row.get("candidate_construction_status")),
        "missing_market_symbols": missing_market_symbols,
        "required_market_symbols": [str(item).upper() for item in producer.get("required_market_symbols") or [] if str(item)],
        "available_market_symbols": [str(item).upper() for item in producer.get("available_market_symbols") or [] if str(item)],
        "blocker_code": blocker_code,
        "blocker_owner": owner,
        "david_action_required": False,
        "next_expected_step": next_step,
        "ui_message": message,
        "source_artifact_paths": [str(path) for path in paths.values()],
        "source_artifact_hashes": {key: file_hash_v1(path) for key, path in paths.items()},
        "computed_at_utc": _now(),
    }
    artifact = {
        "schema_id": "aegis_oil_shock_candidate_flow_enablement",
        "schema_version": "v1",
        "artifact_id": FAMILY,
        "day_utc": str(day_utc),
        "policy_version": POLICY_VERSION,
        "oil_shock": row,
        **row,
        **SAFETY,
        "safety": dict(SAFETY),
    }
    artifact["deterministic_rerun_id"] = stable_hash_v1({"day_utc": str(day_utc), "policy_version": POLICY_VERSION, "source_hashes": row["source_artifact_hashes"]})
    artifact["content_hash"] = stable_hash_v1(_without_generated(artifact))
    return artifact


def write_oil_shock_candidate_flow_enablement_v1(*, truth_root: Path | str, day_utc: str, payload: dict[str, Any] | None = None, repo_root: Path | str | None = None) -> Path:
    body = payload or build_oil_shock_candidate_flow_enablement_v1(truth_root=truth_root, repo_root=repo_root or Path.cwd(), day_utc=day_utc)
    return write_json_v1(oil_shock_candidate_flow_enablement_path_v1(truth_root=truth_root, day_utc=day_utc), body)


def _paths(root: Path, repo: Path, day: str) -> dict[str, Path]:
    return {
        "producer": oil_shock_candidate_producer_path_v1(truth_root=root, day_utc=day),
        "construction": oil_shock_candidate_construction_path_v1(truth_root=root, day_utc=day),
        "flow": oil_shock_candidate_flow_path_v1(truth_root=root, day_utc=day),
        "candidate_diagnostics": report_path_v1(root, "aegis_candidate_generation_diagnostics_v1", day, "candidate_generation_diagnostics.v1.json"),
        "candidate_contracts": report_path_v1(root, "aegis_candidate_contracts_v1", day, "candidate_contracts.v1.json"),
        "candidate_lifecycle": report_path_v1(root, "aegis_candidate_to_paper_lifecycle_v1", day, "candidate_to_paper_lifecycle.v1.json"),
        "entry_price_certification": report_path_v1(root, "aegis_entry_reference_price_certification_v1", day, "entry_reference_price_certification.v1.json"),
        "outcome_registry": report_path_v1(root, "aegis_outcome_registry_v1", day, "outcome_registry.v1.json"),
        "validation_samples": report_path_v1(root, "aegis_validation_samples_v1", day, "validation_samples.v1.json"),
        "producer_tool": repo / "ops/tools/build_aegis_oil_shock_candidate_producer_v1.py",
    }


def _classification(*, producer_registered: bool, producer: Mapping[str, Any], producer_status: str, exact_blocker: str, candidate_count: int, raw_signal_count: int, missing_fields: list[str], missing_market_symbols: list[str]) -> tuple[str, str, str, str, str]:
    status = (producer_status or exact_blocker).upper()
    if not producer_registered:
        return ("PRODUCER_MISSING", "PRODUCER_MISSING", "AEGIS_SYSTEM", "implement deterministic Oil Shock producer shell, then rerun candidate producer", "Oil Shock producer is missing. Aegis system repair required. No David action required.")
    if not producer:
        return ("PRODUCER_MISSING", "PRODUCER_MISSING", "AEGIS_SYSTEM", "run deterministic Oil Shock candidate producer", "Oil Shock producer is missing. Aegis system repair required. No David action required.")
    if raw_signal_count > 0 or candidate_count > 0 or status == "VALID_CANDIDATE_SIGNAL":
        return ("CANDIDATE_FLOW_STARTED", "NONE", "NONE", "route raw signal through candidate contracts, entry-price certification, paper lifecycle, outcome registry, and validation eligibility", "Oil Shock candidate flow started through the normal candidate pipeline.")
    if status == "NO_MARKET_SETUP" or exact_blocker.upper() == "NO_MARKET_SETUP":
        return ("WAITING_FOR_MARKET_CONDITIONS", "NO_MARKET_SETUP", "MARKET_CONDITIONS", "wait for qualifying Oil Shock market setup, then rerun candidate producer", "Oil Shock producer ran. No qualifying market setup yet. No David action required.")
    if missing_market_symbols or "MISSING_MARKET_DATA" in [str(item).upper() for item in producer.get("reason_codes") or []] or status in {"MISSING_DATA", "SYSTEM_DATA_PIPELINE_REQUIRED"}:
        return ("SYSTEM_DATA_PIPELINE_REQUIRED", "SYSTEM_DATA_PIPELINE_REQUIRED", "AEGIS_SYSTEM", "repair or populate system market-data evidence for: " + (", ".join(missing_market_symbols) if missing_market_symbols else "Oil Shock required symbols"), "Oil Shock requires system market-data evidence. No David action required unless Aegis creates a source action.")
    if missing_fields:
        return ("SYSTEM_DATA_PIPELINE_REQUIRED", "SYSTEM_DATA_PIPELINE_REQUIRED", "AEGIS_SYSTEM", "repair missing deterministic evidence fields: " + ", ".join(missing_fields), "Oil Shock requires system evidence fields. No David action required unless Aegis creates a source action.")
    if status == "POLICY_INCOMPLETE":
        return ("POLICY_INCOMPLETE", "POLICY_INCOMPLETE", "AEGIS_SYSTEM", "complete Oil Shock candidate construction policy and governed paper-tracking setup before candidate generation", "Oil Shock candidate construction is incomplete. Aegis system repair required. No David action required.")
    if status == "CANDIDATE_CONSTRUCTION_INCOMPLETE":
        return ("CANDIDATE_CONSTRUCTION_INCOMPLETE", "CANDIDATE_CONSTRUCTION_INCOMPLETE", "AEGIS_SYSTEM", "complete deterministic candidate construction policy before candidate generation", "Oil Shock candidate construction is incomplete. Aegis system repair required. No David action required.")
    return ("SYSTEM_DATA_PIPELINE_REQUIRED", "SYSTEM_DATA_PIPELINE_REQUIRED", "AEGIS_SYSTEM", "inspect deterministic Oil Shock producer evidence and rerun candidate flow", "Oil Shock requires system evidence. No David action required unless Aegis creates a source action.")


def _oil_shock_candidate_contract_count(candidate_contracts: Mapping[str, Any], flow_row: Mapping[str, Any]) -> int:
    rows = candidate_contracts.get("candidate_contracts")
    if isinstance(rows, list):
        oil_rows = [
            row for row in rows
            if isinstance(row, Mapping)
            and (
                text_v1(row.get("hypothesis_id")) == OIL_HYPOTHESIS_ID
                or text_v1(row.get("hypothesis_name")) == OIL_NAME
                or text_v1(row.get("source_artifact")).endswith("oil_shock_candidate_producer.v1.json")
            )
        ]
        return len(oil_rows)
    return int(flow_row.get("candidate_count") or 0)



def _without_generated(value: Any) -> Any:
    if isinstance(value, Mapping):
        return {key: _without_generated(item) for key, item in value.items() if key not in {"computed_at_utc", "content_hash"}}
    if isinstance(value, list):
        return [_without_generated(item) for item in value]
    return value


def _now() -> str:
    return datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")
