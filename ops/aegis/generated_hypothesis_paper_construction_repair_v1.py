from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Mapping

from ops.aegis.intelligence_common_v1 import latest_json_v1, read_json_v1, write_json_v1
from ops.aegis.research_mapping_rules_v1 import file_hash_v1, report_path_v1, stable_hash_v1

FAMILY = "aegis_generated_hypothesis_paper_construction_repair_v1"
FILENAME = "generated_hypothesis_paper_construction_repair.v1.json"
SCHEMA_ID = "aegis_generated_hypothesis_paper_construction_repair"
SCHEMA_VERSION = "v1"
OIL_HYPOTHESIS_ID = "ehp_cdbd8fe683acb622"
OIL_SIGNAL_PREFIX = "c2_oil_shock_reversal_"

SAFETY = {
    "research_only": True,
    "paper_only": True,
    "no_broker_execution": True,
    "no_trade_advice": True,
    "no_live_trading": True,
    "no_real_capital": True,
    "no_autonomous_execution": True,
    "no_order_management": True,
    "no_operator_override": True,
    "no_outcome_generation": True,
    "no_validation_sample_generation": True,
    "no_quality_consumption": True,
    "broker_execution_allowed": False,
    "trade_advice_allowed": False,
    "live_trading_allowed": False,
    "real_capital_allowed": False,
    "autonomous_execution_allowed": False,
}


def generated_hypothesis_paper_construction_repair_path_v1(*, truth_root: Path | str, day_utc: str) -> Path:
    return report_path_v1(truth_root, FAMILY, day_utc, FILENAME)


def build_generated_hypothesis_paper_construction_repair_v1(*, truth_root: Path | str, day_utc: str, computed_at_utc: str | None = None) -> dict[str, Any]:
    root = Path(truth_root).expanduser().resolve()
    day = str(day_utc)
    paths = _paths(root, day)
    payloads = {key: read_json_v1(path) for key, path in paths.items()}
    row = _repair_row(payloads, paths, computed_at_utc or _now())
    body: dict[str, Any] = {
        "schema_id": SCHEMA_ID,
        "schema_version": SCHEMA_VERSION,
        "artifact_id": FAMILY,
        "day_utc": day,
        "target_day": day,
        "computed_at_utc": row["computed_at_utc"],
        "oil_shock": row,
        "summary": {key: row[key] for key in (
            "generated_hypothesis_id",
            "candidate_contract_id",
            "stop_price_present",
            "stop_price_source",
            "stop_price_status",
            "paper_construction_status",
            "paper_observation_found",
            "paper_observation_id",
            "paper_position_ledger_written",
            "furthest_stage_reached",
            "remaining_blocker",
            "blocker_reason",
            "owner",
            "david_action_required",
        )},
        "source_artifact_paths": {key: str(path) for key, path in sorted(paths.items())},
        "source_artifact_hashes": {key: file_hash_v1(path) for key, path in sorted(paths.items())},
        "safety_statement": "Package 017 proof verifies deterministic stop-price repair through the normal candidate, paper-construction, lifecycle, and ledger artifacts only. It does not create outcomes, validation samples, trades, broker actions, allocation changes, or safety-gate changes.",
        **SAFETY,
        "safety": dict(SAFETY),
    }
    body["content_hash"] = stable_hash_v1({k: v for k, v in body.items() if k not in {"computed_at_utc", "content_hash"}})
    return body


def write_generated_hypothesis_paper_construction_repair_v1(*, truth_root: Path | str, day_utc: str, payload: dict[str, Any] | None = None) -> Path:
    body = payload or build_generated_hypothesis_paper_construction_repair_v1(truth_root=truth_root, day_utc=day_utc)
    return write_json_v1(generated_hypothesis_paper_construction_repair_path_v1(truth_root=truth_root, day_utc=day_utc), body)


def _paths(root: Path, day: str) -> dict[str, Path]:
    specs = {
        "candidate_contracts": ("aegis_candidate_contracts_v1", "candidate_contracts.v1.json"),
        "paper_trade_construction": ("paper_trade_construction_v1", "paper_trade_construction.v1.json"),
        "candidate_lifecycle": ("aegis_candidate_to_paper_lifecycle_v1", "candidate_to_paper_lifecycle.v1.json"),
        "paper_position_ledger": ("aegis_paper_position_ledger_v1", "paper_position_ledger.v1.json"),
        "candidate_to_paper_proof": ("aegis_generated_hypothesis_candidate_to_paper_v1", "generated_hypothesis_candidate_to_paper.v1.json"),
    }
    out: dict[str, Path] = {}
    for key, (family, filename) in specs.items():
        path, _payload = latest_json_v1(root, family, day, filename)
        out[key] = path or report_path_v1(root, family, day, filename)
    return out


def _repair_row(payloads: Mapping[str, Any], paths: Mapping[str, Path], computed_at: str) -> dict[str, Any]:
    contract = _oil_contract(payloads.get("candidate_contracts"))
    rejection = _oil_rejection(payloads.get("candidate_contracts"))
    construction = _construction(payloads.get("paper_trade_construction"), contract, rejection)
    lifecycle = _lifecycle(payloads.get("candidate_lifecycle"), contract, rejection, construction)
    position = _position(payloads.get("paper_position_ledger"), contract, lifecycle)
    proof = _dict(payloads.get("candidate_to_paper_proof")).get("oil_shock")
    proof = proof if isinstance(proof, Mapping) else {}

    generated_hypothesis_id = _text(contract.get("hypothesis_id") or rejection.get("hypothesis_id") or lifecycle.get("hypothesis_id") or proof.get("hypothesis_id") or OIL_HYPOTHESIS_ID)
    candidate_id = _text(contract.get("candidate_id") or rejection.get("candidate_id") or construction.get("candidate_id") or lifecycle.get("candidate_id") or proof.get("candidate_id"))
    stop_price = _text(contract.get("stop_price") or construction.get("stop_price"))
    stop_source = _text(contract.get("stop_price_source") or construction.get("stop_policy_source"))
    stop_present = bool(stop_price)
    construction_status = _construction_status(construction)
    paper_position_id = _text(position.get("position_id") or lifecycle.get("paper_position_id"))
    ledger_written = bool(position)
    observation_found = bool(paper_position_id and ledger_written)

    if observation_found:
        furthest = "PAPER_OBSERVATION"
        blocker = "NONE"
        reason = "NONE"
        owner = "NONE"
    elif construction_status == "SUCCESS":
        furthest = "PAPER_CONSTRUCTION"
        blocker = _first_blocker(lifecycle, "PAPER_POSITION_LEDGER_WRITE_FAILED")
        reason = ";".join(_list(lifecycle.get("blocker_reason_codes"))) or blocker
        owner = "AEGIS_SYSTEM"
    elif stop_present:
        furthest = "DETERMINISTIC_STOP_PRICE"
        blocker = "PAPER_CONSTRUCTION_FAILED"
        reason = _text(proof.get("current_stop_reason")) or ";".join(_list(construction.get("missing_fields"))) or "Paper construction did not produce a constructed trade."
        owner = "AEGIS_SYSTEM"
    else:
        furthest = "CANDIDATE_CONTRACT" if contract or rejection else "CANDIDATE_FLOW"
        blocker = _text(rejection.get("rejection_reason") or "STOP_PRICE_MISSING")
        reason = _text(rejection.get("stop_price_missing_reason") or proof.get("current_stop_reason") or "Deterministic stop_price is missing before paper construction.")
        owner = "AEGIS_SYSTEM"

    return {
        "generated_hypothesis_id": generated_hypothesis_id,
        "candidate_contract_id": candidate_id,
        "raw_signal_id": _text(contract.get("raw_signal_id") or rejection.get("raw_signal_id") or proof.get("raw_signal_id")),
        "stop_price_present": stop_present,
        "stop_price": stop_price,
        "stop_loss_bps": _text(contract.get("stop_loss_bps") or construction.get("stop_loss_bps")),
        "stop_price_source": stop_source,
        "stop_price_status": _text(contract.get("stop_price_status") or ("DERIVED" if stop_present else "MISSING")),
        "paper_construction_status": construction_status,
        "paper_trade_id": _text(construction.get("paper_trade_id")),
        "paper_observation_found": observation_found,
        "paper_observation_id": paper_position_id,
        "paper_position_ledger_written": ledger_written,
        "furthest_stage_reached": furthest,
        "remaining_blocker": blocker,
        "blocker_reason": reason,
        "owner": owner,
        "david_action_required": False,
        "missing_fields": _list(construction.get("missing_fields")) or _list(rejection.get("missing_contract_fields")),
        "required_inputs": [
            "candidate_contract.stop_price",
            "candidate_contract.stop_loss_bps",
            "candidate_contract.entry_reference_price",
            "paper_trade_construction_v1.constructed_paper_trades",
            "aegis_candidate_to_paper_lifecycle_v1.rows",
            "aegis_paper_position_ledger_v1.open_positions",
        ],
        "source_artifact_paths": {key: str(path) for key, path in sorted(paths.items())},
        "source_artifact_hashes": {key: file_hash_v1(path) for key, path in sorted(paths.items())},
        "computed_at_utc": computed_at,
        **SAFETY,
    }


def _construction_status(row: Mapping[str, Any]) -> str:
    status = _upper(row.get("construction_status") or row.get("paper_construction_status"))
    missing = [x for x in _list(row.get("missing_fields")) if _text(x)]
    if status == "CONSTRUCTED" and not missing:
        return "SUCCESS"
    if status:
        return "FAILED"
    return "MISSING"


def _oil_contract(payload: Any) -> dict[str, Any]:
    for row in _list(_dict(payload).get("candidate_contracts")):
        if _is_oil(row):
            return dict(row)
    return {}


def _oil_rejection(payload: Any) -> dict[str, Any]:
    for row in _list(_dict(payload).get("rejected_raw_signals")):
        if _is_oil(row):
            return dict(row)
    return {}


def _construction(payload: Any, contract: Mapping[str, Any], rejection: Mapping[str, Any]) -> dict[str, Any]:
    cid = _text(contract.get("candidate_id") or rejection.get("candidate_id"))
    raw = _text(contract.get("raw_signal_id") or rejection.get("raw_signal_id"))
    for row in _list(_dict(payload).get("constructed_paper_trades")) + _list(_dict(payload).get("skipped_candidates")):
        if _matches(row, cid, raw):
            return dict(row)
    return {}


def _lifecycle(payload: Any, contract: Mapping[str, Any], rejection: Mapping[str, Any], construction: Mapping[str, Any]) -> dict[str, Any]:
    cid = _text(contract.get("candidate_id") or rejection.get("candidate_id") or construction.get("candidate_id"))
    raw = _text(contract.get("raw_signal_id") or rejection.get("raw_signal_id") or construction.get("raw_signal_id"))
    for row in _list(_dict(payload).get("rows")):
        if _matches(row, cid, raw):
            return dict(row)
    return {}


def _position(payload: Any, contract: Mapping[str, Any], lifecycle: Mapping[str, Any]) -> dict[str, Any]:
    cid = _text(contract.get("candidate_id") or lifecycle.get("candidate_id"))
    pid = _text(lifecycle.get("paper_position_id"))
    payload = _dict(payload)
    for key in ("positions", "open_positions", "closed_positions", "historical_positions"):
        for row in _list(payload.get(key)):
            if not isinstance(row, Mapping):
                continue
            lineage = row.get("candidate_lineage") if isinstance(row.get("candidate_lineage"), Mapping) else {}
            if (cid and _text(row.get("candidate_id") or lineage.get("candidate_id")) == cid) or (pid and _text(row.get("position_id")) == pid):
                return dict(row)
    return {}


def _is_oil(row: Any) -> bool:
    if not isinstance(row, Mapping):
        return False
    return _text(row.get("hypothesis_id")) == OIL_HYPOTHESIS_ID or _text(row.get("raw_signal_id")).startswith(OIL_SIGNAL_PREFIX) or _text(row.get("sleeve_id")) == "C2_OIL_SHOCK_REVERSAL_V1"


def _matches(row: Any, candidate_id: str, raw_signal_id: str) -> bool:
    if not isinstance(row, Mapping):
        return False
    return bool((candidate_id and _text(row.get("candidate_id")) == candidate_id) or (raw_signal_id and _text(row.get("raw_signal_id")) == raw_signal_id) or _is_oil(row))


def _first_blocker(row: Mapping[str, Any], default: str) -> str:
    codes = [_text(x) for x in _list(row.get("blocker_reason_codes")) if _text(x)]
    return codes[0] if codes else default


def _dict(value: Any) -> dict[str, Any]:
    return value if isinstance(value, dict) else {}


def _list(value: Any) -> list[Any]:
    return value if isinstance(value, list) else []


def _text(value: Any) -> str:
    return str(value or "").strip()


def _upper(value: Any) -> str:
    return _text(value).upper()


def _now() -> str:
    return datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")
