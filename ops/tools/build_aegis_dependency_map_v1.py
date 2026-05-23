#!/usr/bin/env python3
from __future__ import annotations

import argparse
import hashlib
import json
import re
import sys
from collections import defaultdict, deque
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Mapping

REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from constellation_2.phaseD.lib.canon_json_v1 import canonical_json_bytes_v1

SCHEMA_ID = "aegis_dependency_map"
SCHEMA_VERSION = "v1"
REPORT_FAMILY = "aegis_dependency_map_v1"
DOW_INTENT_HASH = "4f60b5008c648dbf1df2524cd06f78be5cbf7e80e5ce1ef7ccae40bc9cc50960"
DOW_TICKET_ID = "ticket:bdbf62f800f2901611e4504f"
SLEEVES = [
    "C2_MEAN_REVERSION_EQ_V1",
    "C2_TREND_EQ_PRIMARY_V1",
    "C2_EVENT_DISLOCATION_V1",
    "C2_VOL_INCOME_DEFINED_RISK_V1",
    "C2_CROSS_ASSET_TREND_V1",
    "C2_MARKET_NEUTRAL_SPREAD_V1",
    "C2_DEFENSIVE_TAIL_V1",
    "C2_INTENT_SIMULATOR_V1",
]
VALID_STATUSES = {"VALID", "VALIDATED", "PASS", "OK", "READY", "COMPLETE", "COMPLETED", "SELECTED", "ADMIT", "APPROVED", "AUTHORIZED", "PRESENT"}
INVALID_STATUSES = {"INVALID", "REJECTED", "BLOCKED", "BLOCK", "FAIL", "FAILED", "MISSING", "FAIL_MISSING_INPUTS", "FAIL_CORRUPT_INPUTS", "FAIL_SCHEMA_VIOLATION"}


def stable_hash_v1(payload: Any) -> str:
    return hashlib.sha256(json.dumps(payload, sort_keys=True, separators=(",", ":"), ensure_ascii=True, default=str).encode()).hexdigest()


def sha256_file_v1(path: Path | None) -> str:
    if path is None or not path.exists() or not path.is_file():
        return ""
    h = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def read_json_v1(path: Path | None) -> dict[str, Any]:
    if path is None or not path.exists() or not path.is_file():
        return {}
    try:
        obj = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}
    return obj if isinstance(obj, dict) else {}


def read_jsonl_events_v1(path: Path) -> list[dict[str, Any]]:
    if not path.exists():
        return []
    out: list[dict[str, Any]] = []
    for line in path.read_text(encoding="utf-8").splitlines():
        if not line.strip():
            continue
        try:
            obj = json.loads(line)
        except Exception:
            continue
        if isinstance(obj, dict):
            out.append(obj)
    return out


def latest_file_v1(root: Path, pattern: str) -> Path | None:
    matches = [p for p in root.glob(pattern) if p.is_file()]
    if not matches:
        return None
    return sorted(matches, key=lambda p: (p.stat().st_mtime_ns, str(p)))[-1].resolve()


def norm_status_v1(payload: Mapping[str, Any], *, exists: bool, preferred: tuple[str, ...] = ()) -> str:
    if not exists:
        return "MISSING"
    values: list[str] = []
    for key in preferred + ("validation_status", "status", "admission_status", "closure_status", "lineage_status", "trade_construction_status"):
        value = str(payload.get(key) or "").strip().upper()
        if value:
            values.append(value)
    for value in values:
        if value in VALID_STATUSES or value == "COMPLETE":
            return "VALID"
        if value in {"REJECTED", "BLOCKED", "BLOCK"}:
            return "REJECTED"
        if value in {"FAIL", "FAILED", "INVALID"}:
            return "INVALID"
        if value in {"MANUAL_REQUIRED", "POLICY_DISABLED", "NOT_APPLICABLE", "WARNING_ONLY", "STALE"}:
            return value
        if value == "MISSING_CONVERSION":
            return "INVALID"
    return "VALID"


def extract_runtime_hash_v1(payload: Mapping[str, Any]) -> str:
    for key in ("runtime_evaluation_hash", "deterministic_output_hash", "RuntimeEvaluation hash", "runtime_hash"):
        value = str(payload.get(key) or "").strip()
        if value:
            return value
    nested = payload.get("runtime_evaluation") if isinstance(payload.get("runtime_evaluation"), Mapping) else {}
    return str(nested.get("deterministic_output_hash") or nested.get("runtime_evaluation_hash") or "")


def blocker_from_payload_v1(payload: Mapping[str, Any]) -> str:
    for key in ("blocker_code", "blocker", "exact_blocker_reason", "first_real_blocker_dependency_id"):
        value = payload.get(key)
        if isinstance(value, str) and value.strip():
            return value.strip()
    first = payload.get("first_real_blocker") if isinstance(payload.get("first_real_blocker"), Mapping) else {}
    if first:
        dep = str(first.get("first_real_blocker_dependency_id") or first.get("dependency_id") or "").strip()
        detail = str(first.get("detail") or "").strip()
        return f"{dep}: {detail}" if dep and detail else dep or detail
    codes = payload.get("blocker_codes") or payload.get("blocking_reason_codes") or payload.get("reason_codes")
    if isinstance(codes, list) and codes:
        return ",".join(str(x) for x in codes if str(x))
    status = str(payload.get("lineage_status") or "").strip()
    return status if status and status != "ACTIVE_CURRENT" else ""


def _runtime_hash_for_map_v1(truth_root: Path, day: str) -> str:
    return str(read_json_v1(truth_root / "reports/aegis_runtime_truth_kernel_v1" / day / "runtime_evaluation.v1.json").get("deterministic_output_hash") or "")


def _classify_target_day_admission_blockers_v1(node: dict[str, Any]) -> None:
    payload_fields = node.get("payload_status_fields") if isinstance(node.get("payload_status_fields"), dict) else {}
    blocker = str(node.get("exact_blocker_reason") or "")
    mapped: list[str] = []
    if "HIDDEN_DEPENDENCY_DETECTED" in blocker:
        mapped.append("TARGET_DAY_ADMISSION_HIDDEN_DEPENDENCY")
    if "PARTIAL_BUILD" in blocker:
        mapped.append("TARGET_DAY_ADMISSION_PARTIAL_BUILD")
    if "REQUIRED_GATE_FAIL" in blocker:
        mapped.append("TARGET_DAY_ADMISSION_REQUIRED_GATE_FAIL")
    if mapped:
        node["status"] = "REJECTED"
        node["exact_blocker_reason"] = ",".join(mapped) + ":" + blocker
        node["target_day_admission_blocker_states"] = mapped
    if payload_fields.get("admission_status") == "ADMIT" and payload_fields.get("validation_status") not in {"VALID", "PASS", "OK"}:
        node["status"] = "REJECTED"
        node["exact_blocker_reason"] = "TARGET_DAY_ADMISSION_PARTIAL_BUILD:admission_status ADMIT without valid closure"


def _classify_positions_static_paper_v1(node: dict[str, Any]) -> None:
    paths = [Path(p) for p in node.get("output_paths") or [] if p]
    payload = read_json_v1(paths[0]) if paths else {}
    source_type = str(payload.get("source_type") or "").strip().upper()
    items = payload.get("items") if isinstance(payload.get("items"), list) else []
    reconciliation = payload.get("reconciliation") if isinstance(payload.get("reconciliation"), Mapping) else {}
    reason_codes = {str(code).strip().upper() for code in payload.get("reason_codes") or [] if str(code).strip()}
    if (
        source_type == "SIMULATION_LEDGER"
        and not items
        and bool(reconciliation.get("broker_statement_present") is True) is False
        and "BROKER_STATEMENT_MISSING_INTERNAL_STATE_ONLY" in reason_codes
    ):
        node["status"] = "VALID"
        node["repairability"] = "AUTO_DETERMINISTIC"
        node["exact_blocker_reason"] = "EMPTY_POSITIONS_STATIC_PAPER: broker statement not required for explicit static-paper empty positions"
        node["positions_source_classification"] = "EMPTY_POSITIONS_STATIC_PAPER"
        node["next_safe_action"] = "Proceed as empty static-paper positions only while policy/source_type remains static-paper; require broker evidence for broker-backed contexts."
    elif "BROKER_STATEMENT_MISSING_INTERNAL_STATE_ONLY" in reason_codes:
        node["status"] = "INVALID"
        node["exact_blocker_reason"] = "BROKER_REQUIRED_BUT_UNAVAILABLE: " + ",".join(sorted(reason_codes))


def _classify_runtime_hash_v1(node: dict[str, Any], *, truth_root: Path, day: str) -> None:
    current = _runtime_hash_for_map_v1(truth_root, day)
    artifact_hash = str(node.get("RuntimeEvaluation_hash") or "")
    if current and artifact_hash and artifact_hash != current and node["node_id"] in {"candidate_identity_set", "capital_authority_allocation", "risk_definition_contract", "paper_trade_construction", "engine_activity_authorization"}:
        node["status"] = "STALE" if node["status"] == "VALID" else node["status"]
        node["exact_blocker_reason"] = "RUNTIME_HASH_MISMATCH"
        node["next_safe_action"] = node.get("next_safe_action") or "Rebuild with current RuntimeEvaluation hash."

@dataclass
class NodeSpec:
    node_id: str
    artifact_name: str
    producer_tool: str
    schema: str
    producer_contract_id: str
    outputs: list[str] = field(default_factory=list)
    sources: list[str] = field(default_factory=list)
    upstream: list[str] = field(default_factory=list)
    repairability: str = "AUTO_DETERMINISTIC"
    next_safe_action: str = "Rebuild with canonical producer if inputs are current."
    blocks: dict[str, bool] = field(default_factory=dict)
    status_hint: str = ""
    blocker_hint: str = ""


BLOCK_KEYS = ["sleeve_readiness", "candidate_generation", "conversion", "submit_boundary", "manual_capture_save", "trade_advice", "broker_submit"]

DOWNSTREAM_BUILDER_READS_V1 = {
    "conversion_package_builder": [
        "candidate_selection",
        "symbol_universe",
        "market_data_readiness",
        "paper_trade_construction",
        "capital_authority_allocation",
        "risk_definition_contract",
        "candidate_identity_set",
        "execution_package_build",
    ],
    "execution_package_builder": [
        "candidate_identity_set",
        "global_context",
        "economic_state_package",
        "capital_authority_allocation",
        "engine_activity_authorization",
        "global_kill_switch_state",
    ],
    "broker_submit_transmit_boundary": [
        "manual_capture_evidence_append",
        "execution_package_build",
        "ib_api_handshake",
        "trade_submit_readiness",
    ],
    "exposure_intent_paper_submission_package": [
        "candidate_selection",
        "capital_authority_allocation",
        "risk_definition_contract",
        "candidate_identity_set",
        "execution_package_build",
    ],
    "submit_boundary_precheck": [
        "paper_trade_construction",
        "conversion_package",
        "paper_intent_evidence",
        "market_freshness_evidence",
        "trade_ticket_lineage",
    ],
    "trade_ticket_lineage": [
        "paper_trade_construction",
        "conversion_package",
        "paper_intent_evidence",
        "market_freshness_evidence",
        "submit_boundary_precheck",
    ],
    "manual_capture_save_path": [
        "ui_projection",
        "paper_trade_construction",
        "trade_ticket_lineage",
        "submit_boundary_precheck",
    ],
}


def block_map(**kwargs: bool) -> dict[str, bool]:
    return {key: bool(kwargs.get(key, False)) for key in BLOCK_KEYS}


def static_downstream_read_nodes_v1() -> list[dict[str, str]]:
    rows: list[dict[str, str]] = []
    for builder, node_ids in DOWNSTREAM_BUILDER_READS_V1.items():
        for node_id in node_ids:
            rows.append({"builder": builder, "node_id": node_id})
    return rows


def map_completeness_v1(nodes: list[dict[str, Any]]) -> dict[str, Any]:
    node_ids = {str(n.get("node_id")) for n in nodes if isinstance(n, Mapping)}
    hidden = [
        {"builder": row["builder"], "node_id": row["node_id"], "reason_code": "HIDDEN_DEPENDENCY_DETECTED"}
        for row in static_downstream_read_nodes_v1()
        if row["node_id"] not in node_ids
    ]
    return {
        "status": "PASS" if not hidden else "FAIL",
        "reason_codes": [] if not hidden else ["MAP_INCOMPLETE", "HIDDEN_DEPENDENCY_DETECTED"],
        "static_read_inventory": static_downstream_read_nodes_v1(),
        "hidden_dependencies": hidden,
    }


def report_dir_v1(truth_root: Path, day: str) -> Path:
    return truth_root / "reports" / REPORT_FAMILY / day


def output_paths_v1(truth_root: Path, day: str) -> tuple[Path, Path, Path, Path]:
    d = report_dir_v1(truth_root, day)
    return d / "aegis_dependency_map.v1.json", d / "aegis_dependency_map.v1.txt", d / "aegis_dependency_map.v1.dot", d / "root_blockers.v1.json"


def producer_contracts_v1() -> dict[str, dict[str, Any]]:
    path = REPO_ROOT / "governance/02_REGISTRIES/AEGIS_PRODUCER_CONTRACTS_V1.json"
    obj = read_json_v1(path)
    rows = obj.get("contracts") if isinstance(obj.get("contracts"), list) else []
    out: dict[str, dict[str, Any]] = {}
    for row in rows:
        if isinstance(row, Mapping):
            pid = str(row.get("producer_id") or "")
            if pid:
                out[pid] = dict(row)
    return out


def event_index_v1(truth_root: Path, day: str) -> dict[str, list[str]]:
    paths = [
        truth_root / "events" / "aegis_evidence_events_v1" / day / "events.jsonl",
        truth_root / "events" / "aegis_evidence_ledger_v1" / day / "events.jsonl",
    ]
    index: dict[str, list[str]] = defaultdict(list)
    for event_path in paths:
        for event in read_jsonl_events_v1(event_path):
            eid = str(event.get("event_id") or "")
            if not eid:
                continue
            for artifact_path in event.get("artifact_paths") or []:
                index[str(artifact_path)].append(eid)
            for artifact_path in (event.get("output_hashes") or {}).keys() if isinstance(event.get("output_hashes"), Mapping) else []:
                index[str(artifact_path)].append(eid)
    return {k: sorted(set(v)) for k, v in index.items()}


def node_to_dict_v1(spec: NodeSpec, *, truth_root: Path, day: str, event_index: Mapping[str, list[str]], contract_by_producer: Mapping[str, dict[str, Any]]) -> dict[str, Any]:
    output_paths = [str(Path(p).expanduser().resolve()) for p in spec.outputs if p]
    source_paths = [str(Path(p).expanduser().resolve()) for p in spec.sources if p]
    existing_outputs = [Path(p) for p in output_paths if Path(p).exists()]
    primary = existing_outputs[0] if existing_outputs else (Path(output_paths[0]) if output_paths else None)
    payload = read_json_v1(primary)
    exists = bool(primary and primary.exists())
    status = spec.status_hint or norm_status_v1(payload, exists=exists)
    payload_blocker = blocker_from_payload_v1(payload)
    blocker = payload_blocker or (spec.blocker_hint if status != "VALID" else "")
    blocker = blocker.replace("{day}", day)
    if status == "VALID" and payload_blocker and any(code in payload_blocker for code in ("MISSING", "BLOCK", "REJECT", "FAILED", "INVALID")):
        status = "INVALID"
    event_ids: list[str] = []
    for p in output_paths + source_paths:
        event_ids.extend(event_index.get(p, []))
    contract = contract_by_producer.get(spec.producer_contract_id) or contract_by_producer.get(spec.producer_tool) or {}
    return {
        "node_id": spec.node_id,
        "artifact_name": spec.artifact_name,
        "producer_tool": spec.producer_tool,
        "schema": spec.schema,
        "producer_contract_id": spec.producer_contract_id,
        "producer_contract_present": bool(contract),
        "source_paths": source_paths,
        "output_paths": output_paths,
        "event_ids": sorted(set(event_ids)),
        "hash": sha256_file_v1(primary) if exists else "",
        "RuntimeEvaluation_hash": extract_runtime_hash_v1(payload),
        "day_utc": str(payload.get("day_utc") or payload.get("target_day") or day),
        "status": status,
        "upstream_dependencies": list(spec.upstream),
        "downstream_consumers": [],
        "blocks": {**block_map(), **spec.blocks},
        "repairability": spec.repairability,
        "exact_blocker_reason": blocker,
        "next_safe_action": spec.next_safe_action,
        "payload_status_fields": {key: payload.get(key) for key in ("status", "validation_status", "admission_status", "closure_status", "lineage_status", "trade_construction_status") if key in payload},
    }


def parse_conversion_details_v1(conversion: Mapping[str, Any]) -> dict[str, Any]:
    raw = str(conversion.get("blocker_message") or "")
    try:
        details = json.loads(raw) if raw else {}
    except Exception:
        details = {}
    return details if isinstance(details, dict) else {}


def detect_cycles_v1(edges: list[dict[str, str]]) -> list[list[str]]:
    graph: dict[str, list[str]] = defaultdict(list)
    for edge in edges:
        graph[edge["from"]].append(edge["to"])
    cycles: list[list[str]] = []
    stack: list[str] = []
    state: dict[str, str] = {}

    def visit(node: str) -> None:
        state[node] = "visiting"
        stack.append(node)
        for nxt in graph.get(node, []):
            if state.get(nxt) == "visiting":
                i = stack.index(nxt)
                cycles.append(stack[i:] + [nxt])
            elif state.get(nxt) != "done":
                visit(nxt)
        stack.pop()
        state[node] = "done"

    for node in sorted(set(graph) | {e["to"] for e in edges}):
        if state.get(node) is None:
            visit(node)
    return cycles


def classify_root_blockers_v1(nodes: list[dict[str, Any]], edges: list[dict[str, str]]) -> list[dict[str, Any]]:
    by_id = {n["node_id"]: n for n in nodes}
    invalid = {n["node_id"] for n in nodes if n["status"] in {"MISSING", "STALE", "INVALID", "REJECTED", "MANUAL_REQUIRED"}}
    root_ids = []
    for node_id in invalid:
        upstream_invalid = [u for u in by_id[node_id].get("upstream_dependencies", []) if u in invalid]
        if not upstream_invalid:
            root_ids.append(node_id)
    return [
        {
            "node_id": node_id,
            "artifact_name": by_id[node_id]["artifact_name"],
            "status": by_id[node_id]["status"],
            "exact_blocker_reason": by_id[node_id].get("exact_blocker_reason", ""),
            "repairability": by_id[node_id].get("repairability", ""),
            "next_safe_action": by_id[node_id].get("next_safe_action", ""),
            "blocks": by_id[node_id].get("blocks", {}),
        }
        for node_id in sorted(root_ids)
    ]


def add_downstreams_v1(nodes: list[dict[str, Any]]) -> None:
    by_id = {n["node_id"]: n for n in nodes}
    for node in nodes:
        for upstream in node.get("upstream_dependencies", []):
            if upstream in by_id:
                by_id[upstream].setdefault("downstream_consumers", []).append(node["node_id"])
    for node in nodes:
        node["downstream_consumers"] = sorted(set(node.get("downstream_consumers") or []))


def build_specs_v1(*, truth_root: Path, day: str, sleeve_id: str, ticket_id: str) -> tuple[list[NodeSpec], dict[str, Any]]:
    sleeve_root = truth_root.parent / "truth_sleeves" / "PRIMARY" / "PAPER"
    ticket_dir = ticket_id.replace(":", "_")
    selected_pointer = truth_root / "pointers/selected_intent_pointer.v1.json"
    construction = truth_root / "reports/paper_trade_construction_v1" / day / "paper_trade_construction.v1.json"
    allocation = truth_root / "allocation_v1/capital_authority_allocation_v1" / day / "capital_authority_allocation.v1.json"
    risk = truth_root / "risk_definition_contract_v1" / day / DOW_INTENT_HASH / "risk_definition_contract.v1.json"
    identity = truth_root / "reports/candidate_identity_set_v1" / day / DOW_INTENT_HASH / "candidate_identity_set.v1.json"
    target = truth_root / "target_day_admission_v1" / f"{day}.json"
    day_activation = latest_file_v1(sleeve_root, f"day_activation_package_v1/{day}/*/day_activation_package.v1.json") or sleeve_root / "day_activation_package_v1" / day / "UNKNOWN" / "day_activation_package.v1.json"
    global_context = latest_file_v1(sleeve_root, f"global_context_package_v1/{day}/*/global_context_package.v1.json") or sleeve_root / "global_context_package_v1" / day / "UNKNOWN" / "global_context_package.v1.json"
    economic_build = latest_file_v1(truth_root, f"reports/economic_state_build_v1/{day}/*/economic_state_build.v1.json") or truth_root / "reports/economic_state_build_v1" / day / "UNKNOWN" / "economic_state_build.v1.json"
    economic_payload = read_json_v1(economic_build)
    first = economic_payload.get("first_real_blocker") if isinstance(economic_payload.get("first_real_blocker"), Mapping) else {}
    economic_pkg_text = str(first.get("path") or "")
    economic_pkg = Path(economic_pkg_text) if economic_pkg_text else (latest_file_v1(sleeve_root, f"economic_state_package_v1/{day}/*/economic_state_package.v1.json") or sleeve_root / "economic_state_package_v1" / day / "UNKNOWN" / "economic_state_package.v1.json")
    conversion = latest_file_v1(truth_root, f"reports/exposure_intent_paper_submission_package_v1/{day}/*/exposure_intent_paper_submission_package.v1.json") or truth_root / "reports/exposure_intent_paper_submission_package_v1" / day / "UNKNOWN" / "exposure_intent_paper_submission_package.v1.json"
    conversion_payload = read_json_v1(conversion)
    conversion_details = parse_conversion_details_v1(conversion_payload)
    execution_build_path = Path(str(conversion_details.get("build_path") or "")) if conversion_details.get("build_path") else latest_file_v1(truth_root, f"reports/execution_build_v1/{day}/*/execution_build.v1.json")
    execution_build = execution_build_path or truth_root / "reports/execution_build_v1" / day / "UNKNOWN" / "execution_build.v1.json"
    execution_package = latest_file_v1(sleeve_root, f"execution_package_v1/{day}/*/execution_package.v1.json") or sleeve_root / "execution_package_v1" / day / "UNKNOWN" / "execution_package.v1.json"
    engine_auth = sleeve_root / "engine_activity_v1" / "authorization_v1" / day / f"{DOW_INTENT_HASH}.authorization.v1.json"
    global_kill_switch = truth_root / "risk_v1" / "kill_switch_v1" / day / "global_kill_switch_state.v1.json"
    ib_handshake = sleeve_root / "ib_api_handshake" / day / "ib_api_handshake.v1.json"
    trade_readiness = sleeve_root / "trade_submit_readiness_c2_v1" / "PAPER" / "DUO847203" / "status.json"
    phasec = str(conversion_payload.get("actual_phasec_order_plan_path") or "")
    context = {"conversion_details": conversion_details, "economic_first_real_blocker": first, "phasec_order_plan_path": phasec}
    specs = [
        NodeSpec("symbol_universe", "engine_universe_candidate_basis_v1", "ops/tools/run_ranked_engine_candidate_basis_day_v1.py", "governance/04_DATA/SCHEMAS/C2/RISK/engine_universe_candidate_basis.v1.schema.json", "ops/tools/run_ranked_engine_candidate_basis_day_v1.py", [str(truth_root / "reports/engine_universe_candidate_basis_v1" / day / sleeve_id / "engine_universe_candidate_basis.v1.json")], repairability="AUTO_DETERMINISTIC", blocks=block_map(sleeve_readiness=True, candidate_generation=True, conversion=True), next_safe_action="Rebuild canonical engine universe candidate basis for the sleeve."),
        NodeSpec("market_data_readiness", "market_data_inputs_v1", "ops/tools/build_aegis_market_data_inputs_v1.py", "governance/04_DATA/SCHEMAS/C2/REPORTS/market_data_inputs.v1.schema.json", "ops/tools/build_aegis_market_data_inputs_v1.py", [str(truth_root / "reports/market_data_inputs_v1" / day / "market_data_inputs.v1.json")], repairability="EXTERNAL_SOURCE_REQUIRED", blocks=block_map(sleeve_readiness=True, candidate_generation=True, conversion=True, submit_boundary=True), next_safe_action="Refresh only from real market data sources; do not synthesize prices."),
        NodeSpec("sleeve_readiness", "aegis_sleeve_readiness_v1", "ops/tools/build_aegis_sleeve_readiness_v1.py", "governance/04_DATA/SCHEMAS/C2/REPORTS/sleeve_readiness.v1.schema.json", "ops/tools/build_aegis_sleeve_readiness_v1.py", [str(truth_root / "reports/aegis_sleeve_readiness_v1" / day / "sleeve_readiness.v1.json")], [str(truth_root / "reports/market_data_inputs_v1" / day / "market_data_inputs.v1.json")], ["symbol_universe", "market_data_readiness"], blocks=block_map(candidate_generation=True, conversion=True), next_safe_action="Rerun sleeve readiness after upstream data inputs are current."),
        NodeSpec("sleeve_evaluation", "sleeve_evaluation_kernel_v1", "ops/tools/run_sleeve_evaluation_kernel_v1.py", "governance/04_DATA/SCHEMAS/C2/REPORTS/sleeve_evaluation.v1.schema.json", "ops/tools/run_sleeve_evaluation_kernel_v1.py", [str(truth_root / "reports/sleeve_evaluation_kernel_v1" / day / sleeve_id / "sleeve_evaluation.v1.json")], upstream=["sleeve_readiness"], blocks=block_map(candidate_generation=True, conversion=True), next_safe_action="Rerun sleeve evaluation kernel if readiness inputs change."),
        NodeSpec("candidate_generation", "candidate_generation_diagnostics_v1", "ops/tools/write_aegis_candidate_generation_diagnostics_v1.py", "governance/04_DATA/SCHEMAS/C2/REPORTS/candidate_generation_diagnostics.v1.schema.json", "ops/tools/write_aegis_candidate_generation_diagnostics_v1.py", [str(truth_root / "reports/aegis_candidate_generation_diagnostics_v1" / day / "candidate_generation_diagnostics.v1.json")], upstream=["sleeve_evaluation"], blocks=block_map(conversion=True), next_safe_action="Regenerate diagnostics from current sleeve evaluation output."),
        NodeSpec("candidate_selection", "selected_intent_pointer_v1", "ops/tools/promote_aegis_selected_intent_v1.py", "governance/04_DATA/SCHEMAS/C2/REPORTS/selected_intent_pointer.v1.schema.json", "ops/tools/promote_aegis_selected_intent_v1.py", [str(selected_pointer)], upstream=["candidate_generation"], blocks=block_map(conversion=True, submit_boundary=True), next_safe_action="Promote a selected intent only through governed selection/pointer producer."),
        NodeSpec("paper_trade_construction", "paper_trade_construction_v1", "ops/tools/run_paper_trade_construction_v1.py", "governance/04_DATA/SCHEMAS/C2/REPORTS/paper_trade_construction.v1.schema.json", "ops/tools/run_paper_trade_construction_v1.py", [str(construction)], [str(selected_pointer)], ["candidate_selection", "market_data_readiness"], blocks=block_map(conversion=True, submit_boundary=True, manual_capture_save=True), next_safe_action="Rebuild construction from current selected intent and RuntimeEvaluation."),
        NodeSpec("capital_authority_allocation", "capital_authority_allocation_v1", "ops/tools/build_capital_authority_allocation_v1.py", "governance/04_DATA/SCHEMAS/C2/REPORTS/capital_authority_allocation.v1.schema.json", "ops/tools/build_capital_authority_allocation_v1.py", [str(allocation)], [str(construction)], ["paper_trade_construction"], blocks=block_map(conversion=True, submit_boundary=True, manual_capture_save=True), next_safe_action="Rebuild governed allocation; manual approval only if policy says MANUAL_REQUIRED."),
        NodeSpec("risk_definition_contract", "risk_definition_contract_v1", "ops/tools/build_risk_definition_contract_v1.py", "governance/04_DATA/SCHEMAS/C2/REPORTS/risk_definition_contract.v1.schema.json", "ops/tools/build_risk_definition_contract_v1.py", [str(risk)], [str(construction), str(allocation)], ["paper_trade_construction", "capital_authority_allocation"], blocks=block_map(conversion=True, submit_boundary=True, manual_capture_save=True), next_safe_action="Rebuild risk contract after allocation/construction are current."),
        NodeSpec("candidate_identity_set", "candidate_identity_set_v1", "ops/tools/build_candidate_identity_set_v1.py", "governance/04_DATA/SCHEMAS/C2/REPORTS/candidate_identity_set.v1.schema.json", "ops/tools/build_candidate_identity_set_v1.py", [str(identity)], [str(phasec) if phasec else "", str(risk), str(allocation)], ["candidate_selection", "capital_authority_allocation", "risk_definition_contract"], blocks=block_map(conversion=True, submit_boundary=True), next_safe_action="Rebuild candidate identity set; reject stale Phase C order plans."),
        NodeSpec("target_day_admission", "target_day_admission_v1", "ops/tools/build_target_day_admission_v1.py", "governance/04_DATA/SCHEMAS/C2/REPORTS/target_day_admission.v1.schema.json", "ops/tools/build_target_day_admission_v1.py", [str(target)], upstream=["candidate_selection"], blocks=block_map(conversion=True, submit_boundary=True, manual_capture_save=True), next_safe_action="Rebuild target-day admission from RuntimeEvaluation authority only."),
        NodeSpec("day_activation", "day_activation_package_v1", "ops/tools/build_day_activation_package_v1.py", "governance/04_DATA/SCHEMAS/C2/CONTEXT/day_activation_package.v1.schema.json", "ops/tools/build_day_activation_package_v1.py", [str(day_activation)], [str(target)], ["target_day_admission"], blocks=block_map(conversion=True, submit_boundary=True, manual_capture_save=True), next_safe_action="Rebuild day activation package after target day is admitted."),
        NodeSpec("global_context", "global_context_package_v1", "ops/tools/build_global_context_package_v1.py", "governance/04_DATA/SCHEMAS/C2/CONTEXT/global_context_package.v1.schema.json", "ops/tools/build_global_context_package_v1.py", [str(global_context)], [str(day_activation)], ["day_activation"], blocks=block_map(conversion=True, submit_boundary=True, manual_capture_save=True), next_safe_action="Rebuild global context package after day activation is valid."),
        NodeSpec("cash_ledger_snapshot", "cash_ledger_snapshot_v1", "constellation_2.phaseF.cash_ledger.run.run_cash_ledger_snapshot_day_v1", "governance/04_DATA/SCHEMAS/C2/CASH_LEDGER/cash_ledger_snapshot.v1.schema.json", "constellation_2.phaseF.cash_ledger.run.run_cash_ledger_snapshot_day_v1", [str(truth_root / "cash_ledger_v1/snapshots" / day / "cash_ledger_snapshot.v1.json")], upstream=["global_context"], repairability="MANUAL_REQUIRED", blocker_hint="Missing governed cash ledger snapshot at cash_ledger_v1/snapshots/{day}/cash_ledger_snapshot.v1.json.", blocks=block_map(conversion=True, submit_boundary=True, manual_capture_save=True), next_safe_action="Produce a real cash ledger snapshot from broker/operator statement evidence; no placeholder cash."),
        NodeSpec("positions_snapshot", "positions_snapshot_v5", "constellation_2.phaseF.positions.run.run_positions_snapshot_day_v5", "governance/04_DATA/SCHEMAS/C2/POSITIONS/positions_snapshot.v5.schema.json", "constellation_2.phaseF.positions.run.run_positions_snapshot_day_v5", [str(truth_root / "positions_v1/snapshots" / day / "positions_snapshot.v5.json")], upstream=["cash_ledger_snapshot"], repairability="EXTERNAL_SOURCE_REQUIRED", blocks=block_map(conversion=True, submit_boundary=True), next_safe_action="Refresh real broker positions snapshot after cash ledger exists."),
        NodeSpec("position_lifecycle_snapshot", "position_lifecycle_snapshot_v2", "ops/tools/run_position_lifecycle_snapshot_v2.py", "governance/04_DATA/SCHEMAS/C2/POSITION_LIFECYCLE/position_lifecycle_snapshot.v2.schema.json", "ops/tools/run_position_lifecycle_snapshot_v2.py", [str(truth_root / "position_lifecycle_v2" / day / "position_lifecycle_snapshot.v2.json")], upstream=["positions_snapshot"], blocks=block_map(conversion=True, submit_boundary=True), next_safe_action="Rebuild position lifecycle snapshot from current positions."),
        NodeSpec("accounting_nav", "accounting_nav_v2", "ops/tools/run_accounting_nav_v2_day_v1.py", "governance/04_DATA/SCHEMAS/C2/ACCOUNTING/nav.v2.schema.json", "ops/tools/run_accounting_nav_v2_day_v1.py", [str(truth_root / "accounting_v2/nav" / day / "nav.v2.json")], upstream=["cash_ledger_snapshot", "positions_snapshot"], blocks=block_map(conversion=True, submit_boundary=True), next_safe_action="Rebuild NAV from real cash and positions snapshots."),
        NodeSpec("economic_state_package", "economic_state_package_v1", "ops/tools/run_economic_state_authority_v1.py", "governance/04_DATA/SCHEMAS/C2/ECONOMIC/economic_state_package.v1.schema.json", "constellation_2.common.economic_state_authority_v1", [str(economic_pkg), str(economic_build)], [str(global_context), str(truth_root / "cash_ledger_v1/snapshots" / day / "cash_ledger_snapshot.v1.json")], ["global_context", "cash_ledger_snapshot", "positions_snapshot", "position_lifecycle_snapshot", "accounting_nav", "capital_authority_allocation"], blocks=block_map(conversion=True, submit_boundary=True, manual_capture_save=True), next_safe_action="Run economic state authority only after cash, positions, lifecycle, NAV, and allocation are current."),
        NodeSpec("engine_activity_authorization", "engine_activity_authorization_v1", "ops/tools/run_authorization_artifacts_day_v1.py", "governance/04_DATA/SCHEMAS/C2/ENGINE_ACTIVITY/authorization.v1.schema.json", "authorization_artifacts_day_v1", [str(engine_auth)], [str(economic_pkg), str(allocation), str(identity)], ["candidate_identity_set", "economic_state_package", "capital_authority_allocation"], repairability="AUTO_DETERMINISTIC", blocker_hint="Missing governed engine activity authorization at engine_activity_v1/authorization_v1/{day}/{intent_hash}.authorization.v1.json.".replace("{intent_hash}", DOW_INTENT_HASH), blocks=block_map(conversion=True, submit_boundary=True, manual_capture_save=True), next_safe_action="Run governed engine activity authorization only after candidate identity, economic state, and capital authority are current; this does not permit broker submit/transmit."),
        NodeSpec("global_kill_switch_state", "global_kill_switch_state_v1", "ops/tools/run_global_kill_switch_v1.py", "governance/04_DATA/SCHEMAS/C2/RISK/global_kill_switch_state.v1.schema.json", "global_kill_switch_v1", [str(global_kill_switch)], upstream=["global_context"], repairability="AUTO_DETERMINISTIC", blocker_hint="Missing governed global kill-switch state at risk_v1/kill_switch_v1/{day}/global_kill_switch_state.v1.json.", blocks=block_map(conversion=True, submit_boundary=True, manual_capture_save=True, broker_submit=True), next_safe_action="Materialize kill-switch state; entries remain blocked if active."),
        NodeSpec("ib_api_handshake", "ib_api_handshake_v1", "ops/tools/run_ib_api_handshake_spine_v1.py", "ib_api_handshake_v1", "trade_submit_readiness_c2_v1", [str(ib_handshake)], repairability="EXTERNAL_SOURCE_REQUIRED", blocker_hint="BROKER_EVENTS_MISSING: IB connectivity evidence is required only for paper broker simulation or broker submit/transmit, not manual capture conversion.", blocks=block_map(broker_submit=True), next_safe_action="Capture non-execution broker connectivity evidence only when paper-broker simulation or broker submit readiness is explicitly requested."),
        NodeSpec("trade_submit_readiness", "trade_submit_readiness_c2_v1", "ops/tools/run_trade_submit_readiness_c2_v1.py", "governance/04_DATA/SCHEMAS/C2/REPORTS/trade_submit_readiness_c2_v1.schema.json", "trade_submit_readiness_c2_v1", [str(trade_readiness)], upstream=["global_kill_switch_state", "capital_authority_allocation", "engine_activity_authorization", "ib_api_handshake"], repairability="EXTERNAL_SOURCE_REQUIRED", blocker_hint="Missing trade submit readiness evidence; this is broker-submit/paper-broker-sim readiness only and does not permit broker submit/transmit.", blocks=block_map(broker_submit=True), next_safe_action="Run readiness evidence after authorization, kill switch, and handshake are present only for broker-submit/paper-broker-sim checks; keep submit disabled."),
        NodeSpec("execution_package_build", "execution_build_v1", "ops/tools/run_execution_package_from_authorized_intent_v1.py", "governance/04_DATA/SCHEMAS/C2/RUNTIME/execution_build.v1.schema.json", "constellation_2.common.execution_build_authority_v1", [str(execution_build), str(execution_package)], [str(identity), str(economic_pkg), str(engine_auth), str(global_kill_switch)], ["candidate_identity_set", "global_context", "economic_state_package", "capital_authority_allocation", "engine_activity_authorization", "global_kill_switch_state"], blocks=block_map(conversion=True, submit_boundary=True, manual_capture_save=True), next_safe_action="Build the execution package only when declared pre-submit construction dependencies are present; broker connectivity remains outside manual capture conversion."),
        NodeSpec("conversion_package", "exposure_intent_paper_submission_package_v1", "ops/tools/run_exposure_intent_paper_submission_package_v1.py", "governance/04_DATA/SCHEMAS/C2/REPORTS/exposure_intent_paper_submission_package.v1.schema.json", "ops/tools/run_exposure_intent_paper_submission_package_v1.py", [str(conversion)], [str(economic_pkg), str(identity), str(risk), str(allocation), str(engine_auth)], ["symbol_universe", "market_data_readiness", "paper_trade_construction", "capital_authority_allocation", "risk_definition_contract", "candidate_identity_set", "global_context", "economic_state_package", "execution_package_build"], blocks=block_map(submit_boundary=True, manual_capture_save=True), next_safe_action="Rerun conversion after economic state and execution-package build dependencies are valid; do not attempt paper submit."),
        NodeSpec("paper_intent_evidence", "paper_intent_evidence_v1", "ops.aegis.trade_ticket_lineage_v1", "governance/04_DATA/SCHEMAS/C2/REPORTS/paper_intent_evidence.v1.schema.json", "ops.aegis.trade_ticket_lineage_v1", [str(truth_root / "reports/paper_intent_evidence_v1" / day / ticket_id.replace(":", "_") / "paper_intent_evidence.v1.json")], [str(construction)], ["paper_trade_construction"], blocks=block_map(submit_boundary=True, manual_capture_save=True), next_safe_action="Rebuild ticket evidence set from current construction."),
        NodeSpec("market_freshness_evidence", "market_freshness_evidence_v1", "ops.aegis.trade_ticket_lineage_v1", "governance/04_DATA/SCHEMAS/C2/REPORTS/market_freshness_evidence.v1.schema.json", "ops.aegis.trade_ticket_lineage_v1", [str(truth_root / "reports/market_freshness_evidence_v1" / day / ticket_id.replace(":", "_") / "market_freshness_evidence.v1.json")], [str(construction)], ["market_data_readiness", "paper_trade_construction"], blocks=block_map(submit_boundary=True, manual_capture_save=True), next_safe_action="Rebuild market freshness evidence from real market inputs."),
        NodeSpec("submit_boundary_precheck", "submit_boundary_precheck_v1", "ops/tools/build_submit_boundary_precheck_v1.py", "governance/04_DATA/SCHEMAS/C2/REPORTS/submit_boundary_precheck.v1.schema.json", "ops/tools/build_submit_boundary_precheck_v1.py", [str(truth_root / "reports/submit_boundary_precheck_v1" / day / ticket_id.replace(":", "_") / "submit_boundary_precheck.v1.json")], upstream=["conversion_package", "paper_intent_evidence", "market_freshness_evidence", "trade_ticket_lineage"], blocks=block_map(manual_capture_save=True), next_safe_action="Rebuild submit-boundary precheck after conversion evidence is valid."),
        NodeSpec("trade_ticket_lineage", "trade_ticket_lineage_v1", "ops.aegis.trade_ticket_lineage_v1", "governance/04_DATA/SCHEMAS/C2/REPORTS/trade_ticket_lineage.v1.schema.json", "ops.aegis.trade_ticket_lineage_v1", [str(truth_root / "reports/trade_ticket_lineage_v1" / day / ticket_id.replace(":", "_") / "trade_ticket_lineage.v1.json")], upstream=["conversion_package", "paper_intent_evidence", "market_freshness_evidence"], blocks=block_map(submit_boundary=True, manual_capture_save=True), next_safe_action="Rebuild lineage; it must remain read-only unless ACTIVE_CURRENT."),
        NodeSpec("ui_projection", "operator_state_snapshot_v1", "ops/tools/build_operator_state_snapshot_v1.py", "governance/04_DATA/SCHEMAS/C2/REPORTS/operator_state_snapshot.v1.schema.json", "ops/tools/build_operator_state_snapshot_v1.py", [str(truth_root / "reports/operator_state_snapshot_v1" / day / "operator_state_snapshot.v1.json"), str(truth_root / "reports/trade_ticket_projection_v1" / day / "trade_ticket_projection.v1.json")], upstream=["trade_ticket_lineage", "submit_boundary_precheck"], blocks=block_map(manual_capture_save=True), next_safe_action="Refresh operator projection after lineage/submit-boundary changes; stale tickets must remain read-only."),
        NodeSpec("manual_capture_save_endpoint", "manual_capture_save_endpoint", "ops.aegis.operator_state.manual_capture_record_v1.append_manual_capture_record_v1", "manual_capture_record_v1", "ops.aegis.operator_state.manual_capture_record_v1", [], [str(truth_root / "reports/trade_ticket_lineage_v1" / day / ticket_id.replace(":", "_") / "trade_ticket_lineage.v1.json"), str(truth_root / "reports/submit_boundary_precheck_v1" / day / ticket_id.replace(":", "_") / "submit_boundary_precheck.v1.json")], upstream=["ui_projection", "trade_ticket_lineage", "submit_boundary_precheck"], status_hint="REJECTED", blocker_hint="Manual capture save requires lineage_status=ACTIVE_CURRENT and submit-boundary EvidenceValidated; current lineage is MISSING_CONVERSION.", blocks=block_map(manual_capture_save=True), next_safe_action="Do not save; resolve conversion and submit-boundary first."),
        NodeSpec("manual_capture_evidence_append", "manual_capture_record_v1", "ops.aegis.operator_state.manual_capture_record_v1", "manual_capture_record_v1", "ops.aegis.operator_state.manual_capture_record_v1", [str(truth_root / "reports/manual_capture_record_v1" / day / "manual_capture_record.v1.jsonl")], upstream=["manual_capture_save_endpoint"], status_hint="NOT_APPLICABLE", blocker_hint="No append is allowed while save endpoint is rejected.", blocks=block_map(), next_safe_action="Append only after manual_capture_save_endpoint returns allowed=true."),
        NodeSpec("broker_submit_boundary", "broker_submit_transmit_boundary", "constellation_2.phaseD.lib.submit_boundary_paper_v4", "execution_submit_boundary", "constellation_2.phaseD.lib.submit_boundary_paper_v4", [], upstream=["manual_capture_evidence_append", "execution_package_build", "ib_api_handshake", "trade_submit_readiness"], status_hint="POLICY_DISABLED", blocker_hint="Broker submit/transmit remains disabled by RuntimeEvaluation; IB/readiness evidence can only satisfy broker-submit or paper-broker-sim readiness, not manual capture authority.", repairability="POLICY_DISABLED", blocks=block_map(broker_submit=True), next_safe_action="Keep disabled; do not invoke broker submit/transmit for manual capture diagnostics."),
    ]
    return specs, context


def apply_overrides_v1(nodes: list[dict[str, Any]], context: Mapping[str, Any], *, truth_root: Path, day: str) -> None:
    by_id = {n["node_id"]: n for n in nodes}
    # Paper construction may be complete while carrying submit-boundary missing as a downstream blocker; keep construction valid.
    construction = by_id.get("paper_trade_construction")
    if construction and construction["status"] in {"INVALID", "REJECTED"}:
        fields = construction.get("payload_status_fields") or {}
        if str(fields.get("trade_construction_status") or "").lower() == "complete":
            construction["status"] = "VALID"
    # Economic package is represented by a failed build plus missing package.
    economic = by_id.get("economic_state_package")
    if economic:
        first = context.get("economic_first_real_blocker") if isinstance(context.get("economic_first_real_blocker"), Mapping) else {}
        if first:
            economic["status"] = "INVALID"
            economic["exact_blocker_reason"] = str(first.get("detail") or first.get("dependency_id") or "")
    # Conversion is blocked by economic state in the current DOW path.
    conversion = by_id.get("conversion_package")
    if conversion:
        details = context.get("conversion_details") if isinstance(context.get("conversion_details"), Mapping) else {}
        blocker = conversion.get("exact_blocker_reason") or ""
        if blocker:
            conversion["status"] = "INVALID"
        if details.get("first_real_blocker"):
            conversion["exact_blocker_reason"] = str((details.get("first_real_blocker") or {}).get("detail") or blocker)
    target = by_id.get("target_day_admission")
    if target:
        _classify_target_day_admission_blockers_v1(target)
    positions = by_id.get("positions_snapshot")
    if positions:
        _classify_positions_static_paper_v1(positions)
    for runtime_node_id in ("candidate_identity_set", "capital_authority_allocation", "risk_definition_contract", "paper_trade_construction", "engine_activity_authorization"):
        node = by_id.get(runtime_node_id)
        if node:
            _classify_runtime_hash_v1(node, truth_root=truth_root, day=day)
    # Submit boundary is an explicit rejection, not missing.
    submit = by_id.get("submit_boundary_precheck")
    if submit and submit.get("payload_status_fields", {}).get("validation_status") == "REJECTED":
        submit["status"] = "REJECTED"
    lineage = by_id.get("trade_ticket_lineage")
    if lineage and lineage.get("payload_status_fields", {}).get("lineage_status") == "MISSING_CONVERSION":
        lineage["status"] = "INVALID"
        lineage["exact_blocker_reason"] = "MISSING_CONVERSION; ticket is read-only and not ACTIVE_CURRENT."
    ui = by_id.get("ui_projection")
    if ui:
        ui["status"] = "VALID"
        ui["exact_blocker_reason"] = "Projection is current enough to show DOW read-only; it does not authorize save."

    manual_save = by_id.get("manual_capture_save_endpoint")
    if manual_save:
        lineage_status = str((lineage or {}).get("payload_status_fields", {}).get("lineage_status") or "") if lineage else ""
        submit_status = str((submit or {}).get("payload_status_fields", {}).get("validation_status") or "") if submit else ""
        if lineage_status == "ACTIVE_CURRENT" and submit_status == "VALIDATED":
            manual_save["status"] = "VALID"
            manual_save["exact_blocker_reason"] = "Manual capture save precheck passed; endpoint may append a manual capture record only and must not invoke broker submit/transmit."
        else:
            manual_save["status"] = "REJECTED"
            manual_save["exact_blocker_reason"] = f"Manual capture save requires lineage_status=ACTIVE_CURRENT and submit-boundary EvidenceValidated; current lineage={lineage_status or 'MISSING'} submit_boundary={submit_status or 'MISSING'}."

    ib = by_id.get("ib_api_handshake")
    if ib:
        ib["ib_api_handshake_classification"] = "REQUIRED_FOR_PAPER_BROKER_SIM"
        ib["manual_capture_required"] = False
        ib["conversion_required"] = False
        ib["broker_submit_required"] = True
    readiness = by_id.get("trade_submit_readiness")
    if readiness:
        readiness["trade_submit_readiness_classification"] = "BROKER_SUBMIT_ONLY"
        readiness["manual_capture_required"] = False
        readiness["conversion_required"] = False
        readiness["broker_submit_required"] = True


def sleeve_summary_v1(truth_root: Path, day: str) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for sleeve in SLEEVES:
        eval_path = truth_root / "reports/sleeve_evaluation_kernel_v1" / day / sleeve / "sleeve_evaluation.v1.json"
        universe_path = truth_root / "reports/engine_universe_candidate_basis_v1" / day / sleeve / "engine_universe_candidate_basis.v1.json"
        eval_payload = read_json_v1(eval_path)
        universe_payload = read_json_v1(universe_path)
        rows.append({
            "sleeve_id": sleeve,
            "sleeve_evaluation_status": norm_status_v1(eval_payload, exists=eval_path.exists()),
            "sleeve_evaluation_path": str(eval_path),
            "symbol_universe_status": norm_status_v1(universe_payload, exists=universe_path.exists()) if universe_path.exists() else "NOT_APPLICABLE",
            "symbol_universe_path": str(universe_path) if universe_path.exists() else "",
            "active_manual_ticket": sleeve == "C2_MEAN_REVERSION_EQ_V1",
        })
    return rows


def hidden_downstream_v1(nodes: list[dict[str, Any]]) -> list[dict[str, str]]:
    by_id = {n["node_id"]: n for n in nodes}
    projected_ids: list[str] = []
    for row in static_downstream_read_nodes_v1():
        node_id = row["node_id"]
        if node_id not in projected_ids:
            projected_ids.append(node_id)
    for node_id in ("positions_snapshot", "position_lifecycle_snapshot", "accounting_nav"):
        if node_id not in projected_ids:
            projected_ids.append(node_id)
    out: list[dict[str, str]] = []
    for node_id in projected_ids:
        n = by_id.get(node_id)
        if not n:
            out.append({"node_id": node_id, "status": "MAP_INCOMPLETE", "reason": "HIDDEN_DEPENDENCY_DETECTED: downstream builder reads this node but it is absent from the dependency map."})
            continue
        if n["status"] in {"MISSING", "INVALID", "REJECTED", "STALE", "MANUAL_REQUIRED"}:
            out.append({"node_id": node_id, "status": n["status"], "reason": n.get("exact_blocker_reason", "") or n.get("next_safe_action", "")})
    return out


def cycle_risk_notes_v1(cycles: list[list[str]]) -> list[dict[str, str]]:
    notes = [
        {"risk": "construction -> allocation -> risk -> conversion", "assessment": "No cycle in map. Construction feeds allocation/risk/conversion; conversion does not feed construction authority."},
        {"risk": "candidate identity -> Phase C order plan", "assessment": "Candidate identity consumes Phase C order plan only as evidence and rejects mismatches; Phase C is not authoritative for selected candidate identity."},
        {"risk": "global context -> RuntimeEvaluation", "assessment": "RuntimeEvaluation authorizes target-day admission; global context consumes day activation. No edge from global context back into RuntimeEvaluation is used for permission."},
        {"risk": "economic state -> allocation/conversion", "assessment": "Potential design tension: economic state manifest also requires allocation while conversion requires economic state. The map keeps allocation upstream and flags economic state as the current downstream blocker; no graph cycle is introduced in the manual-capture path."},
    ]
    if cycles:
        notes.append({"risk": "detected_graph_cycle", "assessment": json.dumps(cycles, sort_keys=True)})
    return notes



def resolve_focus_ticket_id_v1(*, truth_root: Path, day_utc: str, requested_ticket_id: str) -> str:
    if requested_ticket_id != DOW_TICKET_ID:
        return requested_ticket_id
    base = truth_root / "reports" / "trade_ticket_lineage_v1" / day_utc
    active: list[tuple[int, str]] = []
    if base.exists():
        for path in base.glob("*/trade_ticket_lineage.v1.json"):
            payload = read_json_v1(path)
            ticket_id = str(payload.get("ticket_id") or "").strip()
            if ticket_id and str(payload.get("lineage_status") or "").strip() == "ACTIVE_CURRENT":
                active.append((path.stat().st_mtime_ns, ticket_id))
    if active:
        return sorted(active, key=lambda row: row[0])[-1][1]
    return requested_ticket_id

def build_dependency_map_v1(*, truth_root: Path | str, day_utc: str, sleeve_id: str = "C2_MEAN_REVERSION_EQ_V1", ticket_id: str = DOW_TICKET_ID) -> dict[str, Any]:
    root = Path(truth_root).expanduser().resolve()
    ticket_id = resolve_focus_ticket_id_v1(truth_root=root, day_utc=day_utc, requested_ticket_id=ticket_id)
    specs, context = build_specs_v1(truth_root=root, day=day_utc, sleeve_id=sleeve_id, ticket_id=ticket_id)
    events = event_index_v1(root, day_utc)
    contracts = producer_contracts_v1()
    nodes = [node_to_dict_v1(spec, truth_root=root, day=day_utc, event_index=events, contract_by_producer=contracts) for spec in specs]
    apply_overrides_v1(nodes, context, truth_root=root, day=day_utc)
    add_downstreams_v1(nodes)
    edges = [{"from": upstream, "to": node["node_id"]} for node in nodes for upstream in node.get("upstream_dependencies", [])]
    cycles = detect_cycles_v1(edges)
    root_blockers = classify_root_blockers_v1(nodes, edges)
    completeness = map_completeness_v1(nodes)
    by_id = {n["node_id"]: n for n in nodes}
    payload = {
        "schema_id": SCHEMA_ID,
        "schema_version": SCHEMA_VERSION,
        "day_utc": day_utc,
        "truth_root": str(root),
        "focus": {"sleeve_id": sleeve_id, "symbol": "DOW", "ticket_id": ticket_id, "intent_hash": DOW_INTENT_HASH},
        "RuntimeEvaluation_hash": str((read_json_v1(root / "reports/aegis_runtime_truth_kernel_v1" / day_utc / "runtime_evaluation.v1.json")).get("deterministic_output_hash") or ""),
        "nodes": nodes,
        "edges": edges,
        "root_blockers": root_blockers,
        "map_completeness": completeness,
        "all_sleeve_summary": sleeve_summary_v1(root, day_utc),
        "dow_dependency_status": {
            "conversion_status": by_id.get("conversion_package", {}).get("status"),
            "conversion_blocker": by_id.get("conversion_package", {}).get("exact_blocker_reason"),
            "submit_boundary_status": by_id.get("submit_boundary_precheck", {}).get("status"),
            "lineage_status": str(read_json_v1(Path((by_id.get("trade_ticket_lineage", {}).get("output_paths") or [""])[0])).get("lineage_status") or ""),
            "target_day_admission": by_id.get("target_day_admission", {}).get("status"),
            "day_activation": by_id.get("day_activation", {}).get("status"),
            "global_context": by_id.get("global_context", {}).get("status"),
        },
        "remaining_missing_or_invalid_after_global_context": [n for n in nodes if n["status"] in {"MISSING", "STALE", "INVALID", "REJECTED", "MANUAL_REQUIRED"} and n["node_id"] not in {"target_day_admission", "day_activation", "global_context"}],
        "likely_next_blockers_after_economic_state": hidden_downstream_v1(nodes),
        "cycle_detection": {"cycle_count": len(cycles), "cycles": cycles, "no_cycles_detected": not cycles, "cycle_risk_notes": cycle_risk_notes_v1(cycles)},
        "assertions": {
            "economic_state_cash_ledger_is_current_root_blocker": any(b["node_id"] == "cash_ledger_snapshot" for b in root_blockers),
            "economic_state_package_is_final_known_conversion_blocker_before_execution_package": by_id.get("conversion_package", {}).get("exact_blocker_reason", "").find("cash_ledger_snapshot_v1") >= 0,
            "no_cycles_detected": not cycles,
            "stale_phasec_authority_rejected": by_id.get("candidate_identity_set", {}).get("status") == "VALID" and bool(context.get("phasec_order_plan_path")),
            "manual_save_requires_submit_boundary": (by_id.get("manual_capture_save_endpoint", {}).get("status") == "VALID") == (by_id.get("submit_boundary_precheck", {}).get("status") == "VALID"),
            "broker_submit_disabled": by_id.get("broker_submit_boundary", {}).get("status") == "POLICY_DISABLED",
            "ib_not_required_for_manual_capture": by_id.get("ib_api_handshake", {}).get("blocks", {}).get("manual_capture_save") is False and by_id.get("ib_api_handshake", {}).get("blocks", {}).get("conversion") is False,
            "broker_readiness_not_required_for_manual_capture": by_id.get("trade_submit_readiness", {}).get("blocks", {}).get("manual_capture_save") is False and by_id.get("trade_submit_readiness", {}).get("blocks", {}).get("conversion") is False,
        },
        "safety": {"trade_advice_allowed": False, "autonomous_execution_allowed": False, "broker_submit_transmit_allowed": False, "diagnostic_only": True},
    }
    payload["artifact_id"] = f"aegis_dependency_map_v1:{day_utc}:{stable_hash_v1(payload)[:20]}"
    payload["dependency_map_hash"] = stable_hash_v1({**payload, "dependency_map_hash": ""})
    return payload


def render_text_v1(payload: Mapping[str, Any]) -> str:
    lines = [
        "aegis_dependency_map.v1",
        f"day_utc: {payload.get('day_utc')}",
        f"RuntimeEvaluation_hash: {payload.get('RuntimeEvaluation_hash')}",
        f"focus: {payload.get('focus')}",
        "",
        "root blockers:",
    ]
    for b in payload.get("root_blockers") or []:
        lines.append(f"- {b.get('node_id')}: {b.get('status')} - {b.get('exact_blocker_reason') or b.get('next_safe_action')}")
    lines.extend(["", "dependency nodes:"])
    for n in payload.get("nodes") or []:
        blocks = ",".join(k for k, v in (n.get("blocks") or {}).items() if v) or "none"
        lines.append(f"- {n.get('node_id')}: status={n.get('status')} blocks={blocks}")
        if n.get("exact_blocker_reason"):
            lines.append(f"  blocker: {n.get('exact_blocker_reason')}")
        outs = [p for p in n.get("output_paths") or [] if p]
        if outs:
            lines.append(f"  output: {outs[0]}")
        if n.get("next_safe_action"):
            lines.append(f"  next: {n.get('next_safe_action')}")
    lines.extend(["", "all sleeves:"])
    for row in payload.get("all_sleeve_summary") or []:
        lines.append(f"- {row.get('sleeve_id')}: eval={row.get('sleeve_evaluation_status')} universe={row.get('symbol_universe_status')} active_ticket={row.get('active_manual_ticket')}")
    lines.extend(["", "cycle risks:"])
    for row in (payload.get("cycle_detection") or {}).get("cycle_risk_notes") or []:
        lines.append(f"- {row.get('risk')}: {row.get('assessment')}")
    lines.extend(["", "safety:", "- trade_advice_allowed: false", "- autonomous_execution_allowed: false", "- broker_submit_transmit_allowed: false"])
    return "\n".join(lines) + "\n"


def render_dot_v1(payload: Mapping[str, Any]) -> str:
    lines = ["digraph aegis_dependency_map_v1 {", "  rankdir=LR;", "  node [shape=box,fontname=Helvetica];"]
    by_id = {n["node_id"]: n for n in payload.get("nodes") or []}
    for node_id, node in by_id.items():
        status = node.get("status")
        color = "green" if status == "VALID" else "red" if status in {"MISSING", "INVALID", "REJECTED"} else "gray"
        label = f"{node_id}\\n{status}"
        lines.append(f'  "{node_id}" [label="{label}", color="{color}"];')
    for edge in payload.get("edges") or []:
        lines.append(f'  "{edge["from"]}" -> "{edge["to"]}";')
    lines.append("}")
    return "\n".join(lines) + "\n"


def write_dependency_reports_v1(*, truth_root: Path | str, payload: Mapping[str, Any]) -> tuple[Path, Path, Path, Path]:
    root = Path(truth_root).expanduser().resolve()
    json_path, txt_path, dot_path, blockers_path = output_paths_v1(root, str(payload["day_utc"]))
    json_path.parent.mkdir(parents=True, exist_ok=True)
    json_path.write_bytes(canonical_json_bytes_v1(dict(payload)) + b"\n")
    txt_path.write_text(render_text_v1(payload), encoding="utf-8")
    dot_path.write_text(render_dot_v1(payload), encoding="utf-8")
    blockers = {
        "schema_id": "aegis_dependency_root_blockers",
        "schema_version": "v1",
        "day_utc": payload["day_utc"],
        "RuntimeEvaluation_hash": payload.get("RuntimeEvaluation_hash", ""),
        "root_blockers": payload.get("root_blockers", []),
        "likely_next_blockers_after_economic_state": payload.get("likely_next_blockers_after_economic_state", []),
        "map_completeness": payload.get("map_completeness", {}),
        "safety": payload.get("safety", {}),
    }
    blockers_path.write_bytes(canonical_json_bytes_v1(blockers) + b"\n")
    return json_path, txt_path, dot_path, blockers_path


def validate_dependency_map_v1(payload: Mapping[str, Any]) -> list[str]:
    required = {
        "symbol_universe", "market_data_readiness", "sleeve_readiness", "sleeve_evaluation", "candidate_generation", "candidate_selection", "paper_trade_construction", "capital_authority_allocation", "risk_definition_contract", "candidate_identity_set", "day_activation", "target_day_admission", "global_context", "economic_state_package", "cash_ledger_snapshot", "engine_activity_authorization", "global_kill_switch_state", "ib_api_handshake", "trade_submit_readiness", "execution_package_build", "conversion_package", "paper_intent_evidence", "market_freshness_evidence", "submit_boundary_precheck", "trade_ticket_lineage", "ui_projection", "manual_capture_save_endpoint", "manual_capture_evidence_append", "broker_submit_boundary",
    }
    node_ids = {str(n.get("node_id")) for n in payload.get("nodes") or [] if isinstance(n, Mapping)}
    failures = [f"MISSING_NODE:{node}" for node in sorted(required - node_ids)]
    if not (payload.get("cycle_detection") or {}).get("no_cycles_detected", False) and (payload.get("cycle_detection") or {}).get("cycle_count"):
        failures.append("CYCLES_DETECTED")
    completeness = payload.get("map_completeness") if isinstance(payload.get("map_completeness"), Mapping) else {}
    if completeness.get("status") == "FAIL":
        for row in completeness.get("hidden_dependencies") or []:
            if isinstance(row, Mapping):
                failures.append(f"HIDDEN_DEPENDENCY_DETECTED:{row.get('node_id')}:{row.get('builder')}")
    by_id = {n.get("node_id"): n for n in payload.get("nodes") or [] if isinstance(n, Mapping)}
    if by_id.get("cash_ledger_snapshot", {}).get("status") in {"MISSING", "INVALID", "REJECTED", "STALE", "MANUAL_REQUIRED"} and not any(b.get("node_id") == "cash_ledger_snapshot" for b in payload.get("root_blockers") or []):
        failures.append("CASH_LEDGER_BLOCKER_NOT_ROOT_WHEN_MISSING")
    manual_status = by_id.get("manual_capture_save_endpoint", {}).get("status")
    submit_status = by_id.get("submit_boundary_precheck", {}).get("status")
    if manual_status == "VALID" and submit_status != "VALID":
        failures.append("MANUAL_SAVE_VALID_WITHOUT_SUBMIT_BOUNDARY")
    if manual_status == "REJECTED" and submit_status == "VALID":
        failures.append("MANUAL_SAVE_REJECTED_DESPITE_VALID_SUBMIT_BOUNDARY")
    return failures


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="build_aegis_dependency_map_v1")
    parser.add_argument("--truth-root", "--truth_root", dest="truth_root", required=True)
    parser.add_argument("--day-utc", "--day_utc", "--day", dest="day_utc", required=True)
    parser.add_argument("--sleeve-id", "--sleeve_id", dest="sleeve_id", default="C2_MEAN_REVERSION_EQ_V1")
    parser.add_argument("--ticket-id", "--ticket_id", dest="ticket_id", default=DOW_TICKET_ID)
    args = parser.parse_args(argv)
    payload = build_dependency_map_v1(truth_root=Path(args.truth_root), day_utc=str(args.day_utc), sleeve_id=str(args.sleeve_id), ticket_id=str(args.ticket_id))
    failures = validate_dependency_map_v1(payload)
    payload["validation_status"] = "PASS" if not failures else "FAIL"
    payload["validation_failures"] = failures
    json_path, txt_path, dot_path, blockers_path = write_dependency_reports_v1(truth_root=Path(args.truth_root), payload=payload)
    print(json.dumps({
        "validation_status": payload["validation_status"],
        "json_path": str(json_path),
        "txt_path": str(txt_path),
        "dot_path": str(dot_path),
        "root_blockers_path": str(blockers_path),
        "root_blockers": payload.get("root_blockers"),
        "trade_advice_allowed": False,
        "autonomous_execution_allowed": False,
        "broker_submit_transmit_allowed": False,
    }, sort_keys=True))
    return 0 if not failures else 2


if __name__ == "__main__":
    raise SystemExit(main())
