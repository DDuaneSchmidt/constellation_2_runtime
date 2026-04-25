from __future__ import annotations

import traceback
from pathlib import Path
from typing import Any, Dict, List, Optional

from constellation_2.common.diagnostic_foundation_v1 import (
    _read_json_obj,
    _require_day_utc,
    _require_produced_utc,
)
from constellation_2.phaseD.lib.canon_json_v1 import canonical_json_bytes_v1
from constellation_2.phaseD.lib.validate_against_schema_v1 import (
    validate_against_repo_schema_v1,
)
from constellation_2.phaseF.accounting.lib.day_artifact_refresh_v1 import (
    RefreshWriteResultV1,
    write_day_artifact_refreshable_v1,
)


LIFECYCLE_POLICY_ID = "C2_LIFECYCLE_STATE_AUTHORITY_POLICY_V1"
LIFECYCLE_POLICY_VERSION = 1
GATE_CLASSIFICATION_REGISTRY_ID = "GATE_CLASSIFICATION_REGISTRY_V1"
GATE_CLASSIFICATION_REGISTRY_VERSION = 1
LEGACY_GATE_HIERARCHY_ID = "GATE_HIERARCHY_V1"
LEGACY_GATE_HIERARCHY_VERSION = 1
BOOTSTRAP_EXECUTION_POLICY_ID = "C2_BOOTSTRAP_EXECUTION_POLICY_V1"
BOOTSTRAP_EXECUTION_POLICY_VERSION = 1

LIFECYCLE_POLICY_RELPATH = "governance/02_REGISTRIES/C2_LIFECYCLE_STATE_AUTHORITY_POLICY_V1.json"
GATE_CLASSIFICATION_RELPATH = "governance/02_REGISTRIES/GATE_CLASSIFICATION_REGISTRY_V1.json"
GATE_HIERARCHY_RELPATH = "governance/02_REGISTRIES/GATE_HIERARCHY_V1.json"
BOOTSTRAP_EXECUTION_POLICY_RELPATH = "governance/02_REGISTRIES/C2_BOOTSTRAP_EXECUTION_POLICY_V1.json"

LIFECYCLE_STATE_SCHEMA = "governance/04_DATA/SCHEMAS/C2/REPORTS/lifecycle_state_authority.v1.schema.json"
AUTHORIZATION_VERDICT_SCHEMA = "governance/04_DATA/SCHEMAS/C2/REPORTS/authorization_gate_verdict.v1.schema.json"
ECONOMIC_VERDICT_SCHEMA = "governance/04_DATA/SCHEMAS/C2/REPORTS/economic_health_gate_verdict.v1.schema.json"
LEDGER_SCHEMA = "governance/04_DATA/SCHEMAS/C2/REPORTS/gate_decision_ledger.v1.schema.json"

STATE_ORDER = {
    "PRE_TRADE": 1,
    "POST_TRADE_ECONOMIC": 2,
}

PASS_STATUSES = {"PASS", "OK"}
EXECUTION_ALLOWED_STATUSES = {"PASS", "BOOTSTRAP_PASS"}


def _read_registry(repo_root: Path, relpath: str) -> Dict[str, Any]:
    return _read_json_obj((repo_root / relpath).resolve())


def _scope_from_truth_root(truth_root: Path, day_utc: str, mode: Optional[str] = None) -> Dict[str, Any]:
    parts = list(truth_root.parts)
    sleeve_id = None
    inferred_mode = mode
    if "truth_sleeves" in parts:
        idx = parts.index("truth_sleeves")
        if len(parts) > idx + 2:
            sleeve_id = parts[idx + 1]
            inferred_mode = inferred_mode or parts[idx + 2]
    return {
        "day_utc": day_utc,
        "mode": inferred_mode,
        "sleeve_id": sleeve_id,
        "truth_partition": str(truth_root),
    }


def _read_optional_json(path: Path) -> Optional[Dict[str, Any]]:
    if not path.exists() or not path.is_file():
        return None
    try:
        return _read_json_obj(path)
    except Exception:
        return None


def _actual_economic_paths(truth_root: Path, day_utc: str) -> Dict[str, Path]:
    return {
        "positions_snapshot_v2": (truth_root / "positions_v1" / "snapshots" / day_utc / "positions_snapshot.v2.json").resolve(),
        "cash_ledger_snapshot_v1": (truth_root / "cash_ledger_v1" / "snapshots" / day_utc / "cash_ledger_snapshot.v1.json").resolve(),
        "broker_statement_normalized_v1": (
            truth_root / "execution_evidence_v1" / "broker_statement_normalized_v1" / day_utc / "broker_statement_normalized.v1.json"
        ).resolve(),
        "broker_marks_v1": (truth_root / "market_data_snapshot_v1" / "broker_marks_v1" / day_utc / "broker_marks.v1.json").resolve(),
        "accounting_nav_v2": (truth_root / "accounting_v2" / "nav" / day_utc / "nav.v2.json").resolve(),
        "exit_reconciliation_v1": (truth_root / "exit_reconciliation_v1" / day_utc / "exit_reconciliation.v1.json").resolve(),
    }


def _lifecycle_input_paths(truth_root: Path, day_utc: str) -> Dict[str, Path]:
    paths = _actual_economic_paths(truth_root, day_utc)
    paths.update(
        {
            "gate_stack_verdict_v1": (truth_root / "reports" / "gate_stack_verdict_v1" / day_utc / "gate_stack_verdict.v1.json").resolve(),
            "lifecycle_progression_status_v1": (
                truth_root / "reports" / "lifecycle_progression_status_v1" / day_utc / "lifecycle_progression_status.v1.json"
            ).resolve(),
            "economic_finalization_status_v1": (
                truth_root / "reports" / "economic_finalization_status_v1" / day_utc / "economic_finalization_status.v1.json"
            ).resolve(),
            "intent_snapshot_root": (truth_root / "intents_v1" / "snapshots" / day_utc).resolve(),
        }
    )
    return paths


def collect_gate_authority_facts(repo_root: Path, truth_root: Path, day_utc: str) -> Dict[str, Any]:
    paths = _lifecycle_input_paths(truth_root, day_utc)
    gate_stack_doc = _read_optional_json(paths["gate_stack_verdict_v1"])
    lifecycle_doc = _read_optional_json(paths["lifecycle_progression_status_v1"])
    economic_doc = _read_optional_json(paths["economic_finalization_status_v1"])
    intent_root = paths["intent_snapshot_root"]
    intent_snapshot_paths = []
    if intent_root.exists() and intent_root.is_dir():
        intent_snapshot_paths = sorted(
            [str(path.resolve()) for path in intent_root.iterdir() if path.is_file() and path.name.endswith(".json")]
        )
    economic_presence = {
        key: path.exists() and path.is_file()
        for key, path in _actual_economic_paths(truth_root, day_utc).items()
    }
    return {
        "paths": paths,
        "gate_stack_doc": gate_stack_doc,
        "lifecycle_doc": lifecycle_doc,
        "economic_doc": economic_doc,
        "intent_snapshot_paths": intent_snapshot_paths,
        "economic_presence": economic_presence,
    }


def derive_lifecycle_state_authority_doc(
    repo_root: Path,
    truth_root: Path,
    day_utc: str,
    produced_utc: str,
    mode: str,
    facts: Dict[str, Any],
) -> Dict[str, Any]:
    policy = _read_registry(repo_root, LIFECYCLE_POLICY_RELPATH)
    run_scope = _scope_from_truth_root(truth_root, day_utc, mode)
    economic_presence = facts["economic_presence"]

    if any(bool(v) for v in economic_presence.values()):
        lifecycle_state = "POST_TRADE_ECONOMIC"
        reasons = [
            "ACTUAL_ECONOMIC_ARTIFACT_PRESENT",
            "POST_TRADE_ECONOMIC_STATE_REQUIRES_REAL_ECONOMIC_TRUTH_ARTIFACTS",
        ]
    else:
        lifecycle_state = "PRE_TRADE"
        reasons = [
            "NO_ACTUAL_ECONOMIC_TRUTH_ARTIFACTS_PRESENT",
            "PRE_TRADE_STATE_SELECTED_FOR_BOOTSTRAP_OR_NO_EXECUTION_DAY",
        ]

    derivation_inputs = []
    evidence_refs = []
    for key, path in _actual_economic_paths(truth_root, day_utc).items():
        derivation_inputs.append(
            {
                "artifact_family": key,
                "path": str(path),
                "present": bool(path.exists() and path.is_file()),
            }
        )
        if path.exists():
            evidence_refs.append(str(path))
    gate_stack_path = facts["paths"]["gate_stack_verdict_v1"]
    lifecycle_path = facts["paths"]["lifecycle_progression_status_v1"]
    if gate_stack_path.exists():
        evidence_refs.append(str(gate_stack_path))
    if lifecycle_path.exists():
        evidence_refs.append(str(lifecycle_path))
    evidence_refs.extend(facts.get("intent_snapshot_paths", []))

    doc = {
        "schema_id": "lifecycle_state_authority_v1",
        "schema_version": 1,
        "lifecycle_policy_id": policy["lifecycle_policy_id"],
        "lifecycle_policy_version": policy["lifecycle_policy_version"],
        "day_utc": day_utc,
        "mode": mode,
        "run_scope": run_scope,
        "produced_utc": produced_utc,
        "lifecycle_state": lifecycle_state,
        "derivation_inputs": derivation_inputs,
        "derivation_reasons": reasons,
        "evidence_refs": sorted(set(evidence_refs)),
    }
    validate_against_repo_schema_v1(doc, repo_root, LIFECYCLE_STATE_SCHEMA)
    return doc


def _state_is_applicable(current_state: str, earliest_state: str) -> bool:
    return STATE_ORDER[current_state] >= STATE_ORDER[earliest_state]


def _classification_map(repo_root: Path) -> Dict[str, Dict[str, Any]]:
    registry = _read_registry(repo_root, GATE_CLASSIFICATION_RELPATH)
    rows = registry.get("gates")
    if not isinstance(rows, list) or not rows:
        raise RuntimeError("GATE_CLASSIFICATION_REGISTRY_EMPTY")
    by_id: Dict[str, Dict[str, Any]] = {}
    for row in rows:
        if not isinstance(row, dict):
            continue
        gate_id = str(row.get("gate_id") or "").strip()
        if not gate_id:
            raise RuntimeError("GATE_CLASSIFICATION_ROW_MISSING_GATE_ID")
        by_id[gate_id] = row
    return by_id


def _bootstrap_policy(repo_root: Path) -> Dict[str, Any]:
    policy_path = (repo_root / BOOTSTRAP_EXECUTION_POLICY_RELPATH).resolve()
    effective_repo_root = repo_root
    if not policy_path.exists() or not policy_path.is_file():
        effective_repo_root = Path(__file__).resolve().parents[2]
    policy = _read_registry(effective_repo_root, BOOTSTRAP_EXECUTION_POLICY_RELPATH)
    if str(policy.get('bootstrap_execution_policy_id') or '').strip() != BOOTSTRAP_EXECUTION_POLICY_ID:
        raise RuntimeError('BOOTSTRAP_EXECUTION_POLICY_ID_MISMATCH')
    return policy

def _gate_stack_rows(facts: Dict[str, Any]) -> List[Dict[str, Any]]:
    gate_stack_doc = facts.get("gate_stack_doc")
    if not isinstance(gate_stack_doc, dict):
        raise RuntimeError("GATE_STACK_VERDICT_MISSING_OR_INVALID")
    rows = gate_stack_doc.get("gates")
    if not isinstance(rows, list):
        raise RuntimeError("GATE_STACK_VERDICT_GATES_MISSING")
    return [row for row in rows if isinstance(row, dict)]


def _derive_authorization_coherence_reason_codes(
    *,
    facts: Dict[str, Any],
    gate_rows: List[Dict[str, Any]],
    included: List[Dict[str, Any]],
    excluded: List[Dict[str, Any]],
    blocking: List[Dict[str, Any]],
    authorization_status: str,
    bootstrap_mode_applied: bool,
) -> List[str]:
    gate_stack_doc = facts.get("gate_stack_doc")
    if not isinstance(gate_stack_doc, dict):
        raise RuntimeError("GATE_STACK_VERDICT_MISSING_OR_INVALID")
    gate_stack_status = str(gate_stack_doc.get("status") or "").strip().upper()
    if gate_stack_status not in {"PASS", "FAIL"}:
        raise RuntimeError(f"GATE_STACK_VERDICT_STATUS_INVALID:{gate_stack_status or 'UNKNOWN'}")

    included_failing = sorted(
        {
            str(row.get("gate_id") or "").strip()
            for row in included
            if str(row.get("observed_status") or "").strip().upper() not in PASS_STATUSES
            and str(row.get("gate_id") or "").strip()
        }
    )
    excluded_failing = sorted(
        {
            str(row.get("gate_id") or "").strip()
            for row in excluded
            if str(row.get("observed_status") or "").strip().upper() not in PASS_STATUSES
            and str(row.get("gate_id") or "").strip()
        }
    )
    gate_stack_failing = sorted(
        {
            str(row.get("gate_id") or "").strip()
            for row in gate_rows
            if str(row.get("observed_status") or "").strip().upper() not in PASS_STATUSES
            and str(row.get("gate_id") or "").strip()
        }
    )

    if authorization_status == "PASS" and included_failing and not bootstrap_mode_applied:
        raise RuntimeError(
            "AUTHORIZATION_GATE_STACK_INCOHERENT:PASS_WITH_INCLUDED_FAILING_GATES:"
            + ",".join(included_failing)
        )

    if gate_stack_status == "PASS" and authorization_status == "FAIL":
        raise RuntimeError("AUTHORIZATION_GATE_STACK_INCOHERENT:GATE_STACK_PASS_AUTHORIZATION_FAIL")

    if authorization_status != "PASS":
        return []

    if gate_stack_status == "PASS":
        return ["AUTHORIZATION_GATE_STACK_ALIGNED"]

    if bootstrap_mode_applied:
        return ["AUTHORIZATION_BOOTSTRAP_OVERRIDE_APPLIED"]

    if not gate_stack_failing:
        raise RuntimeError("AUTHORIZATION_GATE_STACK_INCOHERENT:GATE_STACK_FAIL_WITHOUT_FAILING_GATES")

    unexplained = sorted(set(gate_stack_failing) - set(excluded_failing))
    if unexplained:
        raise RuntimeError(
            "AUTHORIZATION_GATE_STACK_INCOHERENT:UNEXPLAINED_FAILING_GATES:" + ",".join(unexplained)
        )

    if not blocking and gate_stack_failing:
        return [
            "AUTHORIZATION_DERIVED_FROM_CLASSIFIED_GATE_SUBSET",
            *[f"AUTHORIZATION_EXCLUDED_FAILING_GATE:{gate_id}" for gate_id in excluded_failing],
        ]

    return [
        "AUTHORIZATION_DERIVED_FROM_CLASSIFIED_GATE_SUBSET",
        *[f"AUTHORIZATION_EXCLUDED_FAILING_GATE:{gate_id}" for gate_id in excluded_failing],
    ]


def _legacy_hierarchy_map(repo_root: Path) -> Dict[str, Dict[str, Any]]:
    reg = _read_registry(repo_root, GATE_HIERARCHY_RELPATH)
    gates = reg.get("gates")
    if not isinstance(gates, list):
        raise RuntimeError("GATE_HIERARCHY_MISSING_GATES")
    return {str(row.get("gate_id") or "").strip(): row for row in gates if isinstance(row, dict)}


def _build_gate_evaluation_rows(
    repo_root: Path,
    lifecycle_doc: Dict[str, Any],
    facts: Dict[str, Any],
) -> List[Dict[str, Any]]:
    current_state = str(lifecycle_doc["lifecycle_state"])
    class_map = _classification_map(repo_root)
    hierarchy_map = _legacy_hierarchy_map(repo_root)
    rows: List[Dict[str, Any]] = []

    for gate_row in _gate_stack_rows(facts):
        gate_id = str(gate_row.get("gate_id") or "").strip()
        if gate_id not in class_map:
            raise RuntimeError(f"GATE_CLASSIFICATION_MISSING:{gate_id}")
        classification = class_map[gate_id]
        hierarchy = hierarchy_map.get(gate_id)
        if not isinstance(hierarchy, dict):
            raise RuntimeError(f"GATE_HIERARCHY_ENTRY_MISSING:{gate_id}")

        earliest_state = str(classification["earliest_valid_lifecycle_state"])
        applicable = _state_is_applicable(current_state, earliest_state)
        include_auth = applicable and bool(classification.get("required_in_authorization_verdict"))
        include_economic = applicable and bool(classification.get("required_in_economic_health_verdict"))

        exclusion_reasons: List[str] = []
        if not applicable:
            exclusion_reasons.append(f"LIFECYCLE_STATE_BEFORE_{earliest_state}")
        if not classification.get("required_in_authorization_verdict"):
            exclusion_reasons.append("NOT_REQUIRED_IN_AUTHORIZATION_VERDICT")
        if not classification.get("required_in_economic_health_verdict"):
            exclusion_reasons.append("NOT_REQUIRED_IN_ECONOMIC_HEALTH_VERDICT")

        rows.append(
            {
                "gate_id": gate_id,
                "gate_version": classification["gate_version"],
                "legacy_gate_class": str(gate_row.get("gate_class") or hierarchy.get("class") or ""),
                "legacy_required": bool(gate_row.get("required")),
                "legacy_blocking": bool(gate_row.get("blocking")),
                "observed_status": str(gate_row.get("status") or "UNKNOWN").strip().upper(),
                "artifact_path": str(gate_row.get("artifact_path") or ""),
                "artifact_sha256": str(gate_row.get("artifact_sha256") or ""),
                "reason_codes": list(gate_row.get("reason_codes") or []),
                "bundle_membership": list(classification.get("bundle_membership") or []),
                "earliest_valid_lifecycle_state": earliest_state,
                "blocking_domain": classification["blocking_domain"],
                "action_domain": classification["action_domain"],
                "operator_meaning": classification["operator_meaning"],
                "included_in_authorization_verdict": include_auth,
                "included_in_economic_health_verdict": include_economic,
                "exclusion_reasons": exclusion_reasons,
            }
        )
    return rows


def _verdict_artifact_ref(truth_root: Path, family: str, day_utc: str, filename: str) -> str:
    return str((truth_root / "reports" / family / day_utc / filename).resolve())


def build_authorization_gate_verdict_doc(
    repo_root: Path,
    truth_root: Path,
    day_utc: str,
    produced_utc: str,
    mode: str,
    lifecycle_doc: Dict[str, Any],
    facts: Dict[str, Any],
    gate_rows: List[Dict[str, Any]],
) -> Dict[str, Any]:
    bootstrap_policy = _bootstrap_policy(repo_root)
    included = [row for row in gate_rows if row["included_in_authorization_verdict"]]
    excluded = [row for row in gate_rows if not row["included_in_authorization_verdict"]]
    blocking = [row for row in included if row["observed_status"] not in PASS_STATUSES]
    bootstrap_mode_applied = False
    bootstrap_allowed_missing_gates: List[str] = []

    status = "PASS" if not blocking else "FAIL"
    blocking_class = "NONE" if not blocking else str(blocking[0]["legacy_gate_class"] or "CLASS1_SYSTEM_HARD_STOP")
    reason_codes = ["AUTHORIZATION_GATES_PASS"] if status == "PASS" else [
        f"AUTHORIZATION_GATE_NOT_PASS:{row['gate_id']}:{row['observed_status']}" for row in blocking
    ]

    lifecycle_state = str(lifecycle_doc.get("lifecycle_state") or "")
    no_economic_truth = not any(bool(v) for v in facts["economic_presence"].values())
    policy_rows = bootstrap_policy.get("states")
    bootstrap_row: Optional[Dict[str, Any]] = None
    if isinstance(policy_rows, list):
        for row in policy_rows:
            if isinstance(row, dict) and str(row.get("lifecycle_state") or "").strip() == lifecycle_state:
                bootstrap_row = row
                break

    if status == "FAIL" and bootstrap_row is not None and lifecycle_state == "PRE_TRADE" and no_economic_truth:
        bootstrap_allowed_missing_gates = sorted(
            [str(x).strip() for x in (bootstrap_row.get("allowed_missing_authorization_gates") or []) if str(x).strip()]
        )
        required_passing = sorted(
            [str(x).strip() for x in (bootstrap_row.get("required_passing_authorization_gates") or []) if str(x).strip()]
        )
        blocking_ids = sorted({str(row["gate_id"]) for row in blocking})
        required_failures = [
            row for row in included
            if (str(row["gate_id"]) in required_passing) and (str(row["observed_status"]) not in PASS_STATUSES)
        ]
        if set(blocking_ids).issubset(set(bootstrap_allowed_missing_gates)) and not required_failures:
            status = str(bootstrap_row.get("authorization_override_status") or "BOOTSTRAP_PASS").strip() or "BOOTSTRAP_PASS"
            blocking_class = "CLASS3_CONTROLLED_DEGRADATION"
            bootstrap_mode_applied = True
            reason_codes = [
                "BOOTSTRAP_EXECUTION_MODE_APPLIED",
                *[f"BOOTSTRAP_ALLOWED_MISSING_GATE:{gate_id}" for gate_id in blocking_ids],
            ]

    coherence_reason_codes = _derive_authorization_coherence_reason_codes(
        facts=facts,
        gate_rows=gate_rows,
        included=included,
        excluded=excluded,
        blocking=blocking,
        authorization_status=status,
        bootstrap_mode_applied=bootstrap_mode_applied,
    )
    reason_codes = sorted(set(reason_codes + coherence_reason_codes))

    evidence_refs = [row["artifact_path"] for row in included if row["artifact_path"]]
    evidence_refs.append(_verdict_artifact_ref(truth_root, "lifecycle_state_authority_v1", day_utc, "lifecycle_state_authority.v1.json"))

    doc = {
        'schema_id': 'authorization_gate_verdict_v1',
        'schema_version': 1,
        'gate_classification_registry_id': GATE_CLASSIFICATION_REGISTRY_ID,
        'gate_classification_registry_version': GATE_CLASSIFICATION_REGISTRY_VERSION,
        'lifecycle_state_authority_ref': _verdict_artifact_ref(
            truth_root, 'lifecycle_state_authority_v1', day_utc, 'lifecycle_state_authority.v1.json'
        ),
        'day_utc': day_utc,
        'mode': mode,
        'produced_utc': produced_utc,
        'included_gates': [
            {
                'gate_id': row['gate_id'],
                'status': row['observed_status'],
                'artifact_path': row['artifact_path'],
                'artifact_sha256': row['artifact_sha256'],
                'reason_codes': row['reason_codes'],
            }
            for row in included
        ],
        'excluded_gates': [
            {
                'gate_id': row['gate_id'],
                'status': row['observed_status'],
                'exclusion_reasons': row['exclusion_reasons'],
            }
            for row in excluded
        ],
        'blocking_gates': [
            {
                'gate_id': row['gate_id'],
                'status': row['observed_status'],
                'blocking_domain': row['blocking_domain'],
                'artifact_path': row['artifact_path'],
                'reason_codes': row['reason_codes'],
            }
            for row in blocking
        ],
        'status': status,
        'blocking_class': blocking_class,
        'reason_codes': reason_codes,
        'evidence_refs': sorted(set(evidence_refs)),
        'decision_ledger_ref': _verdict_artifact_ref(truth_root, 'gate_decision_ledger_v1', day_utc, 'gate_decision_ledger.v1.json'),
    }
    validate_against_repo_schema_v1(doc, repo_root, AUTHORIZATION_VERDICT_SCHEMA)
    return doc


def build_economic_health_gate_verdict_doc(
    repo_root: Path,
    truth_root: Path,
    day_utc: str,
    produced_utc: str,
    mode: str,
    lifecycle_doc: Dict[str, Any],
    gate_rows: List[Dict[str, Any]],
) -> Dict[str, Any]:
    included = [row for row in gate_rows if row["included_in_economic_health_verdict"]]
    excluded = [row for row in gate_rows if not row["included_in_economic_health_verdict"]]
    blocking = [row for row in included if row["observed_status"] not in PASS_STATUSES]

    status = "PASS" if not blocking else "FAIL"
    blocking_class = "NONE" if not blocking else str(blocking[0]["legacy_gate_class"] or "CLASS1_SYSTEM_HARD_STOP")
    reason_codes = ["ECONOMIC_HEALTH_GATES_PASS"] if status == "PASS" else [
        f"ECONOMIC_HEALTH_GATE_NOT_PASS:{row['gate_id']}:{row['observed_status']}" for row in blocking
    ]
    evidence_refs = [row["artifact_path"] for row in included if row["artifact_path"]]
    evidence_refs.append(_verdict_artifact_ref(truth_root, "lifecycle_state_authority_v1", day_utc, "lifecycle_state_authority.v1.json"))

    doc = {
        "schema_id": "economic_health_gate_verdict_v1",
        "schema_version": 1,
        "gate_classification_registry_id": GATE_CLASSIFICATION_REGISTRY_ID,
        "gate_classification_registry_version": GATE_CLASSIFICATION_REGISTRY_VERSION,
        "lifecycle_state_authority_ref": _verdict_artifact_ref(
            truth_root, "lifecycle_state_authority_v1", day_utc, "lifecycle_state_authority.v1.json"
        ),
        "day_utc": day_utc,
        "mode": mode,
        "produced_utc": produced_utc,
        "included_gates": [
            {
                "gate_id": row["gate_id"],
                "status": row["observed_status"],
                "artifact_path": row["artifact_path"],
                "artifact_sha256": row["artifact_sha256"],
                "reason_codes": row["reason_codes"],
            }
            for row in included
        ],
        "excluded_gates": [
            {
                "gate_id": row["gate_id"],
                "status": row["observed_status"],
                "exclusion_reasons": row["exclusion_reasons"],
            }
            for row in excluded
        ],
        "blocking_gates": [
            {
                "gate_id": row["gate_id"],
                "status": row["observed_status"],
                "blocking_domain": row["blocking_domain"],
                "artifact_path": row["artifact_path"],
                "reason_codes": row["reason_codes"],
            }
            for row in blocking
        ],
        "status": status,
        "blocking_class": blocking_class,
        "reason_codes": reason_codes,
        "evidence_refs": sorted(set(evidence_refs)),
        "decision_ledger_ref": _verdict_artifact_ref(truth_root, "gate_decision_ledger_v1", day_utc, "gate_decision_ledger.v1.json"),
    }
    validate_against_repo_schema_v1(doc, repo_root, ECONOMIC_VERDICT_SCHEMA)
    return doc


def build_gate_decision_ledger_doc(
    repo_root: Path,
    truth_root: Path,
    day_utc: str,
    produced_utc: str,
    lifecycle_doc: Dict[str, Any],
    gate_rows: List[Dict[str, Any]],
    authorization_doc: Dict[str, Any],
    economic_doc: Dict[str, Any],
) -> Dict[str, Any]:
    gate_stack_path = (truth_root / "reports" / "gate_stack_verdict_v1" / day_utc / "gate_stack_verdict.v1.json").resolve()
    evidence_refs = [str(gate_stack_path)] if gate_stack_path.exists() else []
    evidence_refs.append(_verdict_artifact_ref(truth_root, "lifecycle_state_authority_v1", day_utc, "lifecycle_state_authority.v1.json"))
    evidence_refs.extend([row["artifact_path"] for row in gate_rows if row["artifact_path"]])

    doc = {
        "schema_id": "gate_decision_ledger_v1",
        "schema_version": 1,
        "lifecycle_state_authority_ref": _verdict_artifact_ref(
            truth_root, "lifecycle_state_authority_v1", day_utc, "lifecycle_state_authority.v1.json"
        ),
        "gate_classification_registry_id": GATE_CLASSIFICATION_REGISTRY_ID,
        "gate_classification_registry_version": GATE_CLASSIFICATION_REGISTRY_VERSION,
        "authorization_verdict_ref": _verdict_artifact_ref(
            truth_root, "authorization_gate_verdict_v1", day_utc, "authorization_gate_verdict.v1.json"
        ),
        "economic_health_verdict_ref": _verdict_artifact_ref(
            truth_root, "economic_health_gate_verdict_v1", day_utc, "economic_health_gate_verdict.v1.json"
        ),
        "day_utc": day_utc,
        "produced_utc": produced_utc,
        "gate_evaluation_records": [
            {
                "gate_id": row["gate_id"],
                "observed_status": row["observed_status"],
                "legacy_gate_class": row["legacy_gate_class"],
                "blocking_domain": row["blocking_domain"],
                "action_domain": row["action_domain"],
                "artifact_path": row["artifact_path"],
                "reason_codes": row["reason_codes"],
            }
            for row in gate_rows
        ],
        "inclusion_exclusion_decisions": [
            {
                "gate_id": row["gate_id"],
                "included_in_authorization_verdict": row["included_in_authorization_verdict"],
                "included_in_economic_health_verdict": row["included_in_economic_health_verdict"],
                "exclusion_reasons": row["exclusion_reasons"],
            }
            for row in gate_rows
        ],
        "blocking_domain_decisions": [
            {
                "domain": "AUTHORIZATION",
                "status": authorization_doc["status"],
                "blocking_class": authorization_doc["blocking_class"],
                "reason_codes": authorization_doc["reason_codes"],
            },
            {
                "domain": "ECONOMIC_HEALTH",
                "status": economic_doc["status"],
                "blocking_class": economic_doc["blocking_class"],
                "reason_codes": economic_doc["reason_codes"],
            },
        ],
        "evidence_refs": sorted(set(evidence_refs)),
    }
    validate_against_repo_schema_v1(doc, repo_root, LEDGER_SCHEMA)
    return doc


def build_gate_authority_docs(
    repo_root: Path,
    truth_root: Path,
    day_utc: str,
    produced_utc: str,
    mode: str,
) -> Dict[str, Dict[str, Any]]:
    facts = collect_gate_authority_facts(repo_root, truth_root, day_utc)
    lifecycle_doc = derive_lifecycle_state_authority_doc(
        repo_root=repo_root,
        truth_root=truth_root,
        day_utc=day_utc,
        produced_utc=produced_utc,
        mode=mode,
        facts=facts,
    )
    gate_rows = _build_gate_evaluation_rows(repo_root, lifecycle_doc, facts)
    authorization_doc = build_authorization_gate_verdict_doc(
        repo_root=repo_root,
        truth_root=truth_root,
        day_utc=day_utc,
        produced_utc=produced_utc,
        mode=mode,
        lifecycle_doc=lifecycle_doc,
        facts=facts,
        gate_rows=gate_rows,
    )
    economic_doc = build_economic_health_gate_verdict_doc(
        repo_root=repo_root,
        truth_root=truth_root,
        day_utc=day_utc,
        produced_utc=produced_utc,
        mode=mode,
        lifecycle_doc=lifecycle_doc,
        gate_rows=gate_rows,
    )
    ledger_doc = build_gate_decision_ledger_doc(
        repo_root=repo_root,
        truth_root=truth_root,
        day_utc=day_utc,
        produced_utc=produced_utc,
        lifecycle_doc=lifecycle_doc,
        gate_rows=gate_rows,
        authorization_doc=authorization_doc,
        economic_doc=economic_doc,
    )
    return {
        "lifecycle_state": lifecycle_doc,
        "authorization_verdict": authorization_doc,
        "economic_verdict": economic_doc,
        "ledger": ledger_doc,
    }


def _internal_failure_docs(
    repo_root: Path,
    truth_root: Path,
    day_utc: str,
    produced_utc: str,
    mode: str,
    exc: Exception,
) -> Dict[str, Dict[str, Any]]:
    run_scope = _scope_from_truth_root(truth_root, day_utc, mode)
    error_text = f"{type(exc).__name__}: {exc}"
    tb = traceback.format_exc(limit=5)
    lifecycle_doc = {
        "schema_id": "lifecycle_state_authority_v1",
        "schema_version": 1,
        "lifecycle_policy_id": LIFECYCLE_POLICY_ID,
        "lifecycle_policy_version": LIFECYCLE_POLICY_VERSION,
        "day_utc": day_utc,
        "mode": mode,
        "run_scope": run_scope,
        "produced_utc": produced_utc,
        "lifecycle_state": "PRE_TRADE",
        "derivation_inputs": [],
        "derivation_reasons": ["INTERNAL_FAILURE"],
        "evidence_refs": [tb],
    }
    authorization_doc = {
        'schema_id': 'authorization_gate_verdict_v1',
        'schema_version': 1,
        'gate_classification_registry_id': GATE_CLASSIFICATION_REGISTRY_ID,
        'gate_classification_registry_version': GATE_CLASSIFICATION_REGISTRY_VERSION,
        'lifecycle_state_authority_ref': _verdict_artifact_ref(
            truth_root, 'lifecycle_state_authority_v1', day_utc, 'lifecycle_state_authority.v1.json'
        ),
        'day_utc': day_utc,
        'mode': mode,
        'produced_utc': produced_utc,
        'included_gates': [],
        'excluded_gates': [],
        'blocking_gates': [],
        'status': 'FAIL',
        'blocking_class': 'CLASS1_SYSTEM_HARD_STOP',
        'reason_codes': [f'INTERNAL_FAILURE:{error_text}'],
        'evidence_refs': [tb],
        'decision_ledger_ref': _verdict_artifact_ref(truth_root, 'gate_decision_ledger_v1', day_utc, 'gate_decision_ledger.v1.json'),
    }
    economic_doc = {
        "schema_id": "economic_health_gate_verdict_v1",
        "schema_version": 1,
        "gate_classification_registry_id": GATE_CLASSIFICATION_REGISTRY_ID,
        "gate_classification_registry_version": GATE_CLASSIFICATION_REGISTRY_VERSION,
        "lifecycle_state_authority_ref": _verdict_artifact_ref(
            truth_root, "lifecycle_state_authority_v1", day_utc, "lifecycle_state_authority.v1.json"
        ),
        "day_utc": day_utc,
        "mode": mode,
        "produced_utc": produced_utc,
        "included_gates": [],
        "excluded_gates": [],
        "blocking_gates": [],
        "status": "FAIL",
        "blocking_class": "CLASS1_SYSTEM_HARD_STOP",
        "reason_codes": [f"INTERNAL_FAILURE:{error_text}"],
        "evidence_refs": [tb],
        "decision_ledger_ref": _verdict_artifact_ref(truth_root, "gate_decision_ledger_v1", day_utc, "gate_decision_ledger.v1.json"),
    }
    ledger_doc = {
        "schema_id": "gate_decision_ledger_v1",
        "schema_version": 1,
        "lifecycle_state_authority_ref": _verdict_artifact_ref(
            truth_root, "lifecycle_state_authority_v1", day_utc, "lifecycle_state_authority.v1.json"
        ),
        "gate_classification_registry_id": GATE_CLASSIFICATION_REGISTRY_ID,
        "gate_classification_registry_version": GATE_CLASSIFICATION_REGISTRY_VERSION,
        "authorization_verdict_ref": _verdict_artifact_ref(
            truth_root, "authorization_gate_verdict_v1", day_utc, "authorization_gate_verdict.v1.json"
        ),
        "economic_health_verdict_ref": _verdict_artifact_ref(
            truth_root, "economic_health_gate_verdict_v1", day_utc, "economic_health_gate_verdict.v1.json"
        ),
        "day_utc": day_utc,
        "produced_utc": produced_utc,
        "gate_evaluation_records": [],
        "inclusion_exclusion_decisions": [],
        "blocking_domain_decisions": [
            {"domain": "AUTHORIZATION", "status": "FAIL", "blocking_class": "CLASS1_SYSTEM_HARD_STOP", "reason_codes": [error_text]},
            {"domain": "ECONOMIC_HEALTH", "status": "FAIL", "blocking_class": "CLASS1_SYSTEM_HARD_STOP", "reason_codes": [error_text]},
        ],
        "evidence_refs": [tb],
    }
    validate_against_repo_schema_v1(lifecycle_doc, repo_root, LIFECYCLE_STATE_SCHEMA)
    validate_against_repo_schema_v1(authorization_doc, repo_root, AUTHORIZATION_VERDICT_SCHEMA)
    validate_against_repo_schema_v1(economic_doc, repo_root, ECONOMIC_VERDICT_SCHEMA)
    validate_against_repo_schema_v1(ledger_doc, repo_root, LEDGER_SCHEMA)
    return {
        "lifecycle_state": lifecycle_doc,
        "authorization_verdict": authorization_doc,
        "economic_verdict": economic_doc,
        "ledger": ledger_doc,
    }


def write_gate_authority_plane(
    *,
    repo_root: Path,
    truth_root: Path,
    day_utc: str,
    produced_utc: str,
    mode: str,
) -> Dict[str, RefreshWriteResultV1]:
    day_utc = _require_day_utc(day_utc)
    produced_utc = _require_produced_utc(day_utc, produced_utc)
    mode_text = str(mode or "").strip().upper()
    if mode_text not in {"PAPER", "LIVE"}:
        raise RuntimeError(f"INVALID_MODE:{mode}")
    try:
        docs = build_gate_authority_docs(
            repo_root=repo_root,
            truth_root=truth_root,
            day_utc=day_utc,
            produced_utc=produced_utc,
            mode=mode_text,
        )
    except Exception as exc:
        docs = _internal_failure_docs(
            repo_root=repo_root,
            truth_root=truth_root,
            day_utc=day_utc,
            produced_utc=produced_utc,
            mode=mode_text,
            exc=exc,
        )

    targets = {
        "lifecycle_state": (
            truth_root / "reports" / "lifecycle_state_authority_v1" / day_utc / "lifecycle_state_authority.v1.json"
        ).resolve(),
        "authorization_verdict": (
            truth_root / "reports" / "authorization_gate_verdict_v1" / day_utc / "authorization_gate_verdict.v1.json"
        ).resolve(),
        "economic_verdict": (
            truth_root / "reports" / "economic_health_gate_verdict_v1" / day_utc / "economic_health_gate_verdict.v1.json"
        ).resolve(),
        "ledger": (truth_root / "reports" / "gate_decision_ledger_v1" / day_utc / "gate_decision_ledger.v1.json").resolve(),
    }
    schemas = {
        "lifecycle_state": ("lifecycle_state_authority_v1", 1),
        "authorization_verdict": ("authorization_gate_verdict_v1", 1),
        "economic_verdict": ("economic_health_gate_verdict_v1", 1),
        "ledger": ("gate_decision_ledger_v1", 1),
    }
    writes: Dict[str, RefreshWriteResultV1] = {}
    for key in ("lifecycle_state", "authorization_verdict", "economic_verdict", "ledger"):
        schema_id, schema_version = schemas[key]
        payload = canonical_json_bytes_v1(docs[key]) + b"\n"
        writes[key] = write_day_artifact_refreshable_v1(
            path=targets[key],
            data=payload,
            expected_day_utc=day_utc,
            expected_schema_id=schema_id,
            expected_schema_version=schema_version,
            preserve_statuses=(),
        )
    return writes
