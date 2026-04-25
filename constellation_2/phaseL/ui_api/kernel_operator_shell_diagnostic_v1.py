from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable

from constellation_2.common.advisory.advisory_storage_v1 import (
    load_execution_intent_by_id_v1,
    load_household_snapshot_by_id_v1,
    load_latest_complete_investor_intent_by_household_v1,
    load_policies_by_parent_intent_v1,
    load_policy_by_id_v1,
    load_portfolio_intent_by_id_v1,
    load_promotion_decision_by_id_v1,
    load_promotion_record_by_id_v2,
)
from constellation_2.common.execution_kernel.execution_kernel_storage_v1 import (
    execution_kernel_root_for_truth_root,
)
from constellation_2.common.execution_kernel.execution_state_record_v1 import (
    load_latest_execution_state_record_v1,
)
from constellation_2.common.execution_kernel.execution_submission_record_v1 import (
    ExecutionSubmissionRecordV1,
)
from constellation_2.phaseL.ui_api.common import iso_from_mtime, read_json_dict, utc_now_iso
from constellation_2.phaseL.ui_api.kernel_operator_shell_control_plane_v1 import (
    load_runtime_control_workspace_bundle_v1,
)


def _now_utc() -> str:
    return utc_now_iso()


def _safe_str(value: Any) -> str:
    return str(value).strip() if value is not None else ""


def _parse_iso(value: Any) -> datetime | None:
    raw = _safe_str(value)
    if not raw:
        return None
    try:
        return datetime.fromisoformat(raw.replace("Z", "+00:00"))
    except Exception:
        return None


def _ts_sort_key(*values: Any) -> tuple[Any, ...]:
    parsed = [item for item in (_parse_iso(value) for value in values) if item is not None]
    if not parsed:
        return (datetime.min.replace(tzinfo=timezone.utc),)
    return tuple(parsed)


def _latest_loaded(
    paths: list[Path],
    *,
    key_fn: Callable[[dict[str, Any], Path], tuple[Any, ...]],
) -> tuple[Path | None, dict[str, Any] | None]:
    candidates: list[tuple[tuple[Any, ...], Path, dict[str, Any]]] = []
    for path in paths:
        doc, err = read_json_dict(path)
        if err is not None or doc is None:
            continue
        candidates.append((key_fn(doc, path), path, doc))
    if not candidates:
        return None, None
    candidates.sort(key=lambda item: item[0])
    _, path, doc = candidates[-1]
    return path, doc


def _list_json_files(root: Path, pattern: str) -> list[Path]:
    if not root.exists():
        return []
    return sorted(root.rglob(pattern))


def _latest_advisory_doc_for_household_v1(
    *,
    advisory_runtime_root: str,
    household_id: str,
    rel_dir: str,
    suffix: str,
    fields: tuple[str, ...],
) -> tuple[Path | None, dict[str, Any] | None]:
    root = Path(advisory_runtime_root).resolve() / rel_dir / "households" / household_id
    if not root.exists():
        return None, None
    return _latest_loaded(
        sorted(root.glob(f"*{suffix}")),
        key_fn=lambda doc, path: (*_ts_sort_key(*(doc.get(field) for field in fields)), str(path)),
    )


def load_current_household_id_v1(*, advisory_runtime_root: str) -> str | None:
    root = Path(advisory_runtime_root).resolve()
    candidates: list[tuple[tuple[Any, ...], str]] = []
    patterns = [
        ("authorities/household_snapshot_v1/households/*/*.household_snapshot.v1.json", ("effective_at", "created_at")),
        ("authorities/investor_intent_v1/households/*/*.investor_intent.v1.json", ("effective_at", "created_at")),
        ("artifacts/kernel_run_envelope_v1/households/*/*.kernel_run_envelope.v1.json", ("produced_utc",)),
    ]
    for pattern, fields in patterns:
        path, doc = _latest_loaded(
            sorted(root.rglob(pattern)),
            key_fn=lambda item, item_path, fields=fields: (*_ts_sort_key(*(item.get(field) for field in fields)), str(item_path)),
        )
        if path is None or doc is None:
            continue
        household_id = path.parent.name
        candidates.append(((*_ts_sort_key(*(doc.get(field) for field in fields)), str(path)), household_id))
    if not candidates:
        return None
    candidates.sort(key=lambda item: item[0])
    return candidates[-1][1]


def discover_runtime_scope_v1(*, truth_root: Path, sleeve_truth_root: Path) -> dict[str, str] | None:
    control_bundle = load_runtime_control_workspace_bundle_v1(truth_root=truth_root)
    record = control_bundle.get("record")
    if isinstance(record, dict):
        return {
            "day_utc": _safe_str(record.get("day_utc")),
            "environment": _safe_str(record.get("environment")),
            "ib_account": _safe_str(record.get("ib_account")),
            "sleeve_id": _safe_str(record.get("sleeve_id")) or "PRIMARY",
            "capability_scope": _safe_str(record.get("capability_scope")) or "paper_trade_submit_entry_v1",
            "expected_runtime_control_record_id": _safe_str(record.get("runtime_control_record_id")),
        }

    readiness_paths = _list_json_files(sleeve_truth_root / "trade_submit_readiness_c2_v1", "status.json")
    if not readiness_paths:
        return None
    readiness_path, readiness = _latest_loaded(
        readiness_paths,
        key_fn=lambda doc, path: (*_ts_sort_key(doc.get("as_of_utc"), iso_from_mtime(path)), str(path)),
    )
    if readiness_path is None or readiness is None:
        return None
    return {
        "day_utc": _safe_str(readiness.get("day_utc")) or _now_utc()[:10],
        "environment": _safe_str(readiness.get("environment")),
        "ib_account": _safe_str(readiness.get("ib_account")),
        "sleeve_id": "PRIMARY",
        "capability_scope": "paper_trade_submit_entry_v1",
        "expected_runtime_control_record_id": "",
    }


def load_state_workspace_snapshot_bundle_v1(*, advisory_runtime_root: str, household_id: str) -> dict[str, Any]:
    snapshot_path, snapshot_doc = _latest_advisory_doc_for_household_v1(
        advisory_runtime_root=advisory_runtime_root,
        household_id=household_id,
        rel_dir="authorities/household_snapshot_v1",
        suffix=".household_snapshot.v1.json",
        fields=("effective_at", "created_at"),
    )
    decision_path, decision_doc = _latest_advisory_doc_for_household_v1(
        advisory_runtime_root=advisory_runtime_root,
        household_id=household_id,
        rel_dir="artifacts/snapshot_validation_decision_v1",
        suffix=".snapshot_validation_decision.v1.json",
        fields=("effective_at", "produced_utc"),
    )
    envelope_path, envelope_doc = _latest_advisory_doc_for_household_v1(
        advisory_runtime_root=advisory_runtime_root,
        household_id=household_id,
        rel_dir="artifacts/snapshot_run_envelope_v1",
        suffix=".snapshot_run_envelope.v1.json",
        fields=("effective_at", "produced_utc"),
    )
    return {
        "snapshot_path": snapshot_path,
        "snapshot_doc": snapshot_doc,
        "decision_path": decision_path,
        "decision_doc": decision_doc,
        "envelope_path": envelope_path,
        "envelope_doc": envelope_doc,
    }


def load_advisory_workspace_bundle_v1(*, advisory_runtime_root: str, household_id: str) -> dict[str, Any]:
    envelope_path, envelope_doc = _latest_advisory_doc_for_household_v1(
        advisory_runtime_root=advisory_runtime_root,
        household_id=household_id,
        rel_dir="artifacts/kernel_run_envelope_v1",
        suffix=".kernel_run_envelope.v1.json",
        fields=("day_utc", "produced_utc"),
    )
    investor_intent = load_latest_complete_investor_intent_by_household_v1(advisory_runtime_root, household_id)
    policy = None
    snapshot = None
    portfolio_intent = None
    promotion_decision = None
    promotion_record = None
    execution_intent = None

    if envelope_doc is not None:
        refs = envelope_doc.get("artifact_refs") if isinstance(envelope_doc.get("artifact_refs"), dict) else {}
        if _safe_str(refs.get("policy_id")):
            try:
                policy = load_policy_by_id_v1(advisory_runtime_root, household_id, _safe_str(refs.get("policy_id")))
            except Exception:
                policy = None
        if _safe_str(refs.get("household_snapshot_id")):
            try:
                snapshot = load_household_snapshot_by_id_v1(advisory_runtime_root, household_id, _safe_str(refs.get("household_snapshot_id")))
            except Exception:
                snapshot = None
        if _safe_str(refs.get("portfolio_intent_id")):
            try:
                portfolio_intent = load_portfolio_intent_by_id_v1(advisory_runtime_root, household_id, _safe_str(refs.get("portfolio_intent_id")))
            except Exception:
                portfolio_intent = None
        if _safe_str(refs.get("promotion_decision_id")):
            try:
                promotion_decision = load_promotion_decision_by_id_v1(advisory_runtime_root, household_id, _safe_str(refs.get("promotion_decision_id")))
            except Exception:
                promotion_decision = None
        if _safe_str(refs.get("promotion_record_id")):
            try:
                promotion_record = load_promotion_record_by_id_v2(advisory_runtime_root, household_id, _safe_str(refs.get("promotion_record_id")))
            except Exception:
                promotion_record = None
        if _safe_str(refs.get("execution_intent_id")):
            try:
                execution_intent = load_execution_intent_by_id_v1(advisory_runtime_root, household_id, _safe_str(refs.get("execution_intent_id")))
            except Exception:
                execution_intent = None

    if investor_intent is not None and policy is None:
        policies = load_policies_by_parent_intent_v1(advisory_runtime_root, household_id, investor_intent.intent_id)
        policy = policies[-1] if policies else None

    return {
        "envelope_path": envelope_path,
        "envelope_doc": envelope_doc,
        "investor_intent": investor_intent,
        "policy": policy,
        "snapshot": snapshot,
        "portfolio_intent": portfolio_intent,
        "promotion_decision": promotion_decision,
        "promotion_record": promotion_record,
        "execution_intent": execution_intent,
    }


def _latest_submission_record_v1(*, sleeve_truth_root: Path) -> tuple[Path | None, ExecutionSubmissionRecordV1 | None]:
    root = execution_kernel_root_for_truth_root(truth_root=sleeve_truth_root)
    paths = _list_json_files(root / "submission_records", "submission_record.v1.json")
    if not paths:
        return None, None
    candidates: list[tuple[tuple[Any, ...], Path, ExecutionSubmissionRecordV1]] = []
    for path in paths:
        try:
            record = ExecutionSubmissionRecordV1.load_file(path)
        except Exception:
            continue
        candidates.append(((*_ts_sort_key(record.day_utc, record.produced_utc), str(path)), path, record))
    if not candidates:
        return None, None
    candidates.sort(key=lambda item: item[0])
    _, path, record = candidates[-1]
    return path, record


def _latest_execution_submission_decision_for_intent_v1(
    *,
    sleeve_truth_root: Path,
    execution_intent_id: str,
    submission_id: str | None = None,
) -> tuple[Path | None, dict[str, Any] | None]:
    root = execution_kernel_root_for_truth_root(truth_root=sleeve_truth_root) / "submission_decisions"
    paths = _list_json_files(root, "*.execution_submission_decision.v1.json")
    matches = []
    for path in paths:
        doc, err = read_json_dict(path)
        if err is not None or doc is None:
            continue
        if _safe_str(doc.get("execution_intent_id")) != execution_intent_id:
            continue
        if submission_id and _safe_str(doc.get("predicted_submission_id")) != submission_id:
            continue
        matches.append(path)
    return _latest_loaded(matches, key_fn=lambda doc, path: (*_ts_sort_key(doc.get("day_utc"), doc.get("produced_utc")), str(path)))


def _latest_execution_run_envelope_for_submission_v1(
    *,
    sleeve_truth_root: Path,
    submission_id: str | None,
) -> tuple[Path | None, dict[str, Any] | None]:
    root = execution_kernel_root_for_truth_root(truth_root=sleeve_truth_root) / "run_envelopes"
    paths = _list_json_files(root, "*.execution_run_envelope.v1.json")
    matches = []
    for path in paths:
        doc, err = read_json_dict(path)
        if err is not None or doc is None:
            continue
        refs = doc.get("artifact_refs") if isinstance(doc.get("artifact_refs"), dict) else {}
        if submission_id and _safe_str(refs.get("submission_id")) != submission_id:
            continue
        matches.append(path)
    return _latest_loaded(matches, key_fn=lambda doc, path: (*_ts_sort_key(doc.get("day_utc"), doc.get("produced_utc")), str(path)))


def load_submission_workspace_bundle_v1(*, advisory_runtime_root: str, sleeve_truth_root: Path) -> dict[str, Any]:
    submission_record_path, submission_record = _latest_submission_record_v1(sleeve_truth_root=sleeve_truth_root)
    if submission_record is None or submission_record_path is None:
        return {
            "submission_record_path": None,
            "submission_record": None,
            "execution_intent": None,
            "promotion_record": None,
            "decision_path": None,
            "decision_doc": None,
            "envelope_path": None,
            "envelope_doc": None,
            "current_state": None,
            "lineage_error": "SUBMISSION_RECORD_MISSING",
        }

    try:
        execution_intent = load_execution_intent_by_id_v1(
            advisory_runtime_root,
            submission_record.household_id,
            submission_record.execution_intent_id,
        )
        promotion_record = load_promotion_record_by_id_v2(
            advisory_runtime_root,
            submission_record.household_id,
            execution_intent.promotion_record_id,
        )
    except Exception:
        return {
            "submission_record_path": submission_record_path,
            "submission_record": submission_record,
            "execution_intent": None,
            "promotion_record": None,
            "decision_path": None,
            "decision_doc": None,
            "envelope_path": None,
            "envelope_doc": None,
            "current_state": None,
            "lineage_error": "SUBMISSION_CHAIN_LINEAGE_UNREADABLE",
        }

    decision_path, decision_doc = _latest_execution_submission_decision_for_intent_v1(
        sleeve_truth_root=sleeve_truth_root,
        execution_intent_id=execution_intent.execution_intent_id,
        submission_id=submission_record.submission_id,
    )
    envelope_path, envelope_doc = _latest_execution_run_envelope_for_submission_v1(
        sleeve_truth_root=sleeve_truth_root,
        submission_id=submission_record.submission_id,
    )
    current_state = load_latest_execution_state_record_v1(
        truth_root=sleeve_truth_root,
        day_utc=submission_record.day_utc,
        submission_id=submission_record.submission_id,
    )
    return {
        "submission_record_path": submission_record_path,
        "submission_record": submission_record,
        "execution_intent": execution_intent,
        "promotion_record": promotion_record,
        "decision_path": decision_path,
        "decision_doc": decision_doc,
        "envelope_path": envelope_path,
        "envelope_doc": envelope_doc,
        "current_state": current_state,
        "lineage_error": "",
    }


def load_current_execution_intent_for_submit_v1(
    *,
    advisory_runtime_root: str,
    sleeve_truth_root: Path,
) -> tuple[Any | None, ExecutionSubmissionRecordV1 | None]:
    submission_bundle = load_submission_workspace_bundle_v1(
        advisory_runtime_root=advisory_runtime_root,
        sleeve_truth_root=sleeve_truth_root,
    )
    return submission_bundle.get("execution_intent"), submission_bundle.get("submission_record")


def _latest_execution_state_record_v1(*, sleeve_truth_root: Path) -> tuple[Path | None, dict[str, Any] | None]:
    root = execution_kernel_root_for_truth_root(truth_root=sleeve_truth_root)
    return _latest_loaded(
        _list_json_files(root / "execution_state_records", "*.execution_state_record.v1.json"),
        key_fn=lambda doc, path: (
            *_ts_sort_key(doc.get("day_utc"), doc.get("produced_utc")),
            int(doc.get("transition_index") or 0),
            str(path),
        ),
    )


def _latest_lifecycle_decision_for_submission_v1(
    *,
    sleeve_truth_root: Path,
    submission_id: str | None,
) -> tuple[Path | None, dict[str, Any] | None]:
    root = execution_kernel_root_for_truth_root(truth_root=sleeve_truth_root) / "lifecycle_decisions"
    paths = _list_json_files(root, "*.execution_lifecycle_decision.v1.json")
    matches = []
    for path in paths:
        doc, err = read_json_dict(path)
        if err is not None or doc is None:
            continue
        if submission_id and _safe_str(doc.get("submission_id")) != submission_id:
            continue
        matches.append(path)
    return _latest_loaded(matches, key_fn=lambda doc, path: (*_ts_sort_key(doc.get("day_utc"), doc.get("produced_utc")), str(path)))


def _latest_lifecycle_envelope_for_submission_v1(
    *,
    sleeve_truth_root: Path,
    submission_id: str | None,
) -> tuple[Path | None, dict[str, Any] | None]:
    root = execution_kernel_root_for_truth_root(truth_root=sleeve_truth_root) / "lifecycle_run_envelopes"
    paths = _list_json_files(root, "*.execution_lifecycle_run_envelope.v1.json")
    matches = []
    for path in paths:
        doc, err = read_json_dict(path)
        if err is not None or doc is None:
            continue
        refs = doc.get("artifact_refs") if isinstance(doc.get("artifact_refs"), dict) else {}
        if submission_id and _safe_str(refs.get("submission_id")) != submission_id:
            continue
        matches.append(path)
    return _latest_loaded(matches, key_fn=lambda doc, path: (*_ts_sort_key(doc.get("day_utc"), doc.get("produced_utc")), str(path)))


def load_lifecycle_workspace_bundle_v1(*, sleeve_truth_root: Path) -> dict[str, Any]:
    record_path, record_doc = _latest_execution_state_record_v1(sleeve_truth_root=sleeve_truth_root)
    if record_path is None or record_doc is None:
        return {
            "record_path": None,
            "record_doc": None,
            "submission_record": None,
            "decision_path": None,
            "decision_doc": None,
            "envelope_path": None,
            "envelope_doc": None,
            "lineage_error": "EXECUTION_STATE_RECORD_MISSING",
        }

    submission_id = _safe_str(record_doc.get("submission_id"))
    try:
        submission_record = ExecutionSubmissionRecordV1.load_file(
            execution_submission_record_path_v1(
                truth_root=sleeve_truth_root,
                day_utc=_safe_str(record_doc.get("day_utc")),
                submission_id=submission_id,
            )
        )
    except Exception:
        return {
            "record_path": record_path,
            "record_doc": record_doc,
            "submission_record": None,
            "decision_path": None,
            "decision_doc": None,
            "envelope_path": None,
            "envelope_doc": None,
            "lineage_error": "SUBMISSION_RECORD_FOR_LIFECYCLE_UNREADABLE",
        }

    decision_path, decision_doc = _latest_lifecycle_decision_for_submission_v1(
        sleeve_truth_root=sleeve_truth_root,
        submission_id=submission_id,
    )
    envelope_path, envelope_doc = _latest_lifecycle_envelope_for_submission_v1(
        sleeve_truth_root=sleeve_truth_root,
        submission_id=submission_id,
    )
    return {
        "record_path": record_path,
        "record_doc": record_doc,
        "submission_record": submission_record,
        "decision_path": decision_path,
        "decision_doc": decision_doc,
        "envelope_path": envelope_path,
        "envelope_doc": envelope_doc,
        "lineage_error": "",
    }
