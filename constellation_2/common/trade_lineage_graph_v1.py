from __future__ import annotations

import hashlib
import json
from collections import defaultdict
from datetime import UTC, datetime
from pathlib import Path
from typing import Any


SCHEMA_ID = "C2_TRADE_LINEAGE_GRAPH_V1"
SCHEMA_VERSION = 1

STATES = {
    "NO_INTENTS",
    "INTENT_ONLY",
    "RELEASED",
    "AUTHORIZED",
    "BUILT",
    "PACKAGED",
    "SUBMIT_DECIDED",
    "SUBMIT_RECORDED",
    "BROKER_IDENTIFIED",
    "FILLED",
    "RECONCILED",
    "DRY_RUN_COMPLETE",
    "LINEAGE_GAP",
    "IDENTITY_CONFLICT",
    "DUPLICATE_IDENTITY",
}


def _utc_now_iso() -> str:
    return datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _read_json(path: Path) -> dict[str, Any] | None:
    if not path.exists() or not path.is_file():
        return None
    try:
        obj = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return None
    return obj if isinstance(obj, dict) else None


def _canonical_bytes(payload: dict[str, Any]) -> bytes:
    return json.dumps(payload, sort_keys=True, separators=(",", ":"), ensure_ascii=True).encode("utf-8")


def _sha256_payload(payload: dict[str, Any]) -> str:
    return hashlib.sha256(_canonical_bytes(payload)).hexdigest()


def _coerce_positive_int(value: Any) -> int | None:
    if isinstance(value, bool):
        return None
    if isinstance(value, int) and value > 0:
        return value
    text = str(value or "").strip()
    if not text:
        return None
    try:
        parsed = int(text)
    except ValueError:
        return None
    return parsed if parsed > 0 else None


def _first_nonempty(*values: Any) -> str:
    for value in values:
        text = str(value or "").strip()
        if text:
            return text
    return ""


def _path_exists(path: Path | None) -> bool:
    return bool(path is not None and path.exists())


def _path_text(path: Path | None) -> str:
    return str(path.resolve()) if path is not None else ""


def _sha_from_intent_path(path: Path) -> str:
    name = path.name
    for suffix in (".exposure_intent.v1.json", ".exposure_intent.v2.json", ".json"):
        if name.endswith(suffix):
            return name[: -len(suffix)]
    return path.stem


def _json_files(root: Path, pattern: str) -> list[Path]:
    if not root.exists() or not root.is_dir():
        return []
    return sorted(path.resolve() for path in root.rglob(pattern) if path.is_file())


def _identity_from_path(path_text: str, marker: str) -> str:
    text = str(path_text or "")
    marker_text = f"/{marker}/"
    if marker_text not in text:
        return ""
    tail = text.split(marker_text, 1)[1].split("/")
    if len(tail) >= 2:
        return tail[1]
    return ""


def _phasec_intent_hash_from_path(path_text: str) -> str:
    text = str(path_text or "")
    if "/phaseC_preflight_v1/" not in text:
        return ""
    parts = text.split("/phaseC_preflight_v1/", 1)[1].split("/")
    if len(parts) >= 3 and parts[1].startswith("attempt_"):
        return parts[2]
    return ""


def _artifact_refs(payload: dict[str, Any] | None) -> list[dict[str, Any]]:
    if not isinstance(payload, dict):
        return []
    refs = payload.get("evidence_refs") if isinstance(payload.get("evidence_refs"), list) else []
    out: list[dict[str, Any]] = []
    for ref in refs:
        if isinstance(ref, dict):
            out.append(ref)
    return out


def _evidence_paths(payload: dict[str, Any] | None) -> list[str]:
    if not isinstance(payload, dict):
        return []
    values = payload.get("evidence_artifacts") if isinstance(payload.get("evidence_artifacts"), list) else []
    return [str(item).strip() for item in values if str(item).strip()]


def _broker_ids(payload: dict[str, Any] | None) -> tuple[int | None, int | None]:
    ids = payload.get("broker_ids") if isinstance(payload, dict) and isinstance(payload.get("broker_ids"), dict) else {}
    return _coerce_positive_int(ids.get("order_id")), _coerce_positive_int(ids.get("perm_id"))


def _dry_run(submit_attempt: dict[str, Any] | None, broker_record: dict[str, Any] | None) -> bool:
    if isinstance(submit_attempt, dict) and submit_attempt.get("dry_run") is True:
        return True
    error = broker_record.get("error") if isinstance(broker_record, dict) and isinstance(broker_record.get("error"), dict) else {}
    return str(error.get("code") or "").strip().upper() == "DRY_RUN_NO_BROKER_ID"


def _reconciled(payload: dict[str, Any] | None) -> bool:
    if not isinstance(payload, dict):
        return False
    status = str(payload.get("status") or "").strip().upper()
    semantic = str(payload.get("semantic_status") or "").strip().upper()
    return status in {"PASS", "OK"} and semantic not in {"MATERIALIZED_FAILURE", "BLOCKED_BY_UPSTREAM_PREREQUISITE"}


def _node(node_id: str, node_type: str, path: Path | None, identity: dict[str, Any], status: str = "PRESENT") -> dict[str, Any]:
    return {
        "node_id": node_id,
        "node_type": node_type,
        "artifact_path": _path_text(path),
        "status": status if _path_exists(path) else "MISSING",
        "identity": identity,
    }


def _edge(
    source: str,
    target: str,
    *,
    source_path: Path | None,
    target_path: Path | None,
    expected: dict[str, Any],
    observed: dict[str, Any],
    blocker_code: str = "",
    operator_actionable: bool = True,
) -> dict[str, Any]:
    ok = not blocker_code and bool(_path_exists(target_path))
    return {
        "source_node_id": source,
        "target_node_id": target,
        "source_artifact_path": _path_text(source_path),
        "target_artifact_path_expected": _path_text(target_path),
        "identity_key_expected": expected,
        "identity_key_observed": observed,
        "status": "PASS" if ok else "BROKEN",
        "blocker_code": blocker_code,
        "operator_actionable": bool(operator_actionable),
    }


def _broken_edge(
    source: str,
    target: str,
    *,
    source_path: Path | None,
    target_path: Path | None,
    expected: dict[str, Any],
    observed: dict[str, Any],
    blocker_code: str,
    operator_actionable: bool = True,
) -> dict[str, Any]:
    return _edge(
        source,
        target,
        source_path=source_path,
        target_path=target_path,
        expected=expected,
        observed=observed,
        blocker_code=blocker_code,
        operator_actionable=operator_actionable,
    )


def _discover_intents(*, roots: list[Path], day_utc: str) -> dict[str, dict[str, Any]]:
    out: dict[str, dict[str, Any]] = {}
    for root in roots:
        day_root = root / "intents_v1" / "snapshots" / day_utc
        for path in _json_files(day_root, "*.json"):
            obj = _read_json(path) or {}
            intent_hash = _first_nonempty(obj.get("intent_hash"), obj.get("canonical_json_hash"), _sha_from_intent_path(path))
            if not intent_hash:
                continue
            row = out.setdefault(intent_hash, {"intent_hash": intent_hash, "paths": []})
            row["paths"].append(str(path))
            if not row.get("intent_id"):
                row["intent_id"] = str(obj.get("intent_id") or "").strip()
            row["payload"] = obj
    return out


def _index_by_submission(paths: list[Path]) -> dict[str, tuple[Path, dict[str, Any]]]:
    out: dict[str, tuple[Path, dict[str, Any]]] = {}
    for path in paths:
        obj = _read_json(path) or {}
        sid = _first_nonempty(obj.get("submission_id"), path.parent.name)
        if sid and sid not in out:
            out[sid] = (path, obj)
    return out


def _index_submit_traces(paths: list[Path]) -> list[tuple[Path, dict[str, Any]]]:
    out: list[tuple[Path, dict[str, Any]]] = []
    for path in paths:
        obj = _read_json(path)
        if isinstance(obj, dict):
            out.append((path, obj))
    return out


def _intent_from_submit_trace(trace: dict[str, Any]) -> tuple[str, str, str]:
    intent_id = str(trace.get("intent_id") or "").strip()
    intent_hash = ""
    submission_id = ""
    for ref in _artifact_refs(trace):
        artifact_path = str(ref.get("artifact_path") or "").strip()
        artifact_type = str(ref.get("artifact_type") or "").strip()
        if artifact_type.startswith("exposure_intent"):
            intent_hash = str(ref.get("artifact_sha256") or "").strip() or _sha_from_intent_path(Path(artifact_path))
        if "broker_submission_record" in artifact_type:
            submission_id = _identity_from_path(artifact_path, "submissions")
    return intent_hash, intent_id, submission_id


def _intent_from_submit_attempt(submit_attempt: dict[str, Any] | None) -> tuple[str, str]:
    intent_hash = ""
    package_path = ""
    for path_text in _evidence_paths(submit_attempt):
        if "/execution_package_v1/" in path_text:
            package_path = path_text
        phasec_hash = _phasec_intent_hash_from_path(path_text)
        if phasec_hash:
            intent_hash = phasec_hash
    return intent_hash, package_path


def _resolve_lineage_identity(
    *,
    submission_id: str,
    package_obj: dict[str, Any] | None,
    build_obj: dict[str, Any] | None,
    submit_attempt_obj: dict[str, Any] | None,
    submit_traces: list[tuple[Path, dict[str, Any]]],
) -> tuple[str, str, list[dict[str, Any]]]:
    observations: list[dict[str, Any]] = []
    if isinstance(package_obj, dict):
        observations.append(
            {
                "source": "execution_package_v1",
                "intent_hash": str(package_obj.get("intent_hash") or "").strip(),
                "intent_id": str(package_obj.get("intent_id") or "").strip(),
                "submission_id": str(package_obj.get("submission_id") or "").strip(),
            }
        )
    if isinstance(build_obj, dict):
        observations.append(
            {
                "source": "execution_build_v1",
                "intent_hash": str(build_obj.get("intent_hash") or "").strip(),
                "intent_id": str(build_obj.get("intent_id") or "").strip(),
                "submission_id": str(build_obj.get("submission_id") or "").strip(),
            }
        )
    attempt_hash, _package_path = _intent_from_submit_attempt(submit_attempt_obj)
    if attempt_hash:
        observations.append(
            {
                "source": "broker_submit_attempt_v1",
                "intent_hash": attempt_hash,
                "intent_id": "",
                "submission_id": submission_id,
            }
        )
    for _path, trace in submit_traces:
        trace_hash, trace_intent_id, trace_submission_id = _intent_from_submit_trace(trace)
        if trace_submission_id == submission_id or (not trace_submission_id and trace_hash):
            observations.append(
                {
                    "source": "submit_decision_trace_v1",
                    "intent_hash": trace_hash,
                    "intent_id": trace_intent_id,
                    "submission_id": trace_submission_id or submission_id,
                }
            )
    intent_hash = _first_nonempty(*(row.get("intent_hash") for row in observations))
    intent_id = _first_nonempty(*(row.get("intent_id") for row in observations))
    return intent_hash, intent_id, observations


def _has_retry_marker(rows: list[dict[str, Any]]) -> bool:
    for row in rows:
        for key in ("retry_of_submission_id", "supersedes_submission_id", "supersession_id", "duplicate_classification"):
            text = str(row.get(key) or "").strip().upper()
            if text and text not in {"NEW", "NEW_INSTANCE", "NEW_INSTANCE_SAME_PLAN"}:
                return True
    return False


def trade_lineage_graph_output_path(*, truth_root: Path, day_utc: str) -> Path:
    return (
        Path(truth_root).resolve()
        / "reports"
        / "trade_lineage_graph_v1"
        / day_utc
        / "trade_lineage_graph.v1.json"
    ).resolve()


def evaluate_trade_lineage_graph_v1(
    *,
    day_utc: str,
    truth_root: Path,
    execution_root: Path | None = None,
    environment: str = "PAPER",
    produced_utc: str | None = None,
) -> dict[str, Any]:
    truth_root = Path(truth_root).resolve()
    execution_root = Path(execution_root).resolve() if execution_root is not None else truth_root
    roots = [execution_root, truth_root] if execution_root != truth_root else [truth_root]

    intents = _discover_intents(roots=roots, day_utc=day_utc)
    packages = _index_by_submission(
        _json_files(execution_root / "execution_package_v1" / day_utc, "execution_package.v1.json")
        + _json_files(truth_root / "execution_package_v1" / day_utc, "execution_package.v1.json")
    )
    builds = _index_by_submission(
        _json_files(truth_root / "reports" / "execution_build_v1" / day_utc, "execution_build.v1.json")
        + _json_files(execution_root / "reports" / "execution_build_v1" / day_utc, "execution_build.v1.json")
    )
    submit_traces = _index_submit_traces(_json_files(truth_root / "reports" / "submit_decision_trace_v1" / day_utc, "submit_decision_trace.v1.json"))
    submissions_root = execution_root / "execution_evidence_v1" / "submissions" / day_utc
    if not submissions_root.exists():
        submissions_root = truth_root / "execution_evidence_v1" / "submissions" / day_utc
    submission_dirs = sorted(path.resolve() for path in submissions_root.iterdir() if path.is_dir() and not path.name.startswith("_")) if submissions_root.exists() else []
    lifecycle_path = execution_root / "reports" / "execution_lifecycle_authority_v1" / day_utc / "execution_lifecycle_authority.v1.json"
    lifecycle_obj = _read_json(lifecycle_path) or {}
    lifecycle_by_submission = {
        str(row.get("submission_id") or "").strip(): row
        for row in lifecycle_obj.get("submissions", [])
        if isinstance(row, dict) and str(row.get("submission_id") or "").strip()
    }
    submission_index_path = execution_root / "submission_index_v1" / day_utc / "submission_index.v1.json"
    submission_index_obj = _read_json(submission_index_path) or {}
    current_head_path = execution_root / "execution_evidence_v1" / "current_head" / day_utc / "current_head.v1.json"
    current_head_obj = _read_json(current_head_path) or {}
    reconciliation_path = truth_root / "reports" / "execution_reconciliation_v1" / day_utc / "execution_reconciliation.v1.json"
    if not reconciliation_path.exists():
        reconciliation_path = execution_root / "reports" / "execution_reconciliation_v1" / day_utc / "execution_reconciliation.v1.json"
    reconciliation_obj = _read_json(reconciliation_path) or {}

    rows: list[dict[str, Any]] = []
    rows_by_submission: dict[str, dict[str, Any]] = {}
    broken_edges: list[dict[str, Any]] = []

    for submission_dir in submission_dirs:
        submission_id = submission_dir.name
        broker_path = submission_dir / "broker_submission_record.v2.json"
        broker_obj = _read_json(broker_path) or {}
        submit_attempt_path = submission_dir / "broker_submit_attempt_v1.json"
        submit_attempt_obj = _read_json(submit_attempt_path) or {}
        package_path, package_obj = packages.get(submission_id, (execution_root / "execution_package_v1" / day_utc / submission_id / "execution_package.v1.json", {}))
        build_path, build_obj = builds.get(submission_id, (truth_root / "reports" / "execution_build_v1" / day_utc / submission_id / "execution_build.v1.json", {}))
        fill_path = execution_root / "fill_ledger_v1" / day_utc / f"{submission_id}.fill_ledger.v1.json"
        if not fill_path.exists():
            fill_path = truth_root / "fill_ledger_v1" / day_utc / f"{submission_id}.fill_ledger.v1.json"
        fill_obj = _read_json(fill_path) or {}
        intent_hash, intent_id, identity_observations = _resolve_lineage_identity(
            submission_id=submission_id,
            package_obj=package_obj,
            build_obj=build_obj,
            submit_attempt_obj=submit_attempt_obj,
            submit_traces=submit_traces,
        )
        intent_path = Path(str((intents.get(intent_hash) or {}).get("paths", [""])[0])) if intent_hash and intents.get(intent_hash) else None
        auth_path = execution_root / "engine_activity_v1" / "authorization_v1" / day_utc / f"{intent_hash}.authorization.v1.json"
        if not auth_path.exists():
            auth_path = truth_root / "engine_activity_v1" / "authorization_v1" / day_utc / f"{intent_hash}.authorization.v1.json"
        phasec_path = None
        for path_text in _evidence_paths(submit_attempt_obj):
            if "/phaseC_preflight_v1/" in path_text:
                phasec_path = Path(path_text).resolve()
                break
        if phasec_path is None and intent_hash:
            candidates = _json_files(execution_root / "phaseC_preflight_v1" / day_utc, "submit_preflight_decision.v1.json")
            for candidate in candidates:
                if f"/{intent_hash}/" in str(candidate):
                    phasec_path = candidate
                    break
        matching_trace_path = None
        for trace_path, trace_obj in submit_traces:
            trace_hash, _trace_intent_id, trace_submission_id = _intent_from_submit_trace(trace_obj)
            if trace_submission_id == submission_id or (intent_hash and trace_hash == intent_hash):
                matching_trace_path = trace_path
                break

        order_id, perm_id = _broker_ids(broker_obj)
        dry_run = _dry_run(submit_attempt_obj, broker_obj)
        broker_transmit_enabled = bool(not dry_run and (order_id is not None or perm_id is not None or submit_attempt_obj.get("dry_run") is False))
        lifecycle_row = lifecycle_by_submission.get(submission_id) or {}
        lifecycle_state = str(lifecycle_row.get("current_lifecycle_state") or ("DRY_RUN_COMPLETE" if dry_run else "")).strip()

        nodes = [
            _node(f"intent:{intent_hash}", "intent", intent_path, {"intent_hash": intent_hash, "intent_id": intent_id}),
            _node(f"phasec:{submission_id}", "phaseC_candidate_release", phasec_path, {"intent_hash": intent_hash, "intent_id": intent_id}),
            _node(f"authorization:{intent_hash}", "engine_authorization", auth_path, {"intent_hash": intent_hash, "intent_id": intent_id}),
            _node(f"build:{submission_id}", "execution_build", build_path, {"submission_id": submission_id, "intent_id": intent_id}),
            _node(f"package:{submission_id}", "execution_package", package_path, {"submission_id": submission_id, "intent_hash": intent_hash, "intent_id": intent_id}),
            _node(f"submit_decision:{submission_id}", "submit_decision", matching_trace_path, {"submission_id": submission_id, "intent_hash": intent_hash, "intent_id": intent_id}),
            _node(f"submission:{submission_id}", "broker_submission", broker_path, {"submission_id": submission_id, "intent_hash": intent_hash, "intent_id": intent_id}),
            _node(f"lifecycle:{submission_id}", "execution_lifecycle_authority", lifecycle_path, {"submission_id": submission_id, "lifecycle_state": lifecycle_state}),
            _node(f"fill:{submission_id}", "fill_ledger", fill_path, {"submission_id": submission_id}),
            _node(f"reconciliation:{submission_id}", "execution_reconciliation", reconciliation_path, {"submission_id": submission_id}),
        ]
        edges: list[dict[str, Any]] = []
        expected_identity = {"intent_hash": intent_hash, "intent_id": intent_id, "submission_id": submission_id}

        def add_presence_edge(source: str, target: str, source_path: Path | None, target_path: Path | None, code: str) -> None:
            if _path_exists(target_path):
                edges.append(_edge(source, target, source_path=source_path, target_path=target_path, expected=expected_identity, observed=expected_identity))
            else:
                edges.append(_broken_edge(source, target, source_path=source_path, target_path=target_path, expected=expected_identity, observed={}, blocker_code=code))

        add_presence_edge(f"intent:{intent_hash}", f"phasec:{submission_id}", intent_path, phasec_path, "PHASEC_RELEASE_MISSING")
        add_presence_edge(f"phasec:{submission_id}", f"authorization:{intent_hash}", phasec_path, auth_path, "ENGINE_AUTHORIZATION_MISSING")
        add_presence_edge(f"authorization:{intent_hash}", f"build:{submission_id}", auth_path, build_path, "EXECUTION_BUILD_MISSING")
        if _path_exists(build_path) and not _path_exists(package_path):
            edges.append(
                _broken_edge(
                    f"build:{submission_id}",
                    f"package:{submission_id}",
                    source_path=build_path,
                    target_path=package_path,
                    expected=expected_identity,
                    observed={},
                    blocker_code="EXECUTION_PACKAGE_MISSING_AFTER_BUILD",
                )
            )
        else:
            add_presence_edge(f"build:{submission_id}", f"package:{submission_id}", build_path, package_path, "EXECUTION_PACKAGE_MISSING")
        add_presence_edge(f"package:{submission_id}", f"submit_decision:{submission_id}", package_path, matching_trace_path, "SUBMIT_DECISION_TRACE_MISSING")
        add_presence_edge(f"submit_decision:{submission_id}", f"submission:{submission_id}", matching_trace_path, broker_path, "BROKER_SUBMISSION_RECORD_MISSING")
        add_presence_edge(f"submission:{submission_id}", f"lifecycle:{submission_id}", broker_path, lifecycle_path, "EXECUTION_LIFECYCLE_AUTHORITY_MISSING")
        if _path_exists(fill_path):
            fill_submission_id = str(fill_obj.get("submission_id") or submission_id).strip()
            if fill_submission_id != submission_id:
                edges.append(
                    _broken_edge(
                        f"submission:{submission_id}",
                        f"fill:{submission_id}",
                        source_path=broker_path,
                        target_path=fill_path,
                        expected={"submission_id": submission_id},
                        observed={"submission_id": fill_submission_id},
                        blocker_code="FILL_LEDGER_SUBMISSION_ID_CONFLICT",
                    )
                )
            else:
                edges.append(_edge(f"submission:{submission_id}", f"fill:{submission_id}", source_path=broker_path, target_path=fill_path, expected={"submission_id": submission_id}, observed={"submission_id": fill_submission_id}))
        if _path_exists(reconciliation_path):
            edges.append(_edge(f"fill:{submission_id}", f"reconciliation:{submission_id}", source_path=fill_path, target_path=reconciliation_path, expected={"submission_id": submission_id}, observed={"status": reconciliation_obj.get("status")}))

        observed_hashes = sorted({str(row.get("intent_hash") or "").strip() for row in identity_observations if str(row.get("intent_hash") or "").strip()})
        observed_intent_ids = sorted({str(row.get("intent_id") or "").strip() for row in identity_observations if str(row.get("intent_id") or "").strip()})
        trace_hashes = sorted({str(row.get("intent_hash") or "").strip() for row in identity_observations if row.get("source") == "submit_decision_trace_v1" and str(row.get("intent_hash") or "").strip()})
        package_hashes = sorted({str(row.get("intent_hash") or "").strip() for row in identity_observations if row.get("source") == "execution_package_v1" and str(row.get("intent_hash") or "").strip()})
        duplicate_submission_identity = len(trace_hashes) > 1
        conflicting_identity = bool(
            len(observed_intent_ids) > 1
            or (package_hashes and trace_hashes and set(package_hashes).isdisjoint(set(trace_hashes)))
            or (len(observed_hashes) > 1 and not duplicate_submission_identity)
        )
        if duplicate_submission_identity or conflicting_identity:
            edges.append(
                _broken_edge(
                    f"package:{submission_id}",
                    f"submission:{submission_id}",
                    source_path=package_path,
                    target_path=broker_path,
                    expected={"intent_hash": intent_hash, "intent_id": intent_id, "submission_id": submission_id},
                    observed={"intent_hashes": observed_hashes, "intent_ids": observed_intent_ids},
                    blocker_code="DUPLICATE_IDENTITY" if duplicate_submission_identity else "IDENTITY_CONFLICT",
                )
            )

        if broker_transmit_enabled and (order_id is None or perm_id is None):
            edges.append(
                _broken_edge(
                    f"submission:{submission_id}",
                    f"broker_ids:{submission_id}",
                    source_path=broker_path,
                    target_path=broker_path,
                    expected={"order_id": "positive_int", "perm_id": "positive_int"},
                    observed={"order_id": order_id, "perm_id": perm_id},
                    blocker_code="BROKER_IDENTITY_MISSING_AFTER_TRANSMIT",
                )
            )

        row_broken_edges = [edge for edge in edges if edge.get("status") == "BROKEN"]
        if any(edge.get("blocker_code") == "DUPLICATE_IDENTITY" for edge in row_broken_edges):
            identity_state = "DUPLICATE_IDENTITY"
        elif any(edge.get("blocker_code") in {"IDENTITY_CONFLICT", "FILL_LEDGER_SUBMISSION_ID_CONFLICT"} for edge in row_broken_edges):
            identity_state = "IDENTITY_CONFLICT"
        elif any(edge.get("blocker_code") == "EXECUTION_PACKAGE_MISSING_AFTER_BUILD" for edge in row_broken_edges):
            identity_state = "LINEAGE_GAP"
        elif _reconciled(reconciliation_obj) and _path_exists(fill_path):
            identity_state = "RECONCILED"
        elif dry_run:
            identity_state = "DRY_RUN_COMPLETE"
        elif _path_exists(fill_path):
            identity_state = "FILLED"
        elif order_id is not None and perm_id is not None:
            identity_state = "BROKER_IDENTIFIED"
        elif _path_exists(broker_path):
            identity_state = "SUBMIT_RECORDED"
        elif _path_exists(matching_trace_path):
            identity_state = "SUBMIT_DECIDED"
        elif _path_exists(package_path):
            identity_state = "PACKAGED"
        elif _path_exists(build_path):
            identity_state = "BUILT"
        elif _path_exists(auth_path):
            identity_state = "AUTHORIZED"
        elif _path_exists(phasec_path):
            identity_state = "RELEASED"
        else:
            identity_state = "INTENT_ONLY"

        row = {
            "trade_lineage_id": _sha256_payload({"day_utc": day_utc, "submission_id": submission_id, "intent_hash": intent_hash}),
            "identity_state": identity_state,
            "lifecycle_state": lifecycle_state,
            "intent_id": intent_id,
            "intent_hash": intent_hash,
            "submission_id": submission_id,
            "execution_package_id": str(package_obj.get("execution_package_id") or package_obj.get("package_id") or package_obj.get("canonical_json_hash") or "").strip(),
            "broker_order_id": order_id,
            "broker_perm_id": perm_id,
            "dry_run": dry_run,
            "broker_transmit_enabled": broker_transmit_enabled,
            "reconciliation_complete": _reconciled(reconciliation_obj),
            "first_broken_edge": row_broken_edges[0] if row_broken_edges else None,
            "identity_observations": identity_observations,
            "nodes": nodes,
            "edges": edges,
        }
        rows.append(row)
        rows_by_submission[submission_id] = row
        broken_edges.extend(row_broken_edges)

    for intent_hash, row in intents.items():
        if any(item.get("intent_hash") == intent_hash for item in rows):
            continue
        intent_path = Path(str(row.get("paths", [""])[0]))
        rows.append(
            {
                "trade_lineage_id": _sha256_payload({"day_utc": day_utc, "intent_hash": intent_hash}),
                "identity_state": "INTENT_ONLY",
                "lifecycle_state": "",
                "intent_id": str(row.get("intent_id") or ""),
                "intent_hash": intent_hash,
                "submission_id": "",
                "execution_package_id": "",
                "broker_order_id": None,
                "broker_perm_id": None,
                "dry_run": False,
                "broker_transmit_enabled": False,
                "reconciliation_complete": False,
                "first_broken_edge": None,
                "identity_observations": [{"source": "intents_v1", "intent_hash": intent_hash, "intent_id": str(row.get("intent_id") or ""), "submission_id": ""}],
                "nodes": [_node(f"intent:{intent_hash}", "intent", intent_path, {"intent_hash": intent_hash, "intent_id": str(row.get("intent_id") or "")})],
                "edges": [],
            }
        )

    intent_to_submissions: dict[str, list[dict[str, Any]]] = defaultdict(list)
    submission_to_intents: dict[str, set[str]] = defaultdict(set)
    for row in rows:
        ih = str(row.get("intent_hash") or "").strip()
        sid = str(row.get("submission_id") or "").strip()
        if ih and sid:
            intent_to_submissions[ih].append(row)
            submission_to_intents[sid].add(ih)
    for intent_hash, mapped_rows in intent_to_submissions.items():
        submission_ids = sorted({str(row.get("submission_id") or "") for row in mapped_rows if str(row.get("submission_id") or "")})
        if len(submission_ids) > 1 and not _has_retry_marker(mapped_rows):
            for row in mapped_rows:
                row["identity_state"] = "DUPLICATE_IDENTITY"
                edge = _broken_edge(
                    f"intent:{intent_hash}",
                    f"submission:{row.get('submission_id')}",
                    source_path=None,
                    target_path=None,
                    expected={"intent_hash": intent_hash, "single_active_submission_id": True},
                    observed={"submission_ids": submission_ids},
                    blocker_code="INTENT_HASH_MULTIPLE_ACTIVE_SUBMISSIONS",
                )
                row["edges"].append(edge)
                row["first_broken_edge"] = row.get("first_broken_edge") or edge
                broken_edges.append(edge)
    for submission_id, mapped_intents in submission_to_intents.items():
        if len(mapped_intents) > 1 and submission_id in rows_by_submission:
            row = rows_by_submission[submission_id]
            row["identity_state"] = "DUPLICATE_IDENTITY"

    projection_consistency: list[dict[str, Any]] = []
    selected = str(current_head_obj.get("selected_attempt_id") or "").strip()
    if selected:
        projection_consistency.append(
            {
                "projection": "execution_evidence_v1/current_head",
                "path": str(current_head_path),
                "status": "AGREE" if selected in rows_by_submission else "STALE_PROJECTION",
                "reason_code": "" if selected in rows_by_submission else "CURRENT_HEAD_SELECTED_ATTEMPT_NOT_IN_TRADE_LINEAGE_GRAPH",
            }
        )
    attempts = submission_index_obj.get("attempts") if isinstance(submission_index_obj.get("attempts"), list) else []
    for attempt in attempts:
        if not isinstance(attempt, dict):
            continue
        sid = str(attempt.get("attempt_id") or "").strip()
        if sid and sid not in rows_by_submission:
            projection_consistency.append(
                {
                    "projection": "submission_index_v1",
                    "path": str(submission_index_path),
                    "status": "STALE_PROJECTION",
                    "reason_code": "SUBMISSION_INDEX_ATTEMPT_NOT_IN_TRADE_LINEAGE_GRAPH",
                }
            )

    if not rows:
        identity_state = "NO_INTENTS"
    elif any(row.get("identity_state") == "DUPLICATE_IDENTITY" for row in rows):
        identity_state = "DUPLICATE_IDENTITY"
    elif any(row.get("identity_state") == "IDENTITY_CONFLICT" for row in rows):
        identity_state = "IDENTITY_CONFLICT"
    elif any(row.get("identity_state") == "LINEAGE_GAP" for row in rows):
        identity_state = "LINEAGE_GAP"
    elif all(row.get("identity_state") == "DRY_RUN_COMPLETE" for row in rows if row.get("submission_id")) and any(row.get("submission_id") for row in rows):
        identity_state = "DRY_RUN_COMPLETE"
    elif any(row.get("identity_state") == "RECONCILED" for row in rows):
        identity_state = "RECONCILED"
    elif any(row.get("identity_state") == "BROKER_IDENTIFIED" for row in rows):
        identity_state = "BROKER_IDENTIFIED"
    else:
        identity_state = str(rows[0].get("identity_state") or "INTENT_ONLY")

    all_nodes = [node for row in rows for node in row.get("nodes", [])]
    all_edges = [edge for row in rows for edge in row.get("edges", [])]
    payload: dict[str, Any] = {
        "schema_id": SCHEMA_ID,
        "schema_version": SCHEMA_VERSION,
        "day_utc": day_utc,
        "environment": str(environment).strip().upper() or "PAPER",
        "produced_utc": produced_utc or _utc_now_iso(),
        "authority_scope": "END_TO_END_TRADE_IDENTITY",
        "status": "PASS" if identity_state not in {"LINEAGE_GAP", "IDENTITY_CONFLICT", "DUPLICATE_IDENTITY"} else "FAIL",
        "identity_state": identity_state,
        "lineage_count": len(rows),
        "node_count": len(all_nodes),
        "edge_count": len(all_edges),
        "broken_edge_count": len([edge for edge in all_edges if edge.get("status") == "BROKEN"]),
        "first_broken_edge": broken_edges[0] if broken_edges else None,
        "lineages": rows,
        "projection_consistency": projection_consistency,
        "input_evidence": [
            {"artifact_type": "intents_v1", "path": str((execution_root / "intents_v1" / "snapshots" / day_utc).resolve()), "exists": bool((execution_root / "intents_v1" / "snapshots" / day_utc).exists())},
            {"artifact_type": "trading_day_intent_generation_v1", "path": str((truth_root / "reports" / "trading_day_intent_generation_v1" / day_utc / "trading_day_intent_generation.v1.json").resolve()), "exists": bool((truth_root / "reports" / "trading_day_intent_generation_v1" / day_utc / "trading_day_intent_generation.v1.json").exists())},
            {"artifact_type": "phaseC_preflight_v1", "path": str((execution_root / "phaseC_preflight_v1" / day_utc).resolve()), "exists": bool((execution_root / "phaseC_preflight_v1" / day_utc).exists())},
            {"artifact_type": "engine_activity_v1/authorization_v1", "path": str((execution_root / "engine_activity_v1" / "authorization_v1" / day_utc).resolve()), "exists": bool((execution_root / "engine_activity_v1" / "authorization_v1" / day_utc).exists())},
            {"artifact_type": "execution_build_v1", "path": str((truth_root / "reports" / "execution_build_v1" / day_utc).resolve()), "exists": bool((truth_root / "reports" / "execution_build_v1" / day_utc).exists())},
            {"artifact_type": "execution_package_v1", "path": str((execution_root / "execution_package_v1" / day_utc).resolve()), "exists": bool((execution_root / "execution_package_v1" / day_utc).exists())},
            {"artifact_type": "submit_decision_trace_v1", "path": str((truth_root / "reports" / "submit_decision_trace_v1" / day_utc).resolve()), "exists": bool((truth_root / "reports" / "submit_decision_trace_v1" / day_utc).exists())},
            {"artifact_type": "sleeve_intent_trade_attribution_v1", "path": str((truth_root / "reports" / "sleeve_intent_trade_attribution_v1" / day_utc / "sleeve_intent_trade_attribution.v1.json").resolve()), "exists": bool((truth_root / "reports" / "sleeve_intent_trade_attribution_v1" / day_utc / "sleeve_intent_trade_attribution.v1.json").exists())},
            {"artifact_type": "submission_index_v1", "path": str(submission_index_path), "exists": bool(submission_index_path.exists())},
            {"artifact_type": "execution_evidence_v1/submissions", "path": str(submissions_root.resolve()), "exists": bool(submissions_root.exists())},
            {"artifact_type": "execution_evidence_v1/current_head", "path": str(current_head_path), "exists": bool(current_head_path.exists())},
            {"artifact_type": "execution_lifecycle_authority_v1", "path": str(lifecycle_path), "exists": bool(lifecycle_path.exists())},
            {"artifact_type": "fill_ledger_v1", "path": str((execution_root / "fill_ledger_v1" / day_utc).resolve()), "exists": bool((execution_root / "fill_ledger_v1" / day_utc).exists())},
            {"artifact_type": "execution_reconciliation_v1", "path": str(reconciliation_path), "exists": bool(reconciliation_path.exists())},
        ],
        "canonical_json_hash": "",
    }
    payload["canonical_json_hash"] = _sha256_payload(payload)
    return payload


def write_trade_lineage_graph_v1(*, truth_root: Path, day_utc: str, payload: dict[str, Any]) -> Path:
    output_path = trade_lineage_graph_output_path(truth_root=truth_root, day_utc=day_utc)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_bytes(_canonical_bytes(payload) + b"\n")
    return output_path
