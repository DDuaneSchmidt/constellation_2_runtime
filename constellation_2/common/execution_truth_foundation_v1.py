from __future__ import annotations

import traceback
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from constellation_2.common.diagnostic_foundation_v1 import (
    _git_sha,
    _read_json_obj,
    _require_day_utc,
    _require_produced_utc,
    select_attempt_for_day,
)
from constellation_2.phaseD.lib.canon_json_v1 import canonical_json_bytes_v1
from constellation_2.phaseD.lib.validate_against_schema_v1 import (
    SchemaValidationError,
    validate_against_repo_schema_v1,
)
from constellation_2.phaseF.accounting.lib.day_artifact_refresh_v1 import (
    RefreshWriteResultV1,
    write_day_artifact_refreshable_v1,
)


EXECUTION_COMPLETION_RULESET_ID = "EXECUTION_COMPLETION_RULESET_V1"
EXECUTION_COMPLETION_RULESET_VERSION = 1
DEPENDENCY_REGISTRY_ID = "C2_EXECUTION_DEPENDENCY_REGISTRY_V1"
DEPENDENCY_REGISTRY_VERSION = 1
PAYLOAD_CONTRACTS_ID = "C2_EXECUTION_PAYLOAD_COMPLETENESS_CONTRACTS_V1"
PAYLOAD_CONTRACTS_VERSION = 1

DEPENDENCY_REGISTRY_RELPATH = "governance/02_REGISTRIES/C2_EXECUTION_DEPENDENCY_REGISTRY_V1.json"
PAYLOAD_CONTRACTS_RELPATH = "governance/02_REGISTRIES/C2_EXECUTION_PAYLOAD_COMPLETENESS_CONTRACTS_V1.json"

NORMALIZATION_SCHEMA = "governance/04_DATA/SCHEMAS/C2/REPORTS/execution_evidence_normalization.v1.schema.json"
LIFECYCLE_SCHEMA = "governance/04_DATA/SCHEMAS/C2/REPORTS/lifecycle_progression_status.v1.schema.json"
ECONOMIC_SCHEMA = "governance/04_DATA/SCHEMAS/C2/REPORTS/economic_finalization_status.v1.schema.json"
GAP_SCHEMA = "governance/04_DATA/SCHEMAS/C2/REPORTS/execution_completion_gap_report.v1.schema.json"

ROLLUP_SCHEMA = "governance/04_DATA/SCHEMAS/C2/ENGINE_ACTIVITY/intents_day_rollup.v1.schema.json"
INTENT_SNAPSHOT_SCHEMA = "constellation_2/schemas/exposure_intent.v1.schema.json"
AUTHORIZATION_SCHEMA = "governance/04_DATA/SCHEMAS/C2/ENGINE_ACTIVITY/authorization.v1.schema.json"
BROKER_SUBMISSION_SCHEMA = "constellation_2/schemas/broker_submission_record.v2.schema.json"
EXECUTION_EVENT_SCHEMA = "constellation_2/schemas/execution_event_record.v1.schema.json"
EXECUTION_STREAM_SCHEMA = "governance/04_DATA/SCHEMAS/C2/EXECUTION_EVIDENCE/execution_event_stream_record.v1.schema.json"
FILL_LEDGER_SCHEMA = "governance/04_DATA/SCHEMAS/C2/EXECUTION_EVIDENCE/fill_ledger.v1.schema.json"
CASH_LEDGER_SCHEMA = "governance/04_DATA/SCHEMAS/C2/CASH_LEDGER/cash_ledger_snapshot.v1.schema.json"
POSITIONS_SCHEMA = "governance/04_DATA/SCHEMAS/C2/POSITIONS/positions_snapshot.v2.schema.json"
ACCOUNTING_SCHEMA = "governance/04_DATA/SCHEMAS/C2/ACCOUNTING/accounting_nav.v2.schema.json"
EXIT_RECON_SCHEMA = "governance/04_DATA/SCHEMAS/C2/EXIT_OBLIGATIONS/exit_reconciliation.v1.schema.json"

EXPLANATORY_ARTIFACTS = {
    "gate_stack_verdict_v1": ("reports/gate_stack_verdict_v1/{day}/gate_stack_verdict.v1.json", "governance/04_DATA/SCHEMAS/C2/REPORTS/gate_stack_verdict.v1.schema.json"),
    "root_cause_v1": ("reports/root_cause_v1/{day}/root_cause.v1.json", "governance/04_DATA/SCHEMAS/C2/REPORTS/root_cause.v1.schema.json"),
    "activity_flow_diagnostics_v1": ("reports/activity_flow_diagnostics_v1/{day}/activity_flow_diagnostics.v1.json", "governance/04_DATA/SCHEMAS/C2/REPORTS/activity_flow_diagnostics.v1.schema.json"),
    "daily_summary_v1": ("reports/daily_summary_v1/{day}/daily_summary.v1.json", "governance/04_DATA/SCHEMAS/C2/REPORTS/daily_summary.v1.schema.json"),
}

LIFECYCLE_STAGES = (
    "INTENT_EMITTED",
    "AUTHORIZATION_COMPLETE",
    "SUBMISSION_COMPLETE",
    "EXECUTION_STREAM_COMPLETE",
    "FILL_COMPLETE",
)

ECONOMIC_STAGES = (
    "POSITIONS",
    "CASH",
    "MARKS",
    "ACCOUNTING",
    "EXIT_RECONCILIATION",
)


@dataclass(frozen=True)
class JsonProbe:
    path: Path
    present: bool
    valid: bool
    doc: Optional[Dict[str, Any]]
    error_code: str
    error_detail: str


@dataclass(frozen=True)
class FamilyProbe:
    family: str
    observed_paths: Tuple[str, ...]
    record_count: int
    schema_validation_status: str
    completeness_status: str
    missing_required_fields: Tuple[str, ...]
    missing_join_critical_fields: Tuple[str, ...]
    downstream_stage_blockers: Tuple[str, ...]
    notes: Tuple[str, ...]


def _read_registry(repo_root: Path, relpath: str) -> Dict[str, Any]:
    return _read_json_obj((repo_root / relpath).resolve())


def _probe_json(repo_root: Path, path: Path, schema_relpath: Optional[str]) -> JsonProbe:
    if not path.exists() or not path.is_file():
        return JsonProbe(path=path, present=False, valid=False, doc=None, error_code="MISSING_ARTIFACT", error_detail=str(path))
    try:
        doc = _read_json_obj(path)
        if schema_relpath:
            validate_against_repo_schema_v1(doc, repo_root, schema_relpath)
    except SchemaValidationError as exc:
        return JsonProbe(path=path, present=True, valid=False, doc=_safe_read_obj(path), error_code="SCHEMA_MISMATCH", error_detail=str(exc))
    except Exception as exc:
        return JsonProbe(path=path, present=True, valid=False, doc=_safe_read_obj(path), error_code="PARSE_OR_SHAPE_FAILURE", error_detail=str(exc))
    return JsonProbe(path=path, present=True, valid=True, doc=doc, error_code="", error_detail="")


def _safe_read_obj(path: Path) -> Optional[Dict[str, Any]]:
    try:
        return _read_json_obj(path)
    except BaseException:
        return None


def _has_field(doc: Dict[str, Any], dotted: str) -> bool:
    cur: Any = doc
    for part in dotted.split("."):
        if isinstance(cur, dict) and part in cur:
            cur = cur[part]
            continue
        return False
    return True


def _missing_fields(doc: Optional[Dict[str, Any]], fields: List[str]) -> List[str]:
    if not isinstance(doc, dict):
        return list(fields)
    return [field for field in fields if not _has_field(doc, field)]


def _read_text_json_if_present(path: Path) -> Optional[Dict[str, Any]]:
    if not path.exists() or not path.is_file():
        return None
    try:
        obj = _read_json_obj(path)
    except BaseException:
        return None
    return obj


def _derive_scope_from_truth_root(truth_root: Path, day_utc: str) -> Dict[str, Any]:
    parts = list(truth_root.parts)
    sleeve_id = None
    mode = None
    if "truth_sleeves" in parts:
        idx = parts.index("truth_sleeves")
        if len(parts) > idx + 2:
            sleeve_id = parts[idx + 1]
            mode = parts[idx + 2]
    return {
        "day_utc": day_utc,
        "mode": mode,
        "sleeve_id": sleeve_id,
        "engine_id": None,
        "attempt_id": None,
        "attempt_seq": None,
        "truth_partition": str(truth_root),
    }


def _derive_run_scope(repo_root: Path, truth_root: Path, day_utc: str, facts: Dict[str, Any]) -> Dict[str, Any]:
    scope = _derive_scope_from_truth_root(truth_root, day_utc)
    try:
        selection = select_attempt_for_day(truth_root, day_utc)
        scope["attempt_id"] = selection.pointer_entry.get("attempt_id")
        scope["attempt_seq"] = selection.pointer_entry.get("attempt_seq")
        verdict = selection.verdict
        if isinstance(verdict, dict):
            scope["mode"] = verdict.get("mode", scope["mode"])
            scope["engine_id"] = scope["engine_id"]
    except BaseException:
        pass
    rollup = facts.get("day_rollup_doc")
    if isinstance(rollup, dict):
        engines = rollup.get("engines")
        if isinstance(engines, list) and engines:
            engine0 = engines[0]
            if isinstance(engine0, dict) and isinstance(engine0.get("engine_id"), str):
                scope["engine_id"] = engine0.get("engine_id")
    for path in facts.get("authorization_paths", []):
        auth_doc = _read_text_json_if_present(Path(path))
        if isinstance(auth_doc, dict) and isinstance(auth_doc.get("engine_id"), str):
            scope["engine_id"] = auth_doc.get("engine_id")
            break
    return scope


def _artifact_ref(truth_root: Path, family: str, day_utc: str, filename: str) -> str:
    return str((truth_root / "reports" / family / day_utc / filename).resolve())


def _source_probe_paths(truth_root: Path, day_utc: str) -> Dict[str, Any]:
    submissions_root = (truth_root / "execution_evidence_v1" / "submissions" / day_utc).resolve()
    return {
        "day_rollup": (truth_root / "intents_v1" / "day_rollup" / day_utc / "intents_day_rollup.v1.json").resolve(),
        "intent_snapshot_glob_root": (truth_root / "intents_v1" / "snapshots" / day_utc).resolve(),
        "authorization_root": (truth_root / "engine_activity_v1" / "authorization_v1" / day_utc).resolve(),
        "submissions_root": submissions_root,
        "execution_stream_root": (truth_root / "execution_stream_v1" / day_utc).resolve(),
        "fill_ledger_root": (truth_root / "fill_ledger_v1" / day_utc).resolve(),
        "positions_snapshot": (truth_root / "positions_v1" / "snapshots" / day_utc / "positions_snapshot.v2.json").resolve(),
        "cash_ledger_snapshot": (truth_root / "cash_ledger_v1" / "snapshots" / day_utc / "cash_ledger_snapshot.v1.json").resolve(),
        "broker_statement_normalized": (truth_root / "execution_evidence_v1" / "broker_statement_normalized_v1" / day_utc / "broker_statement_normalized.v1.json").resolve(),
        "broker_marks": (truth_root / "market_data_snapshot_v1" / "broker_marks_v1" / day_utc / "broker_marks.v1.json").resolve(),
        "accounting_nav": (truth_root / "accounting_v2" / "nav" / day_utc / "nav.v2.json").resolve(),
        "exit_reconciliation": (truth_root / "exit_reconciliation_v1" / day_utc / "exit_reconciliation.v1.json").resolve(),
    }


def _list_json_paths(root: Path, pattern: str) -> List[Path]:
    if not root.exists() or not root.is_dir():
        return []
    return sorted([p.resolve() for p in root.glob(pattern) if p.is_file()], key=lambda p: str(p))


def _submission_dirs(root: Path) -> List[Path]:
    if not root.exists() or not root.is_dir():
        return []
    return sorted([p.resolve() for p in root.iterdir() if p.is_dir()], key=lambda p: str(p))


def collect_execution_truth_facts(repo_root: Path, truth_root: Path, day_utc: str) -> Dict[str, Any]:
    paths = _source_probe_paths(truth_root, day_utc)
    day_rollup_probe = _probe_json(repo_root, paths["day_rollup"], ROLLUP_SCHEMA)
    auth_paths = _list_json_paths(paths["authorization_root"], "*.authorization.v1.json")
    intent_snapshot_paths = _list_json_paths(paths["intent_snapshot_glob_root"], "*.exposure_intent.v1.json")
    execution_stream_paths = _list_json_paths(paths["execution_stream_root"], "*.execution_event_stream_record.v1.json")
    fill_ledger_paths = _list_json_paths(paths["fill_ledger_root"], "*.fill_ledger.v1.json")
    submission_dirs = _submission_dirs(paths["submissions_root"])
    repo_input_root = (repo_root / "constellation_2" / "operator_inputs").resolve()
    operator_statement_path = (repo_input_root / "cash_ledger_operator_statements" / day_utc / "operator_statement.v1.json").resolve()
    broker_marks_override_path = (repo_input_root / "broker_marks_operator_overrides" / day_utc / "broker_marks_override.v1.json").resolve()
    explanatory_refs: List[str] = []
    for _, (relpattern, schema_relpath) in EXPLANATORY_ARTIFACTS.items():
        probe = _probe_json(repo_root, (truth_root / relpattern.format(day=day_utc)).resolve(), schema_relpath)
        if probe.present:
            explanatory_refs.append(str(probe.path))
    return {
        "paths": paths,
        "day_rollup_probe": day_rollup_probe,
        "day_rollup_doc": day_rollup_probe.doc,
        "intent_snapshot_paths": [str(p) for p in intent_snapshot_paths],
        "authorization_paths": [str(p) for p in auth_paths],
        "submission_dirs": [str(p) for p in submission_dirs],
        "execution_stream_paths": [str(p) for p in execution_stream_paths],
        "fill_ledger_paths": [str(p) for p in fill_ledger_paths],
        "operator_statement_path": str(operator_statement_path),
        "broker_marks_override_path": str(broker_marks_override_path),
        "explanatory_refs": explanatory_refs,
    }


def _family_probe_from_json_paths(
    *,
    repo_root: Path,
    family: str,
    paths: List[Path],
    schema_relpath: Optional[str],
    required_fields: List[str],
    join_fields: List[str],
    downstream_stage_blockers: List[str],
    missing_status: str = "MISSING_ARTIFACT",
) -> FamilyProbe:
    if not paths:
        return FamilyProbe(
            family=family,
            observed_paths=tuple(),
            record_count=0,
            schema_validation_status=missing_status,
            completeness_status=missing_status,
            missing_required_fields=tuple(required_fields),
            missing_join_critical_fields=tuple(join_fields),
            downstream_stage_blockers=tuple(downstream_stage_blockers),
            notes=tuple(),
        )
    schema_status = "VALID"
    completeness_status = "COMPLETE"
    missing_required: List[str] = []
    missing_join: List[str] = []
    notes: List[str] = []
    for path in paths:
        probe = _probe_json(repo_root, path, schema_relpath)
        if not probe.valid:
            schema_status = probe.error_code or "INVALID"
            completeness_status = "SCHEMA_MISMATCH"
        missing_required.extend(_missing_fields(probe.doc, required_fields))
        missing_join.extend(_missing_fields(probe.doc, join_fields))
    if completeness_status == "COMPLETE" and missing_required:
        completeness_status = "MISSING_REQUIRED_FIELDS"
    if completeness_status == "COMPLETE" and missing_join:
        completeness_status = "MISSING_JOIN_CRITICAL_FIELDS"
    if schema_status != "VALID":
        notes.append("schema_validation_failed")
    return FamilyProbe(
        family=family,
        observed_paths=tuple(str(p) for p in paths),
        record_count=len(paths),
        schema_validation_status=schema_status,
        completeness_status=completeness_status,
        missing_required_fields=tuple(sorted(set(missing_required))),
        missing_join_critical_fields=tuple(sorted(set(missing_join))),
        downstream_stage_blockers=tuple(downstream_stage_blockers),
        notes=tuple(sorted(set(notes))),
    )


def _intents_day_rollup_linkage_probe(repo_root: Path, facts: Dict[str, Any]) -> FamilyProbe:
    probe: JsonProbe = facts["day_rollup_probe"]
    if not probe.present:
        return FamilyProbe(
            family="intents_day_rollup_linkage",
            observed_paths=tuple(),
            record_count=0,
            schema_validation_status="MISSING_ARTIFACT",
            completeness_status="MISSING_ARTIFACT",
            missing_required_fields=("engines",),
            missing_join_critical_fields=("engines.intent_hashes",),
            downstream_stage_blockers=("INTENT_EMITTED",),
            notes=tuple(),
        )
    doc = probe.doc if isinstance(probe.doc, dict) else None
    if not probe.valid:
        return FamilyProbe(
            family="intents_day_rollup_linkage",
            observed_paths=(str(probe.path),),
            record_count=1,
            schema_validation_status=probe.error_code or "SCHEMA_MISMATCH",
            completeness_status="SCHEMA_MISMATCH",
            missing_required_fields=tuple(_missing_fields(doc, ["engines"])),
            missing_join_critical_fields=("engines.intent_hashes",),
            downstream_stage_blockers=("INTENT_EMITTED",),
            notes=(probe.error_detail,),
        )
    missing_join: List[str] = []
    notes: List[str] = []
    engines = doc.get("engines")
    if not isinstance(engines, list) or not engines:
        missing_join.append("engines")
    else:
        linkage_missing = False
        for idx, engine in enumerate(engines):
            if not isinstance(engine, dict):
                linkage_missing = True
                continue
            intent_count = int(engine.get("intent_count") or 0)
            intent_hashes = engine.get("intent_hashes")
            if intent_count > 0 and (not isinstance(intent_hashes, list) or not intent_hashes):
                linkage_missing = True
                missing_join.append(f"engines[{idx}].intent_hashes")
        if linkage_missing:
            notes.append("aggregate_only_rollup_no_record_level_intent_hashes")
    status = "COMPLETE"
    if missing_join:
        status = "AGGREGATE_ONLY_NO_LINKAGE"
    return FamilyProbe(
        family="intents_day_rollup_linkage",
        observed_paths=(str(probe.path),),
        record_count=1,
        schema_validation_status="VALID",
        completeness_status=status,
        missing_required_fields=tuple(),
        missing_join_critical_fields=tuple(sorted(set(missing_join))),
        downstream_stage_blockers=("INTENT_EMITTED",),
        notes=tuple(sorted(set(notes))),
    )


def _fill_ledger_input_probe(repo_root: Path, facts: Dict[str, Any]) -> FamilyProbe:
    submission_dirs = [Path(p) for p in facts.get("submission_dirs", [])]
    if not submission_dirs:
        return FamilyProbe(
            family="fill_ledger_input_bundle",
            observed_paths=tuple(),
            record_count=0,
            schema_validation_status="MISSING_ARTIFACT",
            completeness_status="MISSING_ARTIFACT",
            missing_required_fields=(
                "broker_submission_record.submission_id",
                "execution_event_record.filled_qty",
                "equity_order_plan.qty_shares",
                "execution_stream.submission_id",
            ),
            missing_join_critical_fields=(
                "broker_submission_record.binding_hash",
                "execution_event_record.broker_submission_hash",
                "equity_order_plan.intent_hash",
                "execution_stream.intent_sha256",
            ),
            downstream_stage_blockers=("FILL_COMPLETE",),
            notes=tuple(),
        )
    observed_paths: List[str] = []
    missing_required: List[str] = []
    missing_join: List[str] = []
    notes: List[str] = []
    schema_status = "VALID"
    for subdir in submission_dirs:
        bsr_p = subdir / "broker_submission_record.v2.json"
        evt_p = subdir / "execution_event_record.v1.json"
        plan_p = subdir / "equity_order_plan.v1.json"
        for p in (bsr_p, evt_p, plan_p):
            if p.exists():
                observed_paths.append(str(p.resolve()))
        if bsr_p.exists():
            bsr_probe = _probe_json(repo_root, bsr_p.resolve(), BROKER_SUBMISSION_SCHEMA)
            if not bsr_probe.valid:
                schema_status = bsr_probe.error_code or "SCHEMA_MISMATCH"
            missing_required.extend(
                f"broker_submission_record.{field}" for field in _missing_fields(bsr_probe.doc, ["submission_id", "submitted_at_utc", "binding_hash", "broker_ids.order_id", "broker_ids.perm_id"])
            )
            missing_join.extend(
                f"broker_submission_record.{field}" for field in _missing_fields(bsr_probe.doc, ["submission_id", "binding_hash", "broker_ids.order_id", "broker_ids.perm_id"])
            )
        else:
            missing_required.extend([
                "broker_submission_record.submission_id",
                "broker_submission_record.submitted_at_utc",
                "broker_submission_record.binding_hash",
                "broker_submission_record.broker_ids.order_id",
                "broker_submission_record.broker_ids.perm_id",
            ])
            missing_join.extend([
                "broker_submission_record.submission_id",
                "broker_submission_record.binding_hash",
                "broker_submission_record.broker_ids.order_id",
                "broker_submission_record.broker_ids.perm_id",
            ])
        if evt_p.exists():
            evt_probe = _probe_json(repo_root, evt_p.resolve(), EXECUTION_EVENT_SCHEMA)
            if not evt_probe.valid:
                schema_status = evt_probe.error_code or "SCHEMA_MISMATCH"
            missing_required.extend(
                f"execution_event_record.{field}" for field in _missing_fields(evt_probe.doc, ["created_at_utc", "event_time_utc", "binding_hash", "broker_submission_hash", "broker_order_id", "perm_id", "filled_qty", "avg_price"])
            )
            missing_join.extend(
                f"execution_event_record.{field}" for field in _missing_fields(evt_probe.doc, ["binding_hash", "broker_submission_hash", "broker_order_id", "perm_id"])
            )
        else:
            missing_required.extend([
                "execution_event_record.created_at_utc",
                "execution_event_record.event_time_utc",
                "execution_event_record.binding_hash",
                "execution_event_record.broker_submission_hash",
                "execution_event_record.broker_order_id",
                "execution_event_record.perm_id",
                "execution_event_record.filled_qty",
                "execution_event_record.avg_price",
            ])
            missing_join.extend([
                "execution_event_record.binding_hash",
                "execution_event_record.broker_submission_hash",
                "execution_event_record.broker_order_id",
                "execution_event_record.perm_id",
            ])
        if plan_p.exists():
            plan_probe = _probe_json(repo_root, plan_p.resolve(), "constellation_2/schemas/equity_order_plan.v1.schema.json")
            if not plan_probe.valid:
                schema_status = plan_probe.error_code or "SCHEMA_MISMATCH"
            missing_required.extend(
                f"equity_order_plan.{field}" for field in _missing_fields(plan_probe.doc, ["plan_id", "intent_hash", "symbol", "qty_shares"])
            )
            missing_join.extend(
                f"equity_order_plan.{field}" for field in _missing_fields(plan_probe.doc, ["intent_hash", "qty_shares"])
            )
        else:
            missing_required.extend([
                "equity_order_plan.plan_id",
                "equity_order_plan.intent_hash",
                "equity_order_plan.symbol",
                "equity_order_plan.qty_shares",
            ])
            missing_join.extend([
                "equity_order_plan.intent_hash",
                "equity_order_plan.qty_shares",
            ])
    stream_paths = [Path(p) for p in facts.get("execution_stream_paths", [])]
    if not stream_paths:
        missing_required.extend([
            "execution_stream.submission_id",
            "execution_stream.binding_hash",
            "execution_stream.intent_sha256",
            "execution_stream.fill.fill_qty",
            "execution_stream.fill.fill_price",
        ])
        missing_join.extend([
            "execution_stream.submission_id",
            "execution_stream.binding_hash",
            "execution_stream.intent_sha256",
        ])
        notes.append("execution_stream_missing")
    status = "COMPLETE"
    if schema_status != "VALID":
        status = "SCHEMA_MISMATCH"
    elif missing_required or missing_join:
        status = "INPUTS_TOO_SPARSE"
    return FamilyProbe(
        family="fill_ledger_input_bundle",
        observed_paths=tuple(sorted(set(observed_paths))),
        record_count=len(submission_dirs),
        schema_validation_status=schema_status,
        completeness_status=status,
        missing_required_fields=tuple(sorted(set(missing_required))),
        missing_join_critical_fields=tuple(sorted(set(missing_join))),
        downstream_stage_blockers=("FILL_COMPLETE",),
        notes=tuple(sorted(set(notes))),
    )


def build_payload_probes(repo_root: Path, truth_root: Path, day_utc: str, facts: Dict[str, Any]) -> Dict[str, FamilyProbe]:
    paths = facts["paths"]
    probes = {
        "intents_day_rollup_linkage": _intents_day_rollup_linkage_probe(repo_root, facts),
        "intent_snapshot": _family_probe_from_json_paths(
            repo_root=repo_root,
            family="intent_snapshot",
            paths=[Path(p) for p in facts.get("intent_snapshot_paths", [])],
            schema_relpath=INTENT_SNAPSHOT_SCHEMA,
            required_fields=["intent_id", "created_at_utc", "engine.engine_id", "engine.mode", "underlying.symbol"],
            join_fields=["intent_id", "engine.engine_id", "underlying.symbol"],
            downstream_stage_blockers=["INTENT_EMITTED"],
        ),
        "authorization": _family_probe_from_json_paths(
            repo_root=repo_root,
            family="authorization",
            paths=[Path(p) for p in facts.get("authorization_paths", [])],
            schema_relpath=AUTHORIZATION_SCHEMA,
            required_fields=["schema_id", "schema_version", "day_utc", "engine_id", "intent_id", "intent_hash", "authorization.decision_hash"],
            join_fields=["engine_id", "intent_id", "intent_hash", "authorization.decision_hash"],
            downstream_stage_blockers=["AUTHORIZATION_COMPLETE"],
        ),
        "broker_submission_record": _family_probe_from_json_paths(
            repo_root=repo_root,
            family="broker_submission_record",
            paths=[(Path(p) / "broker_submission_record.v2.json").resolve() for p in facts.get("submission_dirs", []) if (Path(p) / "broker_submission_record.v2.json").exists()],
            schema_relpath=BROKER_SUBMISSION_SCHEMA,
            required_fields=["submission_id", "submitted_at_utc", "binding_hash", "broker_ids.order_id", "broker_ids.perm_id"],
            join_fields=["submission_id", "binding_hash", "broker_ids.order_id", "broker_ids.perm_id"],
            downstream_stage_blockers=["SUBMISSION_COMPLETE", "EXECUTION_STREAM_COMPLETE", "FILL_COMPLETE"],
        ),
        "execution_event_record": _family_probe_from_json_paths(
            repo_root=repo_root,
            family="execution_event_record",
            paths=[(Path(p) / "execution_event_record.v1.json").resolve() for p in facts.get("submission_dirs", []) if (Path(p) / "execution_event_record.v1.json").exists()],
            schema_relpath=EXECUTION_EVENT_SCHEMA,
            required_fields=["created_at_utc", "event_time_utc", "binding_hash", "broker_submission_hash", "broker_order_id", "perm_id", "filled_qty", "avg_price"],
            join_fields=["binding_hash", "broker_submission_hash", "broker_order_id", "perm_id"],
            downstream_stage_blockers=["EXECUTION_STREAM_COMPLETE", "FILL_COMPLETE", "POSITIONS"],
        ),
        "execution_stream": _family_probe_from_json_paths(
            repo_root=repo_root,
            family="execution_stream",
            paths=[Path(p) for p in facts.get("execution_stream_paths", [])],
            schema_relpath=EXECUTION_STREAM_SCHEMA,
            required_fields=["submission_id", "binding_hash", "engine_id", "source_intent_id", "intent_sha256", "event_time_utc", "fill.fill_qty", "fill.fill_price"],
            join_fields=["submission_id", "binding_hash", "intent_sha256", "broker_ids.order_id", "broker_ids.perm_id"],
            downstream_stage_blockers=["EXECUTION_STREAM_COMPLETE", "FILL_COMPLETE"],
        ),
        "fill_ledger_input_bundle": _fill_ledger_input_probe(repo_root, facts),
        "fill_ledger": _family_probe_from_json_paths(
            repo_root=repo_root,
            family="fill_ledger",
            paths=[Path(p) for p in facts.get("fill_ledger_paths", [])],
            schema_relpath=FILL_LEDGER_SCHEMA,
            required_fields=["submission_id", "binding_hash", "engine_id", "source_intent_id", "intent_sha256", "filled_qty", "avg_fill_price_weighted"],
            join_fields=["submission_id", "binding_hash", "intent_sha256"],
            downstream_stage_blockers=["FILL_COMPLETE", "POSITIONS", "ACCOUNTING"],
        ),
        "positions_snapshot": _family_probe_from_json_paths(
            repo_root=repo_root,
            family="positions_snapshot",
            paths=[paths["positions_snapshot"]] if paths["positions_snapshot"].exists() else [],
            schema_relpath=POSITIONS_SCHEMA,
            required_fields=["positions.asof_utc", "positions.items"],
            join_fields=["positions.items"],
            downstream_stage_blockers=["POSITIONS", "ACCOUNTING", "EXIT_RECONCILIATION"],
        ),
        "cash_ledger_snapshot": _family_probe_from_json_paths(
            repo_root=repo_root,
            family="cash_ledger_snapshot",
            paths=[paths["cash_ledger_snapshot"]] if paths["cash_ledger_snapshot"].exists() else [],
            schema_relpath=CASH_LEDGER_SCHEMA,
            required_fields=["snapshot.cash_total_cents", "snapshot.nlv_total_cents", "snapshot.account_id"],
            join_fields=["snapshot.account_id"],
            downstream_stage_blockers=["CASH", "ACCOUNTING"],
        ),
        "broker_statement_normalized": _family_probe_from_json_paths(
            repo_root=repo_root,
            family="broker_statement_normalized",
            paths=[paths["broker_statement_normalized"]] if paths["broker_statement_normalized"].exists() else [],
            schema_relpath=None,
            required_fields=["day_utc", "account_id", "currency", "cash_end", "positions"],
            join_fields=["account_id", "positions"],
            downstream_stage_blockers=["MARKS", "ACCOUNTING"],
        ),
        "broker_marks": _family_probe_from_json_paths(
            repo_root=repo_root,
            family="broker_marks",
            paths=[paths["broker_marks"]] if paths["broker_marks"].exists() else [],
            schema_relpath=None,
            required_fields=["day_utc", "currency", "cash_end", "marks"],
            join_fields=["marks"],
            downstream_stage_blockers=["MARKS", "ACCOUNTING"],
        ),
        "accounting_nav": _family_probe_from_json_paths(
            repo_root=repo_root,
            family="accounting_nav",
            paths=[paths["accounting_nav"]] if paths["accounting_nav"].exists() else [],
            schema_relpath=ACCOUNTING_SCHEMA,
            required_fields=["nav", "history"],
            join_fields=["nav"],
            downstream_stage_blockers=["ACCOUNTING"],
        ),
        "exit_reconciliation": _family_probe_from_json_paths(
            repo_root=repo_root,
            family="exit_reconciliation",
            paths=[paths["exit_reconciliation"]] if paths["exit_reconciliation"].exists() else [],
            schema_relpath=EXIT_RECON_SCHEMA,
            required_fields=["status", "obligations"],
            join_fields=["obligations"],
            downstream_stage_blockers=["EXIT_RECONCILIATION"],
        ),
    }
    return probes


def _find_broker_raw_input(repo_root: Path, day_utc: str) -> Optional[Path]:
    root = (repo_root / "constellation_2" / "operator_inputs").resolve()
    if not root.exists() or not root.is_dir():
        return None
    matches = sorted([p.resolve() for p in root.rglob("*.json") if day_utc in str(p) and "broker" in p.name.lower()])
    return matches[0] if matches else None


def build_dependency_statuses(repo_root: Path, truth_root: Path, day_utc: str, facts: Dict[str, Any]) -> List[Dict[str, Any]]:
    registry = _read_registry(repo_root, DEPENDENCY_REGISTRY_RELPATH)
    entries = registry.get("dependencies")
    if not isinstance(entries, list):
        raise SystemExit(f"FAIL: invalid dependency registry entries: {DEPENDENCY_REGISTRY_RELPATH}")
    statuses: List[Dict[str, Any]] = []
    market_manifest = (truth_root / "market_data_snapshot_v1" / "dataset_manifest.json").resolve()
    raw_broker_input = _find_broker_raw_input(repo_root, day_utc)
    submission_dirs = [Path(p) for p in facts.get("submission_dirs", [])]
    for entry in entries:
        if not isinstance(entry, dict):
            continue
        dependency_id = str(entry.get("dependency_id") or "").strip()
        blocking = bool(entry.get("blocking", False))
        resolved_path: Optional[str] = None
        status = "READY"
        reasons: List[str] = []
        if dependency_id == "market_data_snapshot_for_real_intent_generation":
            resolved_path = str(market_manifest)
            if not market_manifest.exists():
                status = "MISSING_BLOCKING_DEPENDENCY"
                reasons.append("dataset_manifest_missing")
        elif dependency_id == "operator_statement_input":
            resolved_path = facts.get("operator_statement_path")
            if not Path(str(resolved_path)).exists():
                status = "MISSING_BLOCKING_DEPENDENCY"
                reasons.append("operator_statement_missing")
        elif dependency_id == "raw_broker_statement_input":
            resolved_path = None if raw_broker_input is None else str(raw_broker_input)
            if raw_broker_input is None:
                status = "MISSING_BLOCKING_DEPENDENCY"
                reasons.append("raw_broker_statement_input_not_discoverable")
        elif dependency_id == "broker_marks_override_input":
            resolved_path = facts.get("broker_marks_override_path")
            if not Path(str(resolved_path)).exists():
                status = "PARTIAL_DEPENDENCY_SET"
                reasons.append("override_not_present")
        elif dependency_id == "submission_identity_bundle":
            resolved_path = str((truth_root / "execution_evidence_v1" / "submissions" / day_utc).resolve())
            if not submission_dirs:
                status = "MISSING_BLOCKING_DEPENDENCY"
                reasons.append("submission_dirs_missing")
            else:
                incomplete = False
                for subdir in submission_dirs:
                    for name in ("broker_submission_record.v2.json", "execution_event_record.v1.json", "equity_order_plan.v1.json"):
                        if not (subdir / name).exists():
                            incomplete = True
                if incomplete:
                    status = "PARTIAL_DEPENDENCY_SET"
                    reasons.append("submission_identity_bundle_incomplete")
        elif dependency_id == "execution_stream_prerequisites":
            resolved_path = str((truth_root / "execution_stream_v1" / day_utc).resolve())
            if not submission_dirs:
                status = "MISSING_BLOCKING_DEPENDENCY"
                reasons.append("no_submission_dirs_for_execution_stream")
            else:
                stream_root = Path(resolved_path)
                if not stream_root.exists() or not any(stream_root.glob("*.execution_event_stream_record.v1.json")):
                    status = "PARTIAL_DEPENDENCY_SET"
                    reasons.append("execution_stream_not_materialized")
        else:
            status = "PARTIAL_DEPENDENCY_SET"
            reasons.append("unhandled_dependency_id")
        statuses.append(
            {
                "dependency_id": dependency_id,
                "dependency_version": int(entry.get("dependency_version") or 1),
                "dependency_type": entry.get("dependency_type"),
                "blocking": blocking,
                "status": status,
                "resolved_path": resolved_path,
                "reasons": sorted(set(reasons)),
                "downstream_consumers": entry.get("downstream_consumers") or [],
                "expected_source_path": entry.get("expected_source_path"),
                "generation_path": entry.get("generation_path"),
            }
        )
    return statuses


def build_execution_evidence_normalization_doc(
    *,
    repo_root: Path,
    truth_root: Path,
    day_utc: str,
    produced_utc: str,
    run_scope: Dict[str, Any],
    dependency_statuses: List[Dict[str, Any]],
    payload_probes: Dict[str, FamilyProbe],
    facts: Dict[str, Any],
) -> Dict[str, Any]:
    normalized = []
    dependency_by_id = {row["dependency_id"]: row["status"] for row in dependency_statuses}
    for key in [
        "intents_day_rollup_linkage",
        "intent_snapshot",
        "authorization",
        "broker_submission_record",
        "execution_event_record",
        "execution_stream",
        "fill_ledger_input_bundle",
        "fill_ledger",
        "positions_snapshot",
        "cash_ledger_snapshot",
        "broker_statement_normalized",
        "broker_marks",
        "accounting_nav",
        "exit_reconciliation",
    ]:
        probe = payload_probes[key]
        normalized.append(
            {
                "artifact_family": probe.family,
                "observed_paths": list(probe.observed_paths),
                "record_count": probe.record_count,
                "dependency_status": _dependency_status_for_family(probe.family, dependency_by_id),
                "schema_validation_status": probe.schema_validation_status,
                "completeness_status": probe.completeness_status,
                "missing_required_fields": list(probe.missing_required_fields),
                "missing_join_critical_fields": list(probe.missing_join_critical_fields),
                "downstream_stage_blockers": list(probe.downstream_stage_blockers),
                "notes": list(probe.notes),
            }
        )
    integrity_status = "OK"
    if any(row["dependency_status"] != "READY" for row in normalized if row["dependency_status"] is not None):
        integrity_status = "DEPENDENCY_BLOCKED"
    if any(row["completeness_status"] not in {"COMPLETE"} for row in normalized):
        integrity_status = "PAYLOAD_INCOMPLETE"
    return {
        "schema_id": "execution_evidence_normalization_v1",
        "schema_version": 1,
        "day_utc": day_utc,
        "produced_utc": produced_utc,
        "execution_completion_ruleset_id": EXECUTION_COMPLETION_RULESET_ID,
        "execution_completion_ruleset_version": EXECUTION_COMPLETION_RULESET_VERSION,
        "dependency_registry_id": DEPENDENCY_REGISTRY_ID,
        "dependency_registry_version": DEPENDENCY_REGISTRY_VERSION,
        "payload_contracts_id": PAYLOAD_CONTRACTS_ID,
        "payload_contracts_version": PAYLOAD_CONTRACTS_VERSION,
        "run_scope": run_scope,
        "normalized_evidence": normalized,
        "evidence_refs": sorted(set(_evidence_refs(facts))),
        "integrity_status": integrity_status,
    }


def _dependency_status_for_family(family: str, dependency_by_id: Dict[str, str]) -> Optional[str]:
    mapping = {
        "intent_snapshot": dependency_by_id.get("market_data_snapshot_for_real_intent_generation"),
        "broker_statement_normalized": dependency_by_id.get("raw_broker_statement_input"),
        "broker_marks": dependency_by_id.get("broker_marks_override_input"),
        "cash_ledger_snapshot": dependency_by_id.get("operator_statement_input"),
        "broker_submission_record": dependency_by_id.get("submission_identity_bundle"),
        "execution_event_record": dependency_by_id.get("submission_identity_bundle"),
        "execution_stream": dependency_by_id.get("execution_stream_prerequisites"),
        "fill_ledger_input_bundle": dependency_by_id.get("execution_stream_prerequisites"),
        "fill_ledger": dependency_by_id.get("execution_stream_prerequisites"),
    }
    return mapping.get(family)


def build_lifecycle_progression_status_doc(
    *,
    truth_root: Path,
    day_utc: str,
    produced_utc: str,
    run_scope: Dict[str, Any],
    dependency_statuses: List[Dict[str, Any]],
    payload_probes: Dict[str, FamilyProbe],
    facts: Dict[str, Any],
) -> Dict[str, Any]:
    dep = {row["dependency_id"]: row for row in dependency_statuses}
    stage_rows: List[Dict[str, Any]] = []
    prior_complete = True
    for stage in LIFECYCLE_STAGES:
        row: Dict[str, Any]
        if stage == "INTENT_EMITTED":
            intent_probe = payload_probes["intent_snapshot"]
            rollup_probe = payload_probes["intents_day_rollup_linkage"]
            if intent_probe.completeness_status == "COMPLETE":
                row = _stage_row(stage, "COMPLETE", [], [], _maybe_writer(stage, dep))
            elif dep["market_data_snapshot_for_real_intent_generation"]["status"] != "READY":
                row = _stage_row(stage, "BLOCKED_MISSING_DEPENDENCY", ["market_data_snapshot_for_real_intent_generation"], list(intent_probe.missing_required_fields or rollup_probe.missing_join_critical_fields), _maybe_writer(stage, dep))
            elif rollup_probe.completeness_status == "AGGREGATE_ONLY_NO_LINKAGE":
                row = _stage_row(stage, "BLOCKED_PAYLOAD_INCOMPLETE", [], list(rollup_probe.missing_join_critical_fields), _maybe_writer(stage, dep))
            else:
                row = _stage_row(stage, "BLOCKED_MISSING_ARTIFACT", [], list(intent_probe.missing_required_fields), _maybe_writer(stage, dep))
        elif stage == "AUTHORIZATION_COMPLETE":
            probe = payload_probes["authorization"]
            if not prior_complete:
                row = _stage_row(stage, "BLOCKED_UPSTREAM", [], [], _maybe_writer(stage, dep))
            elif probe.completeness_status == "COMPLETE":
                row = _stage_row(stage, "COMPLETE", [], [], _maybe_writer(stage, dep))
            else:
                row = _stage_row(stage, "BLOCKED_PAYLOAD_INCOMPLETE", [], list(probe.missing_required_fields + probe.missing_join_critical_fields), _maybe_writer(stage, dep))
        elif stage == "SUBMISSION_COMPLETE":
            probe = payload_probes["broker_submission_record"]
            if not prior_complete:
                row = _stage_row(stage, "BLOCKED_UPSTREAM", [], [], _maybe_writer(stage, dep))
            elif probe.completeness_status == "COMPLETE":
                row = _stage_row(stage, "COMPLETE", [], [], _maybe_writer(stage, dep))
            else:
                dep_failures = []
                if dep["submission_identity_bundle"]["status"] != "READY":
                    dep_failures.append("submission_identity_bundle")
                row = _stage_row(stage, "BLOCKED_PAYLOAD_INCOMPLETE", dep_failures, list(probe.missing_required_fields + probe.missing_join_critical_fields), _maybe_writer(stage, dep))
        elif stage == "EXECUTION_STREAM_COMPLETE":
            probe = payload_probes["execution_stream"]
            if not prior_complete:
                row = _stage_row(stage, "BLOCKED_UPSTREAM", [], [], _maybe_writer(stage, dep))
            elif probe.completeness_status == "COMPLETE":
                row = _stage_row(stage, "COMPLETE", [], [], _maybe_writer(stage, dep))
            else:
                dep_failures = []
                if dep["execution_stream_prerequisites"]["status"] != "READY":
                    dep_failures.append("execution_stream_prerequisites")
                row = _stage_row(stage, "BLOCKED_MISSING_ARTIFACT" if probe.record_count == 0 else "BLOCKED_PAYLOAD_INCOMPLETE", dep_failures, list(probe.missing_required_fields + probe.missing_join_critical_fields), _maybe_writer(stage, dep))
        else:
            fill_probe = payload_probes["fill_ledger"]
            fill_input_probe = payload_probes["fill_ledger_input_bundle"]
            if not prior_complete:
                row = _stage_row(stage, "BLOCKED_UPSTREAM", [], [], _maybe_writer(stage, dep))
            elif fill_probe.completeness_status == "COMPLETE":
                row = _stage_row(stage, "COMPLETE", [], [], _maybe_writer(stage, dep))
            elif fill_input_probe.completeness_status in {"INPUTS_TOO_SPARSE", "SCHEMA_MISMATCH"}:
                row = _stage_row(stage, "BLOCKED_PAYLOAD_INCOMPLETE", ["execution_stream_prerequisites"], list(fill_input_probe.missing_required_fields + fill_input_probe.missing_join_critical_fields), _maybe_writer(stage, dep))
            else:
                row = _stage_row(stage, "BLOCKED_MISSING_ARTIFACT", ["execution_stream_prerequisites"], list(fill_probe.missing_required_fields), _maybe_writer(stage, dep))
        prior_complete = row["stage_status"] == "COMPLETE"
        stage_rows.append(row)
    first_break_stage = next((row["stage_name"] for row in stage_rows if row["stage_status"] != "COMPLETE"), None)
    integrity_status = "OK" if first_break_stage is None else "PAYLOAD_INCOMPLETE"
    return {
        "schema_id": "lifecycle_progression_status_v1",
        "schema_version": 1,
        "day_utc": day_utc,
        "produced_utc": produced_utc,
        "execution_completion_ruleset_id": EXECUTION_COMPLETION_RULESET_ID,
        "execution_completion_ruleset_version": EXECUTION_COMPLETION_RULESET_VERSION,
        "run_scope": run_scope,
        "stages": stage_rows,
        "first_break_stage": first_break_stage,
        "integrity_status": integrity_status,
        "evidence_refs": sorted(set(_evidence_refs(facts))),
    }


def _stage_row(stage_name: str, stage_status: str, dependency_failures: List[str], missing_fields: List[str], blocked_writer: Optional[str]) -> Dict[str, Any]:
    writers = [] if blocked_writer is None else [blocked_writer]
    return {
        "stage_name": stage_name,
        "stage_status": stage_status,
        "dependency_failures": sorted(set(dependency_failures)),
        "missing_fields": sorted(set(missing_fields)),
        "blocked_writers": writers,
    }


def _maybe_writer(stage_name: str, dependency_statuses: Dict[str, Dict[str, Any]]) -> Optional[str]:
    mapping = {
        "INTENT_EMITTED": "constellation_2/phaseI/trend_eq_primary/run/run_trend_eq_primary_intents_day_v1.py",
        "AUTHORIZATION_COMPLETE": "ops/tools/run_authorization_artifacts_day_v1.py",
        "SUBMISSION_COMPLETE": "constellation_2/phaseF/execution_evidence/run/run_execution_evidence_truth_day_v1.py",
        "EXECUTION_STREAM_COMPLETE": "ops/tools/run_execution_stream_snapshot_day_v1.py",
        "FILL_COMPLETE": "ops/tools/run_fill_ledger_day_v1.py",
    }
    return mapping.get(stage_name)


def build_economic_finalization_status_doc(
    *,
    day_utc: str,
    produced_utc: str,
    run_scope: Dict[str, Any],
    lifecycle_doc: Dict[str, Any],
    dependency_statuses: List[Dict[str, Any]],
    payload_probes: Dict[str, FamilyProbe],
    facts: Dict[str, Any],
) -> Dict[str, Any]:
    dep = {row["dependency_id"]: row for row in dependency_statuses}
    fill_complete = any(row["stage_name"] == "FILL_COMPLETE" and row["stage_status"] == "COMPLETE" for row in lifecycle_doc.get("stages", []))
    stage_rows: List[Dict[str, Any]] = []
    if not fill_complete:
        overall = "NOT_READY_FOR_ECONOMIC_FINALIZATION"
        for stage_name in ECONOMIC_STAGES:
            stage_rows.append({
                "stage_name": stage_name,
                "stage_status": "BLOCKED_UPSTREAM",
                "blocking_reasons": ["FILL_COMPLETE_NOT_REACHED"],
                "blocked_writers": [_economic_writer(stage_name)],
            })
    else:
        overall = "ECONOMIC_FINALIZATION_COMPLETE"
        for stage_name in ECONOMIC_STAGES:
            status = "BLOCKED_MISSING_ARTIFACT"
            reasons: List[str] = []
            if stage_name == "POSITIONS":
                probe = payload_probes["positions_snapshot"]
                if probe.completeness_status == "COMPLETE":
                    status = "COMPLETE"
                else:
                    reasons.extend(probe.missing_required_fields)
                    overall = "READY_FOR_POSITIONS"
            elif stage_name == "CASH":
                probe = payload_probes["cash_ledger_snapshot"]
                if probe.completeness_status == "COMPLETE":
                    status = "COMPLETE"
                else:
                    reasons.extend(probe.missing_required_fields)
                    if overall == "ECONOMIC_FINALIZATION_COMPLETE":
                        overall = "READY_FOR_CASH"
            elif stage_name == "MARKS":
                probe = payload_probes["broker_marks"]
                if probe.completeness_status == "COMPLETE":
                    status = "COMPLETE"
                else:
                    reasons.extend(probe.missing_required_fields)
                    if payload_probes["broker_statement_normalized"].record_count == 0:
                        reasons.append("broker_statement_normalized_missing")
                    if dep.get("broker_marks_override_input", {}).get("status") != "READY":
                        reasons.append("broker_marks_override_not_present")
                    if overall == "ECONOMIC_FINALIZATION_COMPLETE":
                        overall = "READY_FOR_MARKS"
            elif stage_name == "ACCOUNTING":
                probe = payload_probes["accounting_nav"]
                if probe.completeness_status == "COMPLETE":
                    status = "COMPLETE"
                else:
                    reasons.extend(probe.missing_required_fields)
                    if overall == "ECONOMIC_FINALIZATION_COMPLETE":
                        overall = "READY_FOR_ACCOUNTING"
            else:
                probe = payload_probes["exit_reconciliation"]
                if probe.completeness_status == "COMPLETE":
                    status = "COMPLETE"
                else:
                    reasons.extend(probe.missing_required_fields)
                    if overall == "ECONOMIC_FINALIZATION_COMPLETE":
                        overall = "READY_FOR_EXIT_RECONCILIATION"
            stage_rows.append({
                "stage_name": stage_name,
                "stage_status": status,
                "blocking_reasons": sorted(set(reasons)),
                "blocked_writers": [] if status == "COMPLETE" else [_economic_writer(stage_name)],
            })
    integrity_status = "OK" if overall == "ECONOMIC_FINALIZATION_COMPLETE" else "ECONOMIC_FINALIZATION_INCOMPLETE"
    return {
        "schema_id": "economic_finalization_status_v1",
        "schema_version": 1,
        "day_utc": day_utc,
        "produced_utc": produced_utc,
        "execution_completion_ruleset_id": EXECUTION_COMPLETION_RULESET_ID,
        "execution_completion_ruleset_version": EXECUTION_COMPLETION_RULESET_VERSION,
        "run_scope": run_scope,
        "economic_finalization_status": overall,
        "stage_statuses": stage_rows,
        "integrity_status": integrity_status,
        "evidence_refs": sorted(set(_evidence_refs(facts))),
    }


def _economic_writer(stage_name: str) -> str:
    mapping = {
        "POSITIONS": "constellation_2/phaseF/positions/run/run_positions_snapshot_day_v2.py",
        "CASH": "constellation_2/phaseF/cash_ledger/run/run_cash_ledger_snapshot_day_v1.py",
        "MARKS": "ops/tools/run_broker_marks_snapshot_day_v1.py",
        "ACCOUNTING": "ops/tools/run_accounting_nav_v2_day_v1.py",
        "EXIT_RECONCILIATION": "constellation_2/phaseI/exit_reconciliation/run/run_exit_reconciliation_day_v1.py",
    }
    return mapping[stage_name]


def build_execution_completion_gap_report_doc(
    *,
    day_utc: str,
    produced_utc: str,
    run_scope: Dict[str, Any],
    dependency_statuses: List[Dict[str, Any]],
    payload_probes: Dict[str, FamilyProbe],
    lifecycle_doc: Dict[str, Any],
    economic_doc: Dict[str, Any],
    facts: Dict[str, Any],
) -> Dict[str, Any]:
    lifecycle_rows = lifecycle_doc.get("stages", [])
    first_break_stage = lifecycle_doc.get("first_break_stage")
    if first_break_stage is None:
        for row in economic_doc.get("stage_statuses", []):
            if row.get("stage_status") != "COMPLETE":
                first_break_stage = row.get("stage_name")
                break
    missing_dependencies = [
        {
            "dependency_id": row["dependency_id"],
            "status": row["status"],
            "expected_source_path": row.get("expected_source_path"),
            "generation_path": row.get("generation_path"),
            "resolved_path": row.get("resolved_path"),
            "reasons": row.get("reasons") or [],
        }
        for row in dependency_statuses
        if row["status"] != "READY" and bool(row.get("blocking", False))
    ]
    missing_required_fields = []
    for probe in payload_probes.values():
        for field in probe.missing_required_fields:
            missing_required_fields.append({"artifact_family": probe.family, "field": field})
        for field in probe.missing_join_critical_fields:
            missing_required_fields.append({"artifact_family": probe.family, "field": field})
    blocked_writers: List[str] = []
    for row in lifecycle_rows:
        blocked_writers.extend(row.get("blocked_writers") or [])
    for row in economic_doc.get("stage_statuses", []):
        blocked_writers.extend(row.get("blocked_writers") or [])
    blocked_writers = sorted(set(writer for writer in blocked_writers if writer))
    next_satisfiable_writer = None
    if first_break_stage == "AUTHORIZATION_COMPLETE" and payload_probes["authorization"].record_count == 0:
        next_satisfiable_writer = "ops/tools/run_authorization_artifacts_day_v1.py"
    gap_status = "BLOCKED"
    if first_break_stage is None:
        gap_status = "COMPLETE"
    return {
        "schema_id": "execution_completion_gap_report_v1",
        "schema_version": 1,
        "day_utc": day_utc,
        "produced_utc": produced_utc,
        "execution_completion_ruleset_id": EXECUTION_COMPLETION_RULESET_ID,
        "execution_completion_ruleset_version": EXECUTION_COMPLETION_RULESET_VERSION,
        "run_scope": run_scope,
        "target_day_utc": day_utc,
        "dependency_statuses": dependency_statuses,
        "payload_completeness_results": [_family_probe_to_doc(row) for row in payload_probes.values()],
        "lifecycle_stage_statuses": lifecycle_rows,
        "economic_finalization_status": economic_doc.get("economic_finalization_status"),
        "first_break_stage": first_break_stage,
        "missing_dependencies": missing_dependencies,
        "missing_required_fields": missing_required_fields,
        "blocked_writers": blocked_writers,
        "next_satisfiable_writer": next_satisfiable_writer,
        "evidence_refs": sorted(set(_evidence_refs(facts))),
        "integrity_status": "OK" if gap_status == "COMPLETE" else "BLOCKED",
    }


def _family_probe_to_doc(probe: FamilyProbe) -> Dict[str, Any]:
    return {
        "artifact_family": probe.family,
        "observed_paths": list(probe.observed_paths),
        "record_count": probe.record_count,
        "schema_validation_status": probe.schema_validation_status,
        "completeness_status": probe.completeness_status,
        "missing_required_fields": list(probe.missing_required_fields),
        "missing_join_critical_fields": list(probe.missing_join_critical_fields),
        "downstream_stage_blockers": list(probe.downstream_stage_blockers),
        "notes": list(probe.notes),
    }


def _evidence_refs(facts: Dict[str, Any]) -> List[str]:
    refs: List[str] = []
    day_rollup_probe: JsonProbe = facts["day_rollup_probe"]
    if day_rollup_probe.present:
        refs.append(str(day_rollup_probe.path))
    refs.extend(facts.get("intent_snapshot_paths", []))
    refs.extend(facts.get("authorization_paths", []))
    refs.extend(facts.get("submission_dirs", []))
    refs.extend(facts.get("execution_stream_paths", []))
    refs.extend(facts.get("fill_ledger_paths", []))
    refs.extend(facts.get("explanatory_refs", []))
    for key in ("positions_snapshot", "cash_ledger_snapshot", "broker_statement_normalized", "broker_marks", "accounting_nav", "exit_reconciliation"):
        p = facts["paths"][key]
        if p.exists():
            refs.append(str(p))
    return sorted(set(refs))


def build_execution_truth_docs(repo_root: Path, truth_root: Path, day_utc: str, produced_utc: str) -> Dict[str, Dict[str, Any]]:
    facts = collect_execution_truth_facts(repo_root, truth_root, day_utc)
    dependency_statuses = build_dependency_statuses(repo_root, truth_root, day_utc, facts)
    payload_probes = build_payload_probes(repo_root, truth_root, day_utc, facts)
    run_scope = _derive_run_scope(repo_root, truth_root, day_utc, facts)
    normalization = build_execution_evidence_normalization_doc(
        repo_root=repo_root,
        truth_root=truth_root,
        day_utc=day_utc,
        produced_utc=produced_utc,
        run_scope=run_scope,
        dependency_statuses=dependency_statuses,
        payload_probes=payload_probes,
        facts=facts,
    )
    lifecycle = build_lifecycle_progression_status_doc(
        truth_root=truth_root,
        day_utc=day_utc,
        produced_utc=produced_utc,
        run_scope=run_scope,
        dependency_statuses=dependency_statuses,
        payload_probes=payload_probes,
        facts=facts,
    )
    economic = build_economic_finalization_status_doc(
        day_utc=day_utc,
        produced_utc=produced_utc,
        run_scope=run_scope,
        lifecycle_doc=lifecycle,
        dependency_statuses=dependency_statuses,
        payload_probes=payload_probes,
        facts=facts,
    )
    gap = build_execution_completion_gap_report_doc(
        day_utc=day_utc,
        produced_utc=produced_utc,
        run_scope=run_scope,
        dependency_statuses=dependency_statuses,
        payload_probes=payload_probes,
        lifecycle_doc=lifecycle,
        economic_doc=economic,
        facts=facts,
    )
    validate_against_repo_schema_v1(normalization, repo_root, NORMALIZATION_SCHEMA)
    validate_against_repo_schema_v1(lifecycle, repo_root, LIFECYCLE_SCHEMA)
    validate_against_repo_schema_v1(economic, repo_root, ECONOMIC_SCHEMA)
    validate_against_repo_schema_v1(gap, repo_root, GAP_SCHEMA)
    return {
        "normalization": normalization,
        "lifecycle": lifecycle,
        "economic": economic,
        "gap": gap,
    }


def _internal_failure_docs(repo_root: Path, truth_root: Path, day_utc: str, produced_utc: str, exc: Exception) -> Dict[str, Dict[str, Any]]:
    run_scope = _derive_scope_from_truth_root(truth_root, day_utc)
    evidence_refs = []
    error_text = f"{type(exc).__name__}: {exc}"
    tb = traceback.format_exc(limit=5)
    normalization = {
        "schema_id": "execution_evidence_normalization_v1",
        "schema_version": 1,
        "day_utc": day_utc,
        "produced_utc": produced_utc,
        "execution_completion_ruleset_id": EXECUTION_COMPLETION_RULESET_ID,
        "execution_completion_ruleset_version": EXECUTION_COMPLETION_RULESET_VERSION,
        "dependency_registry_id": DEPENDENCY_REGISTRY_ID,
        "dependency_registry_version": DEPENDENCY_REGISTRY_VERSION,
        "payload_contracts_id": PAYLOAD_CONTRACTS_ID,
        "payload_contracts_version": PAYLOAD_CONTRACTS_VERSION,
        "run_scope": run_scope,
        "normalized_evidence": [],
        "evidence_refs": evidence_refs,
        "integrity_status": "INTERNAL_FAILURE",
    }
    lifecycle = {
        "schema_id": "lifecycle_progression_status_v1",
        "schema_version": 1,
        "day_utc": day_utc,
        "produced_utc": produced_utc,
        "execution_completion_ruleset_id": EXECUTION_COMPLETION_RULESET_ID,
        "execution_completion_ruleset_version": EXECUTION_COMPLETION_RULESET_VERSION,
        "run_scope": run_scope,
        "stages": [
            {
                "stage_name": stage,
                "stage_status": "INTERNAL_FAILURE",
                "dependency_failures": [],
                "missing_fields": [],
                "blocked_writers": [],
            }
            for stage in LIFECYCLE_STAGES
        ],
        "first_break_stage": "INTERNAL_FAILURE",
        "integrity_status": "INTERNAL_FAILURE",
        "evidence_refs": evidence_refs,
    }
    economic = {
        "schema_id": "economic_finalization_status_v1",
        "schema_version": 1,
        "day_utc": day_utc,
        "produced_utc": produced_utc,
        "execution_completion_ruleset_id": EXECUTION_COMPLETION_RULESET_ID,
        "execution_completion_ruleset_version": EXECUTION_COMPLETION_RULESET_VERSION,
        "run_scope": run_scope,
        "economic_finalization_status": "INTERNAL_FAILURE",
        "stage_statuses": [
            {
                "stage_name": stage,
                "stage_status": "INTERNAL_FAILURE",
                "blocking_reasons": [error_text],
                "blocked_writers": [],
            }
            for stage in ECONOMIC_STAGES
        ],
        "integrity_status": "INTERNAL_FAILURE",
        "evidence_refs": evidence_refs,
    }
    gap = {
        "schema_id": "execution_completion_gap_report_v1",
        "schema_version": 1,
        "day_utc": day_utc,
        "produced_utc": produced_utc,
        "execution_completion_ruleset_id": EXECUTION_COMPLETION_RULESET_ID,
        "execution_completion_ruleset_version": EXECUTION_COMPLETION_RULESET_VERSION,
        "run_scope": run_scope,
        "target_day_utc": day_utc,
        "dependency_statuses": [],
        "payload_completeness_results": [],
        "lifecycle_stage_statuses": lifecycle["stages"],
        "economic_finalization_status": "INTERNAL_FAILURE",
        "first_break_stage": "INTERNAL_FAILURE",
        "missing_dependencies": [],
        "missing_required_fields": [{"artifact_family": "internal", "field": error_text}],
        "blocked_writers": [],
        "next_satisfiable_writer": None,
        "evidence_refs": evidence_refs + [tb],
        "integrity_status": "INTERNAL_FAILURE",
    }
    validate_against_repo_schema_v1(normalization, repo_root, NORMALIZATION_SCHEMA)
    validate_against_repo_schema_v1(lifecycle, repo_root, LIFECYCLE_SCHEMA)
    validate_against_repo_schema_v1(economic, repo_root, ECONOMIC_SCHEMA)
    validate_against_repo_schema_v1(gap, repo_root, GAP_SCHEMA)
    return {"normalization": normalization, "lifecycle": lifecycle, "economic": economic, "gap": gap}


def write_execution_truth_plane(*, repo_root: Path, truth_root: Path, day_utc: str, produced_utc: str) -> Dict[str, RefreshWriteResultV1]:
    day_utc = _require_day_utc(day_utc)
    produced_utc = _require_produced_utc(day_utc, produced_utc)
    try:
        docs = build_execution_truth_docs(repo_root=repo_root, truth_root=truth_root, day_utc=day_utc, produced_utc=produced_utc)
    except Exception as exc:
        docs = _internal_failure_docs(repo_root=repo_root, truth_root=truth_root, day_utc=day_utc, produced_utc=produced_utc, exc=exc)
    targets = {
        "normalization": (truth_root / "reports" / "execution_evidence_normalization_v1" / day_utc / "execution_evidence_normalization.v1.json").resolve(),
        "lifecycle": (truth_root / "reports" / "lifecycle_progression_status_v1" / day_utc / "lifecycle_progression_status.v1.json").resolve(),
        "economic": (truth_root / "reports" / "economic_finalization_status_v1" / day_utc / "economic_finalization_status.v1.json").resolve(),
        "gap": (truth_root / "reports" / "execution_completion_gap_report_v1" / day_utc / "execution_completion_gap_report.v1.json").resolve(),
    }
    schemas = {
        "normalization": ("execution_evidence_normalization_v1", 1),
        "lifecycle": ("lifecycle_progression_status_v1", 1),
        "economic": ("economic_finalization_status_v1", 1),
        "gap": ("execution_completion_gap_report_v1", 1),
    }
    writes: Dict[str, RefreshWriteResultV1] = {}
    for key in ("normalization", "lifecycle", "economic", "gap"):
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
