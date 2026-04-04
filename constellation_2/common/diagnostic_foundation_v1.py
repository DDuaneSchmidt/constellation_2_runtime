from __future__ import annotations

import json
import re
import subprocess
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from constellation_2.phaseD.lib.canon_json_v1 import canonical_json_bytes_v1
from constellation_2.phaseD.lib.validate_against_schema_v1 import validate_against_repo_schema_v1
from constellation_2.phaseF.accounting.lib.day_artifact_refresh_v1 import (
    RefreshWriteResultV1,
    write_day_artifact_refreshable_v1,
)


DIAGNOSTIC_RULESET_ID = "BATCH1_FOUNDATION_RULESET"
DIAGNOSTIC_RULESET_VERSION = 1
FLOW_RULESET_ID = "ACTIVITY_FLOW_DIAGNOSTICS_RULESET"
FLOW_RULESET_VERSION = 1
REGRESSION_RULESET_ID = "RUNTIME_REGRESSION_ANALYTICS_RULESET"
REGRESSION_RULESET_VERSION = 1
REGRESSION_COMPARISON_MODE = "PRIOR_DAY"

ROOT_CAUSE_SCHEMA = "governance/04_DATA/SCHEMAS/C2/REPORTS/root_cause.v1.schema.json"
INTENT_ABSENCE_SCHEMA = "governance/04_DATA/SCHEMAS/C2/REPORTS/intent_absence_analysis.v1.schema.json"
DECISION_LEDGER_SCHEMA = "governance/04_DATA/SCHEMAS/C2/REPORTS/diagnostic_decision_ledger.v1.schema.json"
ACTIVITY_FLOW_SCHEMA = "governance/04_DATA/SCHEMAS/C2/REPORTS/activity_flow_diagnostics.v1.schema.json"
REGRESSION_SCHEMA = "governance/04_DATA/SCHEMAS/C2/REPORTS/runtime_regression_analytics.v1.schema.json"

DATE_RE = re.compile(r"^\d{4}-\d{2}-\d{2}$")


@dataclass(frozen=True)
class AttemptSelection:
    pointer_index_path: Path
    pointer_entry: Dict[str, Any]
    attempt_manifest_path: Path
    attempt_manifest: Dict[str, Any]
    verdict_path: Path
    verdict: Dict[str, Any]


def _require_day_utc(day_utc: str) -> str:
    day = str(day_utc or "").strip()
    if not DATE_RE.match(day):
        raise SystemExit(f"FAIL: bad day_utc: {day!r}")
    return day


def _require_produced_utc(day_utc: str, produced_utc: str) -> str:
    expected = f"{day_utc}T00:00:00Z"
    got = str(produced_utc or "").strip()
    if got != expected:
        raise SystemExit(f"FAIL: produced_utc_must_equal_day_marker expected={expected!r} got={got!r}")
    return got


def _git_sha(repo_root: Path) -> str:
    try:
        out = subprocess.check_output(["/usr/bin/git", "rev-parse", "HEAD"], cwd=str(repo_root))
        return out.decode("utf-8").strip()
    except Exception:
        return "UNKNOWN"


def _read_json_obj(path: Path) -> Dict[str, Any]:
    if not path.exists() or not path.is_file():
        raise SystemExit(f"FAIL: missing_or_not_file: {path}")
    try:
        obj = json.loads(path.read_text(encoding="utf-8"))
    except Exception as exc:
        raise SystemExit(f"FAIL: json_parse_failed path={path} err={exc!r}") from exc
    if not isinstance(obj, dict):
        raise SystemExit(f"FAIL: top_level_not_object: {path}")
    return obj


def _safe_read_json_obj(path: Path) -> Optional[Dict[str, Any]]:
    if not path.exists() or not path.is_file():
        return None
    try:
        obj = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return None
    return obj if isinstance(obj, dict) else None


def _sha256_file(path: Path) -> str:
    import hashlib

    h = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def _write_refreshable_json(
    *,
    repo_root: Path,
    out_path: Path,
    doc: Dict[str, Any],
    schema_relpath: str,
    expected_schema_id: str,
    expected_schema_version: Any,
    expected_day_utc: str,
) -> RefreshWriteResultV1:
    validate_against_repo_schema_v1(doc, repo_root, schema_relpath)
    payload = canonical_json_bytes_v1(doc) + b"\n"
    return write_day_artifact_refreshable_v1(
        path=out_path,
        data=payload,
        expected_day_utc=expected_day_utc,
        expected_schema_id=expected_schema_id,
        expected_schema_version=expected_schema_version,
        preserve_statuses=(),
    )


def _count_intents_from_rollup(rollup: Optional[Dict[str, Any]]) -> Optional[int]:
    if not isinstance(rollup, dict):
        return None
    engines = rollup.get("engines")
    if not isinstance(engines, list):
        return None
    total = 0
    for row in engines:
        if not isinstance(row, dict):
            continue
        count = row.get("intent_count")
        if not isinstance(count, int):
            return None
        total += count
    return total


def _read_latest_pointer_entry(pointer_index_path: Path) -> Dict[str, Any]:
    if not pointer_index_path.exists() or not pointer_index_path.is_file():
        raise SystemExit(f"FAIL: missing pointer index: {pointer_index_path}")
    best: Optional[Dict[str, Any]] = None
    best_seq = -1
    for line in pointer_index_path.read_text(encoding="utf-8").splitlines():
        raw = line.strip()
        if not raw:
            continue
        try:
            obj = json.loads(raw)
        except Exception as exc:
            raise SystemExit(f"FAIL: invalid pointer index jsonl: {pointer_index_path}: {exc!r}") from exc
        if not isinstance(obj, dict):
            continue
        try:
            seq = int(obj.get("pointer_seq"))
        except Exception:
            continue
        if seq > best_seq:
            best_seq = seq
            best = obj
    if best is None:
        raise SystemExit(f"FAIL: no pointer entries: {pointer_index_path}")
    return best


def select_attempt_for_day(truth_root: Path, day_utc: str) -> AttemptSelection:
    pointer_index_path = (
        truth_root / "reports" / "orchestrator_run_verdict_v2" / day_utc / "canonical_pointer_index.v1.jsonl"
    ).resolve()
    pointer_entry = _read_latest_pointer_entry(pointer_index_path)
    verdict_path = Path(str(pointer_entry.get("points_to") or "")).expanduser().resolve()
    attempt_manifest_path = Path(str(pointer_entry.get("attempt_manifest_path") or "")).expanduser().resolve()
    if not verdict_path:
        raise SystemExit(f"FAIL: missing points_to in pointer entry: {pointer_index_path}")
    if not attempt_manifest_path:
        raise SystemExit(f"FAIL: missing attempt_manifest_path in pointer entry: {pointer_index_path}")
    return AttemptSelection(
        pointer_index_path=pointer_index_path,
        pointer_entry=pointer_entry,
        attempt_manifest_path=attempt_manifest_path,
        attempt_manifest=_read_json_obj(attempt_manifest_path),
        verdict_path=verdict_path,
        verdict=_read_json_obj(verdict_path),
    )


def _attempt_stage_view(doc: Dict[str, Any], stage_id: str) -> Dict[str, Any]:
    stages = doc.get("stages")
    if not isinstance(stages, list):
        return {"present": False, "status": None, "reason_codes": []}
    for stage in stages:
        if not isinstance(stage, dict):
            continue
        if str(stage.get("stage_id") or "") != stage_id:
            continue
        status = stage.get("status")
        return {
            "present": True,
            "status": str(status).upper() if isinstance(status, str) and status.strip() else None,
            "reason_codes": stage.get("reason_codes") if isinstance(stage.get("reason_codes"), list) else [],
        }
    return {"present": False, "status": None, "reason_codes": []}


def _count_authorization_rejected(truth_root: Path, day_utc: str) -> Tuple[int, List[str]]:
    root = (truth_root / "engine_activity_v1" / "authorization_v1" / day_utc).resolve()
    if not root.exists() or not root.is_dir():
        return 0, [str(root)]
    seen = set()
    count = 0
    for path in sorted(root.glob("*.authorization.v1.json"), key=lambda p: p.name):
        obj = _safe_read_json_obj(path)
        if not isinstance(obj, dict):
            continue
        intent_hash = str(obj.get("intent_hash") or "").strip() or path.name.split(".")[0]
        if intent_hash in seen:
            continue
        seen.add(intent_hash)
        status = str(obj.get("status") or obj.get("decision") or "").strip().upper()
        if status == "REJECTED":
            count += 1
    return count, []


def _count_phasec_veto_records(truth_root: Path, day_utc: str) -> Tuple[int, List[str]]:
    root = (truth_root / "phaseC_preflight_v1" / day_utc).resolve()
    if not root.exists() or not root.is_dir():
        return 0, [str(root)]
    seen = set()
    count = 0
    for path in sorted(root.rglob("*.veto_record.v1.json"), key=lambda p: str(p)):
        intent_hash = path.name.split(".")[0]
        if intent_hash in seen:
            continue
        seen.add(intent_hash)
        count += 1
    return count, []


def _count_submissions_and_fills(truth_root: Path, day_utc: str) -> Tuple[Dict[str, int], List[str]]:
    root = (truth_root / "execution_evidence_v1" / "submissions" / day_utc).resolve()
    if not root.exists() or not root.is_dir():
        return {"submitted": 0, "filled": 0}, [str(root)]
    submitted = 0
    filled = 0
    for path in sorted(root.rglob("*.json"), key=lambda p: str(p)):
        obj = _safe_read_json_obj(path)
        if not isinstance(obj, dict):
            continue
        schema_id = str(obj.get("schema_id") or "")
        if "broker_submission_record" in schema_id:
            submitted += 1
        if "execution_event_record" in schema_id:
            status = str(obj.get("status") or "").upper()
            if "FILL" in status:
                filled += 1
    return {"submitted": submitted, "filled": filled}, []


def _load_activity_rollup(truth_root: Path, day_utc: str) -> Optional[Dict[str, Any]]:
    path = (
        truth_root / "monitoring_v1" / "activity_ledger_rollup_v1" / day_utc / "activity_ledger_rollup.v1.json"
    ).resolve()
    return _safe_read_json_obj(path)


def _extract_flow_from_activity_rollup(doc: Optional[Dict[str, Any]]) -> Dict[str, Optional[int]]:
    out: Dict[str, Optional[int]] = {
        "intents": None,
        "authorized": None,
        "submitted": None,
        "filled": None,
        "reconciled": None,
        "blocked_liquidity": None,
        "blocked_correlation": None,
        "blocked_attestation": None,
        "blocked_convex": None,
        "blocked_capital": None,
    }
    if not isinstance(doc, dict):
        return out
    src = None
    if isinstance(doc.get("totals"), dict):
        src = doc["totals"]
    elif isinstance(doc.get("counts"), dict):
        src = doc["counts"]
    else:
        src = doc

    def _get_int(keys: List[str]) -> Optional[int]:
        if not isinstance(src, dict):
            return None
        for key in keys:
            value = src.get(key)
            if isinstance(value, int):
                return value
        return None

    out["intents"] = _get_int(["intents_total", "intents_today", "intents"])
    out["authorized"] = _get_int(["authorized_total", "authorizations_total", "authorized"])
    out["submitted"] = _get_int(["submissions_total", "submitted_total", "submitted"])
    out["filled"] = _get_int(["fills_total", "filled_total", "filled"])
    out["reconciled"] = _get_int(["reconciled_total", "reconciled"])
    blocked = doc.get("blocked_by_gate")
    if isinstance(blocked, dict):
        mapping = [
            ("liquidity", "blocked_liquidity"),
            ("correlation", "blocked_correlation"),
            ("attestation", "blocked_attestation"),
            ("convex", "blocked_convex"),
            ("capital", "blocked_capital"),
        ]
        for source_key, out_key in mapping:
            value = blocked.get(source_key)
            if isinstance(value, int):
                out[out_key] = value
    return out


def build_activity_flow_counts(
    truth_root: Path,
    day_utc: str,
    attempt: AttemptSelection,
) -> Dict[str, Any]:
    rollup_doc = _load_activity_rollup(truth_root, day_utc)
    flow = _extract_flow_from_activity_rollup(rollup_doc)
    intents_rollup_path = (truth_root / "intents_v1" / "day_rollup" / day_utc / "intents_day_rollup.v1.json").resolve()
    intents_rollup = _safe_read_json_obj(intents_rollup_path)
    intents_from_rollup = _count_intents_from_rollup(intents_rollup)
    rejected_count, rejected_missing = _count_authorization_rejected(truth_root, day_utc)
    veto_count, veto_missing = _count_phasec_veto_records(truth_root, day_utc)
    sub_counts, submission_missing = _count_submissions_and_fills(truth_root, day_utc)

    counts: Dict[str, Optional[int]] = {
        "intents": int(flow["intents"]) if isinstance(flow.get("intents"), int) else intents_from_rollup,
        "authorized": int(flow["authorized"]) if isinstance(flow.get("authorized"), int) else None,
        "submitted": int(flow["submitted"]) if isinstance(flow.get("submitted"), int) else sub_counts["submitted"],
        "filled": int(flow["filled"]) if isinstance(flow.get("filled"), int) else sub_counts["filled"],
        "reconciled": int(flow["reconciled"]) if isinstance(flow.get("reconciled"), int) else None,
        "rejected": rejected_count if rejected_count > 0 else 0,
        "vetoed": veto_count if veto_count > 0 else 0,
    }

    auth_stage = _attempt_stage_view(attempt.verdict, "A6B_AUTHORIZATION_ARTIFACTS_DAY_V1")
    recon_stage = _attempt_stage_view(attempt.verdict, "B2_EXECUTION_RECONCILIATION_V1")
    if counts["authorized"] is None and auth_stage.get("status") in ("OK", "SKIP", "PASS"):
        counts["authorized"] = counts["submitted"] if isinstance(counts["submitted"], int) else None
    if counts["reconciled"] is None and recon_stage.get("status") in ("OK", "SKIP", "PASS"):
        counts["reconciled"] = counts["submitted"] if isinstance(counts["submitted"], int) else None

    suspicion_flags: List[str] = []
    if isinstance(counts["filled"], int) and isinstance(counts["submitted"], int) and counts["filled"] > counts["submitted"]:
        suspicion_flags.append("FILLED_EXCEEDS_SUBMITTED")
    if isinstance(counts["submitted"], int) and isinstance(counts["authorized"], int) and counts["submitted"] > counts["authorized"]:
        suspicion_flags.append("SUBMITTED_EXCEEDS_AUTHORIZED")
    if isinstance(counts["authorized"], int) and isinstance(counts["intents"], int) and counts["authorized"] > counts["intents"]:
        suspicion_flags.append("AUTHORIZED_EXCEEDS_INTENTS")

    evidence_paths = sorted(
        {
            str(attempt.pointer_index_path),
            str(attempt.attempt_manifest_path),
            str(attempt.verdict_path),
            str(intents_rollup_path),
            str((truth_root / "engine_activity_v1" / "authorization_v1" / day_utc).resolve()),
            str((truth_root / "phaseC_preflight_v1" / day_utc).resolve()),
            str((truth_root / "execution_evidence_v1" / "submissions" / day_utc).resolve()),
        }
    )

    missing_paths = sorted(set(rejected_missing + veto_missing + submission_missing))
    return {
        "counts": counts,
        "suspicion_flags": sorted(set(suspicion_flags)),
        "evidence_paths": evidence_paths,
        "missing_paths": missing_paths,
    }


def build_activity_flow_diagnostics_doc(
    *,
    repo_root: Path,
    truth_root: Path,
    day_utc: str,
    produced_utc: str,
) -> Dict[str, Any]:
    attempt = select_attempt_for_day(truth_root, day_utc)
    flow = build_activity_flow_counts(truth_root, day_utc, attempt)
    counts = flow["counts"]
    verdict_status = str(attempt.verdict.get("status") or "UNKNOWN").upper()

    stage_metrics = [
        {
            "stage_name": "INTENTS",
            "input_count": counts.get("intents"),
            "passed_count": counts.get("intents"),
            "rejected_count": 0,
            "emitted_count": counts.get("intents"),
            "availability_status": "PRESENT" if isinstance(counts.get("intents"), int) else "UNAVAILABLE",
        },
        {
            "stage_name": "AUTHORIZATION",
            "input_count": counts.get("intents"),
            "passed_count": counts.get("authorized"),
            "rejected_count": counts.get("rejected"),
            "emitted_count": counts.get("authorized"),
            "availability_status": "PRESENT" if isinstance(counts.get("authorized"), int) else "UNAVAILABLE",
        },
        {
            "stage_name": "GOVERNED_SUBMIT",
            "input_count": counts.get("authorized"),
            "passed_count": counts.get("submitted"),
            "rejected_count": counts.get("vetoed"),
            "emitted_count": counts.get("submitted"),
            "availability_status": "PRESENT" if isinstance(counts.get("submitted"), int) else "UNAVAILABLE",
        },
        {
            "stage_name": "BROKER_FILL",
            "input_count": counts.get("submitted"),
            "passed_count": counts.get("filled"),
            "rejected_count": None,
            "emitted_count": counts.get("filled"),
            "availability_status": "PRESENT" if isinstance(counts.get("filled"), int) else "UNAVAILABLE",
        },
        {
            "stage_name": "EXECUTION_RECONCILIATION",
            "input_count": counts.get("submitted"),
            "passed_count": counts.get("reconciled"),
            "rejected_count": None,
            "emitted_count": counts.get("reconciled"),
            "availability_status": "PRESENT" if isinstance(counts.get("reconciled"), int) else "UNAVAILABLE",
        },
    ]

    rejection_breakdown = []
    if isinstance(counts.get("rejected"), int) and counts["rejected"] > 0:
        rejection_breakdown.append(
            {"stage_name": "AUTHORIZATION", "reason_classification": "REJECTED", "count": counts["rejected"]}
        )
    if isinstance(counts.get("vetoed"), int) and counts["vetoed"] > 0:
        rejection_breakdown.append(
            {"stage_name": "GOVERNED_SUBMIT", "reason_classification": "VETOED", "count": counts["vetoed"]}
        )

    if verdict_status == "PASS":
        terminal_state = "FLOW_COMPLETED"
    elif verdict_status == "DEGRADED" and int(counts.get("intents") or 0) == 0:
        terminal_state = "NO_ACTIVITY_DAY"
    elif verdict_status in {"FAIL", "ABORTED"}:
        terminal_state = "UPSTREAM_BLOCKED"
    else:
        terminal_state = "PARTIAL_OR_UNKNOWN"

    return {
        "schema_id": "activity_flow_diagnostics_v1",
        "schema_version": 1,
        "diagnostic_ruleset_id": FLOW_RULESET_ID,
        "diagnostic_ruleset_version": FLOW_RULESET_VERSION,
        "day_utc": day_utc,
        "produced_utc": produced_utc,
        "selected_attempt_id": str(attempt.pointer_entry.get("attempt_id") or ""),
        "present": True,
        "terminal_state": terminal_state,
        "integrity_status": "EVIDENCE_INCONSISTENT" if flow["suspicion_flags"] else "OK",
        "suspicion_flags": flow["suspicion_flags"],
        "counts": counts,
        "stage_metrics": stage_metrics,
        "rejection_breakdown": rejection_breakdown,
        "evidence_paths": flow["evidence_paths"],
        "producer": {
            "repo": "constellation_2_runtime",
            "module": "constellation_2/common/diagnostic_foundation_v1.py",
            "git_sha": _git_sha(repo_root),
        },
    }


def _first_failed_stage(attempt_manifest: Dict[str, Any]) -> Tuple[Optional[str], List[str], Optional[str]]:
    stages = attempt_manifest.get("stages")
    if not isinstance(stages, list):
        return None, [], None
    for stage in stages:
        if not isinstance(stage, dict):
            continue
        status = str(stage.get("status") or "").upper()
        if status != "FAIL":
            continue
        classification = stage.get("classification") if isinstance(stage.get("classification"), dict) else {}
        reason_codes = stage.get("reason_codes") if isinstance(stage.get("reason_codes"), list) else []
        if bool(classification.get("effective_blocking")):
            return str(stage.get("stage_id") or ""), [str(x) for x in reason_codes], "BLOCKING_STAGE_FAIL"
    for stage in stages:
        if not isinstance(stage, dict):
            continue
        status = str(stage.get("status") or "").upper()
        if status != "FAIL":
            continue
        reason_codes = stage.get("reason_codes") if isinstance(stage.get("reason_codes"), list) else []
        return str(stage.get("stage_id") or ""), [str(x) for x in reason_codes], "REQUIRED_STAGE_FAIL"
    return None, [], None


def build_batch1_docs(
    *,
    repo_root: Path,
    truth_root: Path,
    day_utc: str,
    produced_utc: str,
    readiness_artifact_path: Optional[Path] = None,
) -> Dict[str, Dict[str, Any]]:
    attempt = select_attempt_for_day(truth_root, day_utc)
    flow = build_activity_flow_counts(truth_root, day_utc, attempt)
    intents_rollup_path = (truth_root / "intents_v1" / "day_rollup" / day_utc / "intents_day_rollup.v1.json").resolve()
    intents_rollup = _safe_read_json_obj(intents_rollup_path)
    final_intent_count = _count_intents_from_rollup(intents_rollup)

    readiness_status = None
    readiness_reason_codes: List[str] = []
    if readiness_artifact_path is not None:
        readiness_doc = _safe_read_json_obj(readiness_artifact_path)
        if isinstance(readiness_doc, dict):
            readiness_status = str(readiness_doc.get("status") or "").upper() or None
            readiness_reason_codes = [str(x) for x in readiness_doc.get("reason_codes", []) if isinstance(x, str)]

    failing_stage, failing_reason_codes, failing_classification = _first_failed_stage(attempt.attempt_manifest)
    verdict_status = str(attempt.verdict.get("status") or "UNKNOWN").upper()

    root_rules: List[Dict[str, Any]] = []
    root_winning_rule = ""
    root_classification = "NO_ROOT_CAUSE_IDENTIFIED"
    first_break_cause = "NONE"
    severity = "INFO"
    violating_invariant = None

    def _append_rule(rule_id: str, matched: bool, reason: str) -> None:
        root_rules.append({"rule_id": rule_id, "matched": bool(matched), "reason": reason})

    if readiness_status not in (None, "", "PASS", "OK"):
        _append_rule("READINESS_BLOCKED", True, f"readiness_status={readiness_status}")
        root_winning_rule = "READINESS_BLOCKED"
        root_classification = "READINESS_BLOCKED"
        first_break_cause = readiness_reason_codes[0] if readiness_reason_codes else "READINESS_BLOCKED"
        severity = "ERROR"
    else:
        _append_rule("READINESS_BLOCKED", False, "readiness artifact absent or passing")
        if failing_stage and failing_classification == "BLOCKING_STAGE_FAIL":
            _append_rule("BLOCKING_STAGE_FAIL", True, f"stage_id={failing_stage}")
            root_winning_rule = "BLOCKING_STAGE_FAIL"
            root_classification = "INVARIANT_VIOLATION"
            first_break_cause = failing_stage
            severity = "ERROR"
            violating_invariant = "BLOCKING_STAGE_FAIL"
        else:
            _append_rule("BLOCKING_STAGE_FAIL", False, "no blocking stage failure")
            if verdict_status == "ABORTED":
                _append_rule("RUN_ABORTED", True, "orchestrator verdict ABORTED")
                root_winning_rule = "RUN_ABORTED"
                root_classification = "INVARIANT_VIOLATION"
                first_break_cause = "RUN_ABORTED"
                severity = "ERROR"
                violating_invariant = "ORCHESTRATOR_ABORTED"
            else:
                _append_rule("RUN_ABORTED", False, f"verdict_status={verdict_status}")
                if failing_stage:
                    _append_rule("REQUIRED_STAGE_FAIL", True, f"stage_id={failing_stage}")
                    root_winning_rule = "REQUIRED_STAGE_FAIL"
                    root_classification = "INVARIANT_VIOLATION"
                    first_break_cause = failing_stage
                    severity = "ERROR"
                    violating_invariant = "REQUIRED_STAGE_FAIL"
                else:
                    _append_rule("REQUIRED_STAGE_FAIL", False, "no failed stage")
                    if final_intent_count is None:
                        _append_rule("MISSING_REQUIRED_ARTIFACT", True, f"missing={intents_rollup_path}")
                        root_winning_rule = "MISSING_REQUIRED_ARTIFACT"
                        root_classification = "MISSING_REQUIRED_ARTIFACT"
                        first_break_cause = "INTENTS_DAY_ROLLUP_MISSING"
                        severity = "WARN"
                    else:
                        _append_rule("MISSING_REQUIRED_ARTIFACT", False, "required Batch1 inputs present")
                        root_winning_rule = "NO_ROOT_CAUSE_IDENTIFIED"

    evidence_refs = sorted(
        {
            str(attempt.pointer_index_path),
            str(attempt.attempt_manifest_path),
            str(attempt.verdict_path),
            str(intents_rollup_path),
            str(readiness_artifact_path) if readiness_artifact_path else "",
        }
    )
    evidence_refs = [x for x in evidence_refs if x]

    ledger_doc = {
        "schema_id": "diagnostic_decision_ledger_v1",
        "schema_version": 1,
        "diagnostic_ruleset_id": DIAGNOSTIC_RULESET_ID,
        "diagnostic_ruleset_version": DIAGNOSTIC_RULESET_VERSION,
        "day_utc": day_utc,
        "selected_attempt_id": str(attempt.pointer_entry.get("attempt_id") or ""),
        "evidence_refs": evidence_refs,
        "root_cause_rules_evaluated": root_rules,
        "root_cause_winning_rule": root_winning_rule,
        "intent_absence_rules_evaluated": [],
        "intent_absence_winning_rule": "",
        "final_classifications": {
            "root_cause": root_classification,
            "intent_absence": "",
        },
    }

    root_cause_doc = {
        "schema_id": "root_cause_v1",
        "schema_version": 1,
        "diagnostic_ruleset_id": DIAGNOSTIC_RULESET_ID,
        "diagnostic_ruleset_version": DIAGNOSTIC_RULESET_VERSION,
        "day_utc": day_utc,
        "selected_attempt_id": str(attempt.pointer_entry.get("attempt_id") or ""),
        "diagnosis_status": "DIAGNOSED" if root_winning_rule != "NO_ROOT_CAUSE_IDENTIFIED" else "NO_ROOT_CAUSE",
        "first_break_cause": first_break_cause,
        "contributing_causes": failing_reason_codes,
        "downstream_effects": ["ZERO_INTENTS"] if int(final_intent_count or 0) == 0 else [],
        "failing_stage": failing_stage,
        "failing_artifact_or_dependency": str(readiness_artifact_path) if root_classification == "READINESS_BLOCKED" else None,
        "violated_invariant": violating_invariant,
        "decision_ledger_ref": str((truth_root / "reports" / "diagnostic_decision_ledger_v1" / day_utc / "diagnostic_decision_ledger.v1.json").resolve()),
        "severity": severity,
        "evidence_refs": evidence_refs,
        "deterministic_classification": root_classification,
    }

    absence_rules: List[Dict[str, Any]] = []

    def _append_absence_rule(rule_id: str, matched: bool, reason: str) -> None:
        absence_rules.append({"rule_id": rule_id, "matched": bool(matched), "reason": reason})

    if final_intent_count is None:
        _append_absence_rule("MISSING_INTENTS_DAY_ROLLUP", True, f"path={intents_rollup_path}")
        zero_classification = "DETERMINISTICALLY_UNRESOLVED_DUE_TO_MISSING_EVIDENCE"
        winning_absence_rule = "MISSING_INTENTS_DAY_ROLLUP"
        dominant_stage = None
        dominant_reason = "INTENTS_DAY_ROLLUP_MISSING"
    elif final_intent_count > 0:
        _append_absence_rule("NON_ZERO_INTENTS", True, f"intent_count={final_intent_count}")
        zero_classification = "NON_ZERO_INTENTS"
        winning_absence_rule = "NON_ZERO_INTENTS"
        dominant_stage = None
        dominant_reason = None
    elif root_classification == "READINESS_BLOCKED" or verdict_status in {"FAIL", "ABORTED"}:
        _append_absence_rule("UPSTREAM_BLOCKED", True, f"root_classification={root_classification} verdict_status={verdict_status}")
        zero_classification = "UPSTREAM_BLOCKED_NO_INTENT_PATH"
        winning_absence_rule = "UPSTREAM_BLOCKED"
        dominant_stage = failing_stage
        dominant_reason = first_break_cause
    else:
        _append_absence_rule("ZERO_INTENTS_EXPECTED", True, "intents rollup is zero with no upstream block")
        zero_classification = "ZERO_INTENTS_EXPECTED"
        winning_absence_rule = "ZERO_INTENTS_EXPECTED"
        dominant_stage = "INTENTS"
        dominant_reason = "NO_INTENTS_EMITTED"

    ledger_doc["intent_absence_rules_evaluated"] = absence_rules
    ledger_doc["intent_absence_winning_rule"] = winning_absence_rule
    ledger_doc["final_classifications"]["intent_absence"] = zero_classification

    intent_absence_doc = {
        "schema_id": "intent_absence_analysis_v1",
        "schema_version": 1,
        "diagnostic_ruleset_id": DIAGNOSTIC_RULESET_ID,
        "diagnostic_ruleset_version": DIAGNOSTIC_RULESET_VERSION,
        "day_utc": day_utc,
        "selected_attempt_id": str(attempt.pointer_entry.get("attempt_id") or ""),
        "final_intent_count": final_intent_count,
        "dominant_elimination_stage": dominant_stage,
        "dominant_elimination_reason": dominant_reason,
        "zero_intent_classification": zero_classification,
        "decision_ledger_ref": str((truth_root / "reports" / "diagnostic_decision_ledger_v1" / day_utc / "diagnostic_decision_ledger.v1.json").resolve()),
        "evidence_refs": evidence_refs,
        "deterministic_classification": zero_classification,
    }

    return {
        "decision_ledger": ledger_doc,
        "root_cause": root_cause_doc,
        "intent_absence": intent_absence_doc,
    }


def _to_metric_value(doc: Dict[str, Any], key: str) -> Optional[int]:
    counts = doc.get("counts")
    if not isinstance(counts, dict):
        return None
    value = counts.get(key)
    return value if isinstance(value, int) else None


def _find_latest_prior_activity_flow_day(truth_root: Path, day_utc: str) -> Optional[str]:
    root = (truth_root / "reports" / "activity_flow_diagnostics_v1").resolve()
    if not root.exists() or not root.is_dir():
        return None
    days = sorted([p.name for p in root.iterdir() if p.is_dir() and DATE_RE.match(p.name)])
    prior = [day for day in days if day < day_utc]
    return prior[-1] if prior else None


def build_runtime_regression_doc(
    *,
    repo_root: Path,
    truth_root: Path,
    day_utc: str,
    produced_utc: str,
) -> Dict[str, Any]:
    current_path = (truth_root / "reports" / "activity_flow_diagnostics_v1" / day_utc / "activity_flow_diagnostics.v1.json").resolve()
    current_doc = _read_json_obj(current_path)
    comparison_day = _find_latest_prior_activity_flow_day(truth_root, day_utc)
    comparison_path = None
    comparison_doc = None
    comparison_status = "PRIOR_DAY_AVAILABLE"
    if comparison_day is not None:
        comparison_path = (
            truth_root / "reports" / "activity_flow_diagnostics_v1" / comparison_day / "activity_flow_diagnostics.v1.json"
        ).resolve()
        comparison_doc = _safe_read_json_obj(comparison_path)
    if comparison_day is None or not isinstance(comparison_doc, dict):
        comparison_status = "PRIOR_DAY_UNAVAILABLE"

    metrics = ["intents", "authorized", "submitted", "filled", "rejected", "vetoed"]
    compared_metrics: List[Dict[str, Any]] = []
    material_changes: List[Dict[str, Any]] = []
    for metric in metrics:
        current_value = _to_metric_value(current_doc, metric)
        comparison_value = _to_metric_value(comparison_doc, metric) if isinstance(comparison_doc, dict) else None
        if current_value is None or comparison_value is None:
            delta = None
            relative_change = None
            metric_status = "UNAVAILABLE"
        else:
            delta = current_value - comparison_value
            relative_change = None if comparison_value == 0 else int(((delta * 10000) / comparison_value))
            metric_status = "COMPARED"
            if delta != 0:
                material_changes.append({"metric_id": metric, "absolute_change": delta})
        compared_metrics.append(
            {
                "metric_id": metric,
                "current_value": current_value,
                "comparison_value": comparison_value,
                "absolute_change": delta,
                "relative_change_bps": relative_change,
                "comparison_status": metric_status,
            }
        )

    return {
        "schema_id": "runtime_regression_analytics_v1",
        "schema_version": 1,
        "regression_ruleset_id": REGRESSION_RULESET_ID,
        "regression_ruleset_version": REGRESSION_RULESET_VERSION,
        "comparison_mode": REGRESSION_COMPARISON_MODE,
        "day_utc": day_utc,
        "comparison_day_utc": comparison_day,
        "produced_utc": produced_utc,
        "comparison_status": comparison_status,
        "current_activity_flow_ref": str(current_path),
        "comparison_activity_flow_ref": str(comparison_path) if comparison_path else None,
        "compared_metrics": compared_metrics,
        "material_changes": material_changes,
        "integrity_status": "OK" if comparison_status == "PRIOR_DAY_AVAILABLE" else "COMPARISON_UNAVAILABLE",
        "producer": {
            "repo": "constellation_2_runtime",
            "module": "constellation_2/common/diagnostic_foundation_v1.py",
            "git_sha": _git_sha(repo_root),
        },
    }


def write_batch1_diagnostics(
    *,
    repo_root: Path,
    truth_root: Path,
    day_utc: str,
    produced_utc: str,
    readiness_artifact_path: Optional[Path] = None,
) -> Dict[str, RefreshWriteResultV1]:
    docs = build_batch1_docs(
        repo_root=repo_root,
        truth_root=truth_root,
        day_utc=day_utc,
        produced_utc=produced_utc,
        readiness_artifact_path=readiness_artifact_path,
    )
    ledger_path = (truth_root / "reports" / "diagnostic_decision_ledger_v1" / day_utc / "diagnostic_decision_ledger.v1.json").resolve()
    root_path = (truth_root / "reports" / "root_cause_v1" / day_utc / "root_cause.v1.json").resolve()
    absence_path = (truth_root / "reports" / "intent_absence_analysis_v1" / day_utc / "intent_absence_analysis.v1.json").resolve()
    return {
        "decision_ledger": _write_refreshable_json(
            repo_root=repo_root,
            out_path=ledger_path,
            doc=docs["decision_ledger"],
            schema_relpath=DECISION_LEDGER_SCHEMA,
            expected_schema_id="diagnostic_decision_ledger_v1",
            expected_schema_version=1,
            expected_day_utc=day_utc,
        ),
        "root_cause": _write_refreshable_json(
            repo_root=repo_root,
            out_path=root_path,
            doc=docs["root_cause"],
            schema_relpath=ROOT_CAUSE_SCHEMA,
            expected_schema_id="root_cause_v1",
            expected_schema_version=1,
            expected_day_utc=day_utc,
        ),
        "intent_absence": _write_refreshable_json(
            repo_root=repo_root,
            out_path=absence_path,
            doc=docs["intent_absence"],
            schema_relpath=INTENT_ABSENCE_SCHEMA,
            expected_schema_id="intent_absence_analysis_v1",
            expected_schema_version=1,
            expected_day_utc=day_utc,
        ),
    }


def write_activity_flow_diagnostics(
    *,
    repo_root: Path,
    truth_root: Path,
    day_utc: str,
    produced_utc: str,
) -> RefreshWriteResultV1:
    doc = build_activity_flow_diagnostics_doc(
        repo_root=repo_root,
        truth_root=truth_root,
        day_utc=day_utc,
        produced_utc=produced_utc,
    )
    out_path = (truth_root / "reports" / "activity_flow_diagnostics_v1" / day_utc / "activity_flow_diagnostics.v1.json").resolve()
    return _write_refreshable_json(
        repo_root=repo_root,
        out_path=out_path,
        doc=doc,
        schema_relpath=ACTIVITY_FLOW_SCHEMA,
        expected_schema_id="activity_flow_diagnostics_v1",
        expected_schema_version=1,
        expected_day_utc=day_utc,
    )


def write_runtime_regression_analytics(
    *,
    repo_root: Path,
    truth_root: Path,
    day_utc: str,
    produced_utc: str,
) -> RefreshWriteResultV1:
    doc = build_runtime_regression_doc(
        repo_root=repo_root,
        truth_root=truth_root,
        day_utc=day_utc,
        produced_utc=produced_utc,
    )
    out_path = (
        truth_root / "reports" / "runtime_regression_analytics_v1" / day_utc / "runtime_regression_analytics.v1.json"
    ).resolve()
    return _write_refreshable_json(
        repo_root=repo_root,
        out_path=out_path,
        doc=doc,
        schema_relpath=REGRESSION_SCHEMA,
        expected_schema_id="runtime_regression_analytics_v1",
        expected_schema_version=1,
        expected_day_utc=day_utc,
    )
