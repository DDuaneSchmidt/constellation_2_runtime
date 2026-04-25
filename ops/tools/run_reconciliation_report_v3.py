#!/usr/bin/env python3
"""
run_reconciliation_report_v3.py

Reconciliation Report v3: SAFE_IDLE aware, forward-only readiness artifact.

Run:
  python3 ops/tools/run_reconciliation_report_v3.py --day_utc YYYY-MM-DD
"""

from __future__ import annotations

import sys
from pathlib import Path

_THIS_FILE = Path(__file__).resolve()
_REPO_ROOT_FROM_FILE = _THIS_FILE.parents[2]
if str(_REPO_ROOT_FROM_FILE) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT_FROM_FILE))

if not (_REPO_ROOT_FROM_FILE / "constellation_2").exists():
    raise SystemExit(f"FATAL: repo_root_missing_constellation_2: derived={_REPO_ROOT_FROM_FILE}")
if not (_REPO_ROOT_FROM_FILE / "governance").exists():
    raise SystemExit(f"FATAL: repo_root_missing_governance: derived={_REPO_ROOT_FROM_FILE}")

import argparse
import hashlib
import json
import subprocess
from typing import Any, Dict, List, Optional

from constellation_2.common.day_open_attempt_v1 import (
    read_day_open_attempt_runtime_lifecycle_ref_v1,
)
from constellation_2.common.runtime_authority_bridge_v1 import resolve_canonical_truth_root_bridge_v1
from constellation_2.common.runtime_contract_v1 import resolve_release_provenance_release_current_first_v1
from constellation_2.common.constitutional_runtime_v1 import (
    CLOSURE_STATE_BLOCKED,
    CLOSURE_STATE_COMPLETE,
    FINALITY_PROVISIONAL,
    assert_constitutional_writer_allowed_v1,
    build_artifact_dependency_declaration_v1,
    build_governed_artifact_lineage_v1,
    build_machine_blocker_envelope_v1,
    validate_governed_artifact_payload_v1,
)
from constellation_2.phaseD.lib.validate_against_schema_v1 import validate_against_repo_schema_v1
from constellation_2.phaseF.accounting.lib.day_artifact_refresh_v1 import write_day_artifact_refreshable_v1

REPO_ROOT = _REPO_ROOT_FROM_FILE.resolve()
TRUTH = resolve_canonical_truth_root_bridge_v1(
    caller="ops/tools/run_reconciliation_report_v3.py"
).resolve()

SCHEMA_RELPATH = "governance/04_DATA/SCHEMAS/C2/REPORTS/reconciliation_report.v3.schema.json"

BROKER_EVENTS_ROOT = (TRUTH / "execution_evidence_v1/broker_events").resolve()
EXEC_TRUTH_ROOT = (TRUTH / "execution_evidence_v1/submissions").resolve()

OUT_ROOT = (TRUTH / "reports" / "reconciliation_report_v3").resolve()


def _resolve_truth_root(truth_root_arg: str) -> Path:
    raw = (truth_root_arg or "").strip()
    if not raw:
        return TRUTH
    p = Path(raw).expanduser().resolve()
    if not p.is_absolute():
        raise SystemExit(f"FAIL: --truth_root must be absolute: {p}")
    if not p.exists() or not p.is_dir():
        raise SystemExit(f"FAIL: --truth_root must exist and be a directory: {p}")
    return p


def _git_sha() -> str:
    try:
        s = str(
            resolve_release_provenance_release_current_first_v1(
                caller="ops/tools/run_reconciliation_report_v3.py"
            ).get("git_sha")
            or ""
        ).strip()
        if s:
            return s
    except Exception:
        pass
    try:
        out = subprocess.check_output(["/usr/bin/git", "rev-parse", "HEAD"], cwd=str(REPO_ROOT))
        return out.decode("utf-8").strip()
    except Exception:
        # Clean runtime roots can be source-derived without .git metadata.
        return "0" * 40


def _parse_day_utc(s: str) -> str:
    d = (s or "").strip()
    if len(d) != 10 or d[4] != "-" or d[7] != "-":
        raise ValueError(f"BAD_DAY_UTC_FORMAT_EXPECTED_YYYY_MM_DD: {d!r}")
    return d


def _sha256_file(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def _sha256_bytes(b: bytes) -> str:
    return hashlib.sha256(b).hexdigest()


def _read_json(path: Path) -> Dict[str, Any]:
    obj = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(obj, dict):
        raise ValueError(f"TOP_LEVEL_NOT_OBJECT: {path}")
    return obj


def _read_jsonl_rows(path: Path) -> List[Dict[str, Any]]:
    if not path.exists() or not path.is_file():
        return []
    rows: List[Dict[str, Any]] = []
    for raw in path.read_text(encoding="utf-8").splitlines():
        line = raw.strip()
        if not line:
            continue
        try:
            obj = json.loads(line)
        except Exception:
            continue
        if isinstance(obj, dict):
            rows.append(obj)
    return rows


def _legacy_event_type(row: Dict[str, Any]) -> str:
    return str(row.get("event_type") or "").strip()


def _legacy_event_args(row: Dict[str, Any]) -> List[str]:
    ib_fields = row.get("ib_fields")
    if not isinstance(ib_fields, dict):
        return []
    args = ib_fields.get("args")
    if not isinstance(args, list):
        return []
    out: List[str] = []
    for item in args:
        if not isinstance(item, dict):
            continue
        value = str(item.get("value") or "").strip()
        if value:
            out.append(value)
    return out


def _kv_from_args(args: List[str]) -> Dict[str, str]:
    out: Dict[str, str] = {}
    for item in args:
        if "=" not in item:
            continue
        key, value = item.split("=", 1)
        key = str(key).strip()
        value = str(value).strip()
        if key and key not in out:
            out[key] = value
    return out


def _broker_positions_capture(rows: List[Dict[str, Any]], *, position_fact_path: Path) -> tuple[str, str]:
    if position_fact_path.exists() and position_fact_path.is_file():
        return "OK", f"broker position fact ledger present path={position_fact_path}"
    event_types = {_legacy_event_type(row) for row in rows}
    if "positionEnd" in event_types:
        return "OK", "broker position capture completed via positionEnd"
    return "FAIL", "broker position capture missing for active-trading reconciliation"


def _broker_cash_capture(
    rows: List[Dict[str, Any]],
    *,
    broker_statement_path: Path,
) -> tuple[str, str]:
    if broker_statement_path.exists() and broker_statement_path.is_file():
        try:
            payload = _read_json(broker_statement_path)
        except Exception as exc:
            return "FAIL", f"broker statement normalized unreadable: {exc!r}"
        if str(payload.get("day_utc") or "").strip():
            return "OK", f"broker statement normalized present path={broker_statement_path}"
    saw_summary_end = False
    saw_cash_value = False
    for row in rows:
        event_type = _legacy_event_type(row)
        if event_type == "accountSummaryEnd":
            saw_summary_end = True
            continue
        if event_type not in {"accountSummary", "updateAccountValue"}:
            continue
        mapping = _kv_from_args(_legacy_event_args(row))
        tag = str(mapping.get("tag") or mapping.get("key") or "").strip()
        value = str(mapping.get("value") or "").strip()
        if tag in {"TotalCashValue", "TotalCashBalance", "CashBalance", "NetLiquidation"} and value:
            saw_cash_value = True
    if saw_cash_value and saw_summary_end:
        return "OK", "broker cash capture completed via accountSummary"
    return "FAIL", "broker cash capture missing for active-trading reconciliation"


def _find_ok_broker_manifest(day_dir: Path) -> Optional[Path]:
    broker_log = (day_dir / "broker_event_log.v1.jsonl").resolve()
    current_log_sha = _sha256_file(broker_log) if broker_log.exists() and broker_log.is_file() else ""

    fixed = day_dir / "broker_event_day_manifest.v1.json"
    cands = sorted([p for p in day_dir.glob("broker_event_day_manifest.v1.*.json") if p.is_file()])
    if fixed.exists() and fixed.is_file():
        cands.append(fixed)

    ranked: List[tuple[bool, str, str, Path]] = []
    for p in cands:
        try:
            o = _read_json(p)
        except Exception:
            continue
        if str(o.get("status")) != "OK":
            continue
        log = o.get("log") if isinstance(o.get("log"), dict) else {}
        manifest_log_sha = str(log.get("log_sha256") or "").strip()
        if not manifest_log_sha:
            for item in o.get("input_manifest") if isinstance(o.get("input_manifest"), list) else []:
                if not isinstance(item, dict):
                    continue
                if str(item.get("type") or "").strip() == "broker_event_log_v1_jsonl":
                    manifest_log_sha = str(item.get("sha256") or "").strip()
                    break
        produced_utc = str(o.get("produced_utc") or "").strip()
        ranked.append((bool(current_log_sha and manifest_log_sha == current_log_sha), produced_utc, p.name, p))

    if ranked:
        ranked.sort()
        return ranked[-1][3]
    return None


def _submission_dirs(exec_day_dir: Path) -> List[Path]:
    if not exec_day_dir.exists() or not exec_day_dir.is_dir():
        return []
    return sorted([path for path in exec_day_dir.iterdir() if path.is_dir()])


def _submission_expects_execdetails(submission_dir: Path) -> bool:
    execution_event_path = (submission_dir / "execution_event_record.v1.json").resolve()
    if execution_event_path.exists() and execution_event_path.is_file():
        return True

    broker_submission_path = (submission_dir / "broker_submission_record.v2.json").resolve()
    if not broker_submission_path.exists() or not broker_submission_path.is_file():
        return True

    try:
        payload = _read_json(broker_submission_path)
    except Exception:
        return True

    broker_ids = payload.get("broker_ids") if isinstance(payload.get("broker_ids"), dict) else {}
    order_id = broker_ids.get("order_id")
    perm_id = broker_ids.get("perm_id")
    if order_id is not None or perm_id is not None:
        return True

    status = str(payload.get("status") or "").strip().upper()
    if status in {"PARTIALLY_FILLED", "FILLED"}:
        return True

    return False


def main() -> int:
    ap = argparse.ArgumentParser(prog="run_reconciliation_report_v3")
    ap.add_argument("--day_utc", required=True, help="YYYY-MM-DD")
    ap.add_argument("--truth_root", default="", help="Absolute truth root override; defaults to repo truth")
    args = ap.parse_args()

    day = _parse_day_utc(args.day_utc)
    truth_root = _resolve_truth_root(str(args.truth_root))
    broker_events_root = (truth_root / "execution_evidence_v1/broker_events").resolve()
    exec_truth_root = (truth_root / "execution_evidence_v1/submissions").resolve()
    out_root = (truth_root / "reports" / "reconciliation_report_v3").resolve()

    produced_utc = f"{day}T00:00:00Z"

    input_manifest: List[Dict[str, str]] = []
    reason_codes: List[str] = []
    notes: List[str] = []
    day_open_attempt_path, runtime_lifecycle_ref = read_day_open_attempt_runtime_lifecycle_ref_v1(
        truth_root=truth_root,
        day_utc=day,
    )

    # --- Truth side ---
    exec_day_dir = (exec_truth_root / day).resolve()
    submission_dirs = _submission_dirs(exec_day_dir)
    truth_ids: List[str] = [path.name for path in submission_dirs]

    submissions_total = int(len(truth_ids))
    execdetails_expected_total = int(sum(1 for path in submission_dirs if _submission_expects_execdetails(path)))

    input_manifest.append(
        {
            "type": "exec_evidence_truth_day_dir",
            "path": str(exec_day_dir),
            "sha256": _sha256_bytes(b"present") if exec_day_dir.exists() else _sha256_bytes(b""),
        }
    )
    if runtime_lifecycle_ref is not None:
        input_manifest.append(
            {
                "type": "day_open_attempt_v1",
                "path": str(day_open_attempt_path),
                "sha256": _sha256_file(day_open_attempt_path),
            }
        )

    # SAFE_IDLE: If no submissions, reconciliation is OK and broker truth is not required.
    if submissions_total == 0:
        reason_codes.append("SAFE_IDLE_NO_SUBMISSIONS_OK")

        broker_day_dir = (broker_events_root / day).resolve()
        broker_log = (broker_day_dir / "broker_event_log.v1.jsonl").resolve()
        broker_manifest_default = (broker_day_dir / "broker_event_day_manifest.v1.json").resolve()

        broker_event_log_sha = _sha256_bytes(b"")
        input_manifest.append({"type": "broker_event_log_v1_jsonl_skipped_safe_idle", "path": str(broker_log), "sha256": broker_event_log_sha})
        input_manifest.append({"type": "broker_event_day_manifest_skipped_safe_idle", "path": str(broker_manifest_default), "sha256": _sha256_bytes(b"")})

        blocker_envelope = build_machine_blocker_envelope_v1(
            closure_state=CLOSURE_STATE_COMPLETE,
            reason_codes=[],
            missing_dependency_artifacts=[],
        )
        constitutional_dependency_declaration = build_artifact_dependency_declaration_v1(
            artifact_type="reconciliation_report_v3",
            artifact_class="outcome_record",
            authority_id="reconciliation_report_v3",
            declared_dependency_artifacts=[],
            dependency_refs=[],
        )
        constitutional_lineage = build_governed_artifact_lineage_v1(
            artifact_type="reconciliation_report_v3",
            artifact_version="v3",
            artifact_class="outcome_record",
            authority_id="reconciliation_report_v3",
            producer_id="ops/tools/run_reconciliation_report_v3.py",
            generated_at_utc=produced_utc,
            effective_at_utc=produced_utc,
            finality_state=FINALITY_PROVISIONAL,
            input_artifact_refs=[],
            policy_snapshot_refs=[],
            code_version=_git_sha(),
            run_id=f"reconciliation_report_v3:{day}",
        )

        report: Dict[str, Any] = {
            "schema_id": "reconciliation_report",
            "schema_version": "v3",
            "day_utc": day,
            "produced_utc": produced_utc,
            "producer": {"repo": "constellation_2_runtime", "module": "ops/tools/run_reconciliation_report_v3.py", "git_sha": _git_sha()},
            "status": "OK",
            "reason_codes": sorted(set(reason_codes)),
            "blocking_codes": list(blocker_envelope["blocking_codes"]),
            "closure_state": str(blocker_envelope["closure_state"]),
            "first_blocker_code": str(blocker_envelope["first_blocker_code"]),
            "missing_dependency_artifacts": list(blocker_envelope["missing_dependency_artifacts"]),
            "constitutional_dependency_declaration": constitutional_dependency_declaration,
            "constitutional_lineage": constitutional_lineage,
            "notes": notes,
            "input_manifest": input_manifest,
            "broker_side": {
                "broker_event_log_path": str(broker_log),
                "broker_event_log_sha256": broker_event_log_sha,
                "broker_event_manifest_path": str(broker_manifest_default),
                "counts": {"broker_events_total": 0, "execDetails_total": 0},
            },
            "truth_side": {
                "exec_evidence_day_dir": str(exec_day_dir),
                "submission_ids": truth_ids,
                "counts": {"submissions_total": submissions_total},
            },
            "comparisons": {
                "truth_submissions_vs_broker_execdetails": {"status": "SKIPPED_SAFE_IDLE", "reason": "SAFE_IDLE: no submissions; broker execDetails not required"},
                "cash": {"status": "SKIPPED_SAFE_IDLE", "reason": "SAFE_IDLE: no submissions; cash broker truth capture not required"},
                "positions": {"status": "SKIPPED_SAFE_IDLE", "reason": "SAFE_IDLE: no submissions; positions broker truth capture not required"},
            },
        }
        if runtime_lifecycle_ref is not None:
            report["runtime_lifecycle_ref"] = dict(runtime_lifecycle_ref)

        validate_against_repo_schema_v1(report, REPO_ROOT, SCHEMA_RELPATH)
        assert_constitutional_writer_allowed_v1(REPO_ROOT, "reconciliation_report_v3", "ops/tools/run_reconciliation_report_v3.py")
        validate_governed_artifact_payload_v1(
            repo_root=REPO_ROOT,
            artifact_id="reconciliation_report_v3",
            payload=report,
            required_finality_states=["provisional", "finalized", "corrected"],
        )

        out_dir = (out_root / day).resolve()
        out_path = (out_dir / "reconciliation_report.v3.json").resolve()
        payload = (json.dumps(report, sort_keys=True, separators=(",", ":")) + "\n").encode("utf-8")

        wr = write_day_artifact_refreshable_v1(
            path=out_path,
            data=payload,
            expected_day_utc=day,
            expected_schema_id="reconciliation_report",
            expected_schema_version="v3",
            preserve_statuses=(),
        )

        print(f"OK: RECON_REPORT_V3_WRITTEN day_utc={day} status=OK path={wr.path} sha256={wr.sha256} action={wr.action}")
        return 0

    # --- Active-trading mode (submissions present): broker truth required ---
    broker_day_dir = (broker_events_root / day).resolve()
    broker_log = (broker_day_dir / "broker_event_log.v1.jsonl").resolve()
    ok_manifest_path = _find_ok_broker_manifest(broker_day_dir)
    broker_rows = _read_jsonl_rows(broker_log)
    broker_statement_path = (
        truth_root / "execution_evidence_v1" / "broker_statement_normalized_v1" / day / "broker_statement_normalized.v1.json"
    ).resolve()
    position_fact_path = (
        truth_root / "broker_fact_spine_v1" / "fact_ledger" / day / "observed_position_fact.v1.jsonl"
    ).resolve()

    if not broker_log.exists():
        reason_codes.append("MISSING_BROKER_EVENT_LOG")
    if ok_manifest_path is None:
        reason_codes.append("MISSING_OK_BROKER_EVENT_DAY_MANIFEST")

    broker_event_log_sha = _sha256_file(broker_log) if broker_log.exists() else _sha256_bytes(b"")
    input_manifest.append({"type": "broker_event_log_v1_jsonl", "path": str(broker_log), "sha256": broker_event_log_sha})

    broker_events_total = 0
    execdetails_total = 0
    if ok_manifest_path is not None:
        okm_sha = _sha256_file(ok_manifest_path)
        input_manifest.append({"type": "broker_event_day_manifest_ok", "path": str(ok_manifest_path), "sha256": okm_sha})
        okm = _read_json(ok_manifest_path)
        broker_events_total = int(okm.get("log", {}).get("line_count") or 0)
        execdetails_total = int(okm.get("log", {}).get("event_type_counts", {}).get("execDetails") or 0)
    else:
        input_manifest.append({"type": "broker_event_day_manifest_missing", "path": str((broker_day_dir / "broker_event_day_manifest.v1.json").resolve()), "sha256": _sha256_bytes(b"")})

    cmp_status = "OK"
    cmp_reason = "Truth submissions count and broker execDetails count are structurally compatible."
    if "MISSING_BROKER_EVENT_LOG" in reason_codes or "MISSING_OK_BROKER_EVENT_DAY_MANIFEST" in reason_codes:
        cmp_status = "FAIL"
        cmp_reason = "Broker truth missing; reconciliation cannot be performed."
    elif execdetails_expected_total == 0:
        cmp_status = "OK"
        cmp_reason = "Submission-only truth is present without broker-linked execution identifiers; broker execDetails are not yet required."
    elif execdetails_total == 0:
        cmp_status = "FAIL"
        cmp_reason = "Truth submissions exist but broker execDetails count is zero."
        reason_codes.append("BROKER_EXECDETAILS_COUNT_ZERO")

    cash_cmp_status, cash_cmp_reason = _broker_cash_capture(
        broker_rows,
        broker_statement_path=broker_statement_path,
    )
    pos_cmp_status, pos_cmp_reason = _broker_positions_capture(
        broker_rows,
        position_fact_path=position_fact_path,
    )
    if cash_cmp_status != "OK":
        reason_codes.append("MISSING_CASH_BROKER_TRUTH_CAPTURE")
    if pos_cmp_status != "OK":
        reason_codes.append("MISSING_POSITIONS_BROKER_TRUTH_CAPTURE")

    status = "OK" if (cmp_status == "OK" and cash_cmp_status == "OK" and pos_cmp_status == "OK") else "FAIL"
    reason_codes = sorted(set(reason_codes))
    blocker_envelope = build_machine_blocker_envelope_v1(
        closure_state=(CLOSURE_STATE_COMPLETE if status == "OK" else CLOSURE_STATE_BLOCKED),
        reason_codes=reason_codes,
        missing_dependency_artifacts=[],
    )
    constitutional_dependency_declaration = build_artifact_dependency_declaration_v1(
        artifact_type="reconciliation_report_v3",
        artifact_class="outcome_record",
        authority_id="reconciliation_report_v3",
        declared_dependency_artifacts=[],
        dependency_refs=[],
    )
    constitutional_lineage = build_governed_artifact_lineage_v1(
        artifact_type="reconciliation_report_v3",
        artifact_version="v3",
        artifact_class="outcome_record",
        authority_id="reconciliation_report_v3",
        producer_id="ops/tools/run_reconciliation_report_v3.py",
        generated_at_utc=produced_utc,
        effective_at_utc=produced_utc,
        finality_state=FINALITY_PROVISIONAL,
        input_artifact_refs=[],
        policy_snapshot_refs=[],
        code_version=_git_sha(),
        run_id=f"reconciliation_report_v3:{day}",
    )

    report2: Dict[str, Any] = {
        "schema_id": "reconciliation_report",
        "schema_version": "v3",
        "day_utc": day,
        "produced_utc": produced_utc,
        "producer": {"repo": "constellation_2_runtime", "module": "ops/tools/run_reconciliation_report_v3.py", "git_sha": _git_sha()},
        "status": status,
        "reason_codes": reason_codes,
        "blocking_codes": list(blocker_envelope["blocking_codes"]),
        "closure_state": str(blocker_envelope["closure_state"]),
        "first_blocker_code": str(blocker_envelope["first_blocker_code"]),
        "missing_dependency_artifacts": list(blocker_envelope["missing_dependency_artifacts"]),
        "constitutional_dependency_declaration": constitutional_dependency_declaration,
        "constitutional_lineage": constitutional_lineage,
        "notes": notes,
        "input_manifest": input_manifest,
        "broker_side": {
            "broker_event_log_path": str(broker_log),
            "broker_event_log_sha256": broker_event_log_sha,
            "broker_event_manifest_path": str(ok_manifest_path) if ok_manifest_path is not None else str((broker_day_dir / "broker_event_day_manifest.v1.json").resolve()),
            "counts": {"broker_events_total": int(broker_events_total), "execDetails_total": int(execdetails_total)},
        },
        "truth_side": {
            "exec_evidence_day_dir": str(exec_day_dir),
            "submission_ids": truth_ids,
            "counts": {"submissions_total": submissions_total},
        },
        "comparisons": {
            "truth_submissions_vs_broker_execdetails": {"status": cmp_status, "reason": cmp_reason},
            "cash": {"status": cash_cmp_status, "reason": cash_cmp_reason},
            "positions": {"status": pos_cmp_status, "reason": pos_cmp_reason},
        },
    }
    if runtime_lifecycle_ref is not None:
        report2["runtime_lifecycle_ref"] = dict(runtime_lifecycle_ref)

    validate_against_repo_schema_v1(report2, REPO_ROOT, SCHEMA_RELPATH)
    assert_constitutional_writer_allowed_v1(REPO_ROOT, "reconciliation_report_v3", "ops/tools/run_reconciliation_report_v3.py")
    validate_governed_artifact_payload_v1(
        repo_root=REPO_ROOT,
        artifact_id="reconciliation_report_v3",
        payload=report2,
        required_finality_states=["provisional", "finalized", "corrected"],
    )

    out_dir2 = (out_root / day).resolve()
    out_path2 = (out_dir2 / "reconciliation_report.v3.json").resolve()
    payload2 = (json.dumps(report2, sort_keys=True, separators=(",", ":")) + "\n").encode("utf-8")

    wr2 = write_day_artifact_refreshable_v1(
        path=out_path2,
        data=payload2,
        expected_day_utc=day,
        expected_schema_id="reconciliation_report",
        expected_schema_version="v3",
        preserve_statuses=(),
    )

    print(f"OK: RECON_REPORT_V3_WRITTEN day_utc={day} status={status} path={wr2.path} sha256={wr2.sha256} action={wr2.action}")
    return 0 if status == "OK" else 1


if __name__ == "__main__":
    raise SystemExit(main())
