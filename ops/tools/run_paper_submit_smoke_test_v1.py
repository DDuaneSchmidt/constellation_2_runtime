#!/usr/bin/env python3
"""
run_paper_submit_smoke_test_v1.py

Execute an explicit TEST-ONLY PAPER submit/evidence smoke path.

This path is separate from the normal orchestrator. It is fail-closed, PAPER-only,
and uses the existing execution package, submission record, submit boundary, and
evidence lifecycle machinery.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import subprocess
import sys
from pathlib import Path
from typing import Any, Dict

REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from constellation_2.common.advisory.execution_intent_v1 import ExecutionIntentV1
from constellation_2.common.execution_kernel.execution_kernel_runner_v1 import run_execution_kernel_v1
from constellation_2.common.execution_kernel.execution_submission_record_v1 import ExecutionSubmissionRecordV1
from constellation_2.common.paper_execution_authority_v1 import (
    resolve_governed_paper_execution_profile,
    resolve_governed_paper_execution_roots,
)
from constellation_2.common.paper_session_path_alignment_v1 import (
    normalize_paper_submit_smoke_test_request_nonce,
    resolve_paper_submit_smoke_test_request_path,
)
from constellation_2.common.runtime_contract_v1 import resolve_canonical_truth_root
from constellation_2.common.execution_build_authority_v1 import run_execution_build_authority_v1
from constellation_2.common.advisory.execution_package_builder_from_execution_intent_v1 import (
    derive_execution_submission_identity_from_execution_intent_v1,
    stage_candidate_from_execution_intent_v1,
)
from constellation_2.phaseD.lib.canon_json_v1 import canonical_hash_for_c2_artifact_v1

POLICY_PATH = (REPO_ROOT / "governance" / "02_REGISTRIES" / "C2_PAPER_SUBMIT_SMOKE_TEST_POLICY_V1.json").resolve()
RISK_BUDGET_PATH = (REPO_ROOT / "constellation_2" / "phaseD" / "inputs" / "sample_risk_budget.v1.json").resolve()


def _require_dir(raw: str, *, label: str) -> Path:
    p = Path(str(raw).strip()).expanduser().resolve()
    if not p.is_absolute() or not p.exists() or not p.is_dir():
        raise SystemExit(f"FAIL: invalid --{label}: {p}")
    return p


def _require_day(day_utc: str) -> str:
    day = str(day_utc).strip()
    if len(day) != 10 or day[4] != "-" or day[7] != "-":
        raise SystemExit(f"FAIL: bad --day_utc: {day!r}")
    return day


def _require_eval_time(eval_time_utc: str, *, day_utc: str) -> str:
    value = str(eval_time_utc).strip()
    if not value.endswith("Z") or "T" not in value:
        raise SystemExit(f"FAIL: bad --eval_time_utc: {value!r}")
    if not value.startswith(f"{day_utc}T"):
        raise SystemExit(f"FAIL: eval_time_utc day mismatch: day_utc={day_utc} eval_time_utc={value}")
    return value


def _read_json_obj(path: Path) -> Dict[str, Any]:
    try:
        obj = json.loads(path.read_text(encoding="utf-8"))
    except Exception as e:
        raise SystemExit(f"FAIL: cannot parse json: {path}: {e!r}") from e
    if not isinstance(obj, dict):
        raise SystemExit(f"FAIL: json top-level not object: {path}")
    return obj


def _require_str(obj: Dict[str, Any], key: str) -> str:
    value = obj.get(key)
    if not isinstance(value, str) or not value.strip():
        raise SystemExit(f"FAIL: REQUIRED_STRING_MISSING: {key}")
    return value.strip()


def _sha256_file(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _git_sha() -> str:
    try:
        out = subprocess.check_output(["/usr/bin/git", "-C", str(REPO_ROOT), "rev-parse", "HEAD"], text=True)
        value = str(out).strip()
        if value:
            return value
    except Exception:
        pass
    return "0" * 40


def _load_policy() -> Dict[str, Any]:
    if not POLICY_PATH.exists() or not POLICY_PATH.is_file():
        raise SystemExit(f"FAIL: POLICY_PATH_MISSING: {POLICY_PATH}")
    policy = _read_json_obj(POLICY_PATH)
    if _require_str(policy, "schema_id") != "C2_PAPER_SUBMIT_SMOKE_TEST_POLICY_V1":
        raise SystemExit(f"FAIL: POLICY_SCHEMA_ID_INVALID: {POLICY_PATH}")
    if _require_str(policy.get("scope") if isinstance(policy.get("scope"), dict) else {}, "environment") != "PAPER":
        raise SystemExit("FAIL: POLICY_ENVIRONMENT_INVALID")
    rules = policy.get("smoke_test_rules")
    if not isinstance(rules, dict) or rules.get("test_only") is not True:
        raise SystemExit("FAIL: POLICY_RULES_INVALID")
    return policy


def _load_request(
    *,
    operator_input_root: Path,
    day_utc: str,
    ib_account: str,
    request_nonce: str,
) -> tuple[Path, Dict[str, Any]]:
    request_path = resolve_paper_submit_smoke_test_request_path(
        operator_input_root=operator_input_root,
        day_utc=day_utc,
        request_nonce=request_nonce,
    )
    if not request_path.exists() or not request_path.is_file():
        raise SystemExit(f"FAIL: SMOKE_REQUEST_MISSING: {request_path}")
    request = _read_json_obj(request_path)
    if _require_str(request, "environment") != "PAPER":
        raise SystemExit("FAIL: SMOKE_REQUEST_ENVIRONMENT_INVALID")
    if request.get("test_only") is not True:
        raise SystemExit("FAIL: SMOKE_REQUEST_TEST_ONLY_REQUIRED")
    if _require_str(request, "day_utc") != day_utc:
        raise SystemExit(f"FAIL: SMOKE_REQUEST_DAY_MISMATCH: day_utc={day_utc}")
    if _require_str(request, "ib_account") != ib_account:
        raise SystemExit(f"FAIL: SMOKE_REQUEST_ACCOUNT_MISMATCH: ib_account={ib_account}")
    if str(request.get("request_nonce") or "").strip() != request_nonce:
        raise SystemExit(
            "FAIL: SMOKE_REQUEST_NONCE_MISMATCH: "
            f"expected_request_nonce={request_nonce!r} request_path={request_path}"
        )
    return request_path, request


def build_smoke_execution_intent_v1(
    *,
    day_utc: str,
    eval_time_utc: str,
    ib_account: str,
    request_path: Path,
    request_obj: Dict[str, Any],
    policy: Dict[str, Any],
) -> ExecutionIntentV1:
    instrument = request_obj.get("instrument")
    if not isinstance(instrument, dict):
        raise SystemExit("FAIL: SMOKE_REQUEST_INSTRUMENT_INVALID")
    order_terms = request_obj.get("order_terms")
    if not isinstance(order_terms, dict):
        raise SystemExit("FAIL: SMOKE_REQUEST_ORDER_TERMS_INVALID")
    quantity_shares = int(request_obj.get("quantity_shares") or 0)
    if quantity_shares <= 0:
        raise SystemExit("FAIL: SMOKE_REQUEST_QUANTITY_INVALID")
    request_id = _require_str(request_obj, "request_id")
    request_nonce = str(request_obj.get("request_nonce") or "").strip()
    source_refs = [
        f"paper_submit_smoke_test_request_path:{request_path.resolve()}",
        f"paper_submit_smoke_test_policy_path:{POLICY_PATH}",
        "paper_submit_smoke_test_marker:TEST_ONLY",
        "paper_submit_smoke_test_environment:PAPER",
    ]
    obj = {
        "schema_id": "execution_intent",
        "schema_version": "v1",
        "record_id": request_id,
        "execution_intent_id": request_id,
        "promotion_record_id": f"paper-smoke:{request_id}",
        "household_id": "PAPER_SMOKE_TEST",
        "created_at_utc": eval_time_utc,
        "effective_at_utc": eval_time_utc,
        "actor_source": "paper_submit_smoke_test_v1",
        "contract_version": "execution_intent_contract_v1",
        "builder_version": "execution_intent_builder_v1",
        "idempotency_key": request_id,
        "operation_type": "fresh_paper_entry_v1",
        "day_utc": day_utc,
        "environment": "PAPER",
        "sleeve_id": _require_str(request_obj, "sleeve_id"),
        "account_id": ib_account,
        "engine_id": _require_str(request_obj, "engine_id"),
        "instrument": {
            "kind": _require_str(instrument, "kind"),
            "symbol": _require_str(instrument, "symbol"),
            "currency": _require_str(instrument, "currency"),
            "ib_conId": int(instrument.get("ib_conId") or 0),
            "ib_localSymbol": _require_str(instrument, "ib_localSymbol"),
        },
        "side": _require_str(request_obj, "side"),
        "quantity_shares": quantity_shares,
        "order_terms": dict(order_terms),
        "parent_lineage_refs": [
            f"paper_submit_smoke_test_request_id:{request_id}",
            f"paper_submit_smoke_test_policy_id:{_require_str(policy, 'policy_id')}",
        ],
        "source_artifact_refs": source_refs,
        "canonical_json_hash": None,
    }
    if request_nonce:
        obj["parent_lineage_refs"].append(f"paper_submit_smoke_test_request_nonce:{request_nonce}")
        obj["source_artifact_refs"].append(f"paper_submit_smoke_test_request_nonce:{request_nonce}")
    obj["canonical_json_hash"] = canonical_hash_for_c2_artifact_v1({**obj, "canonical_json_hash": None})
    return ExecutionIntentV1.from_dict(obj)


def build_smoke_authorization_obj(
    *,
    day_utc: str,
    produced_utc: str,
    intent: ExecutionIntentV1,
    request_path: Path,
    request_obj: Dict[str, Any],
    policy: Dict[str, Any],
) -> Dict[str, Any]:
    git_sha = _git_sha()
    rules = policy["smoke_test_rules"]
    request_sha = _sha256_file(request_path)
    decision_hash = canonical_hash_for_c2_artifact_v1(
        {
            "intent_hash": intent.idempotency_key,
            "day_utc": day_utc,
            "decision": "AUTHORIZED",
            "authorized_quantity": int(request_obj["quantity_shares"]),
            "policy_sha256": _sha256_file(POLICY_PATH),
        }
    )
    obj = {
        "schema_id": "C2_AUTHORIZATION_V1",
        "schema_version": 1,
        "produced_utc": produced_utc,
        "day_utc": day_utc,
        "producer": {
            "repo": REPO_ROOT.name,
            "git_sha": git_sha,
            "module": "ops/tools/run_paper_submit_smoke_test_v1.py",
        },
        "status": "AUTHORIZED",
        "reason_codes": [
            _require_str(rules, "authorization_reason_code"),
            "PAPER_ONLY",
            "TEST_ONLY",
        ],
        "input_manifest": [
            {
                "type": "paper_submit_smoke_test_request_v1",
                "path": str(request_path.resolve()),
                "sha256": request_sha,
                "day_utc": day_utc,
                "producer": "ensure_paper_submit_smoke_test_request_v1.py",
            },
            {
                "type": "paper_submit_smoke_test_policy_v1",
                "path": str(POLICY_PATH),
                "sha256": _sha256_file(POLICY_PATH),
                "day_utc": day_utc,
                "producer": "governance",
            },
        ],
        "engine_id": intent.engine_id,
        "intent_id": intent.execution_intent_id,
        "intent_hash": intent.idempotency_key,
        "authorization": {
            "decision": "AUTHORIZED",
            "authorized_quantity": int(request_obj["quantity_shares"]),
            "constraints": [str(x) for x in rules.get("authorization_constraint_codes") or [] if str(x).strip()],
            "decision_hash": decision_hash,
        },
    }
    return obj


def _write_smoke_authorization(
    *,
    canonical_truth_root: Path,
    day_utc: str,
    intent: ExecutionIntentV1,
    request_path: Path,
    request_obj: Dict[str, Any],
    policy: Dict[str, Any],
    produced_utc: str,
) -> Path:
    out_path = (
        canonical_truth_root
        / "engine_activity_v1"
        / "authorization_v1"
        / day_utc
        / f"{intent.idempotency_key}.authorization.v1.json"
    ).resolve()
    obj = build_smoke_authorization_obj(
        day_utc=day_utc,
        produced_utc=produced_utc,
        intent=intent,
        request_path=request_path,
        request_obj=request_obj,
        policy=policy,
    )
    out_path.parent.mkdir(parents=True, exist_ok=True)
    payload = json.dumps(obj, ensure_ascii=False, sort_keys=True, indent=2) + "\n"
    if out_path.exists():
        existing = out_path.read_text(encoding="utf-8")
        if existing != payload:
            raise SystemExit(f"FAIL: SMOKE_AUTHORIZATION_IMMUTABLE_CONFLICT: {out_path}")
    else:
        out_path.write_text(payload, encoding="utf-8")
    return out_path


def _report_path(*, sleeve_truth_root: Path, day_utc: str) -> Path:
    return (
        sleeve_truth_root
        / "reports"
        / "paper_submit_smoke_test_v1"
        / day_utc
        / "paper_submit_smoke_test.v1.json"
    ).resolve()


def run_paper_submit_smoke_test_v1(
    *,
    day_utc: str,
    eval_time_utc: str,
    operator_input_root: Path,
    ib_account: str,
    request_nonce: str,
    allow_broker_transmit: str,
) -> Dict[str, Any]:
    if str(allow_broker_transmit).strip().upper() != "YES":
        raise SystemExit("FAIL: BROKER_TRANSMIT_NOT_EXPLICITLY_APPROVED")
    policy = _load_policy()
    request_path, request = _load_request(
        operator_input_root=operator_input_root,
        day_utc=day_utc,
        ib_account=ib_account,
        request_nonce=request_nonce,
    )

    if _require_str(request, "environment") != "PAPER":
        raise SystemExit("FAIL: SMOKE_REQUEST_NOT_PAPER")

    execution_profile = resolve_governed_paper_execution_profile(
        repo_root=REPO_ROOT,
        environment="PAPER",
        ib_account=ib_account,
        sleeve_id=_require_str(request, "sleeve_id"),
    )
    execution_roots = resolve_governed_paper_execution_roots(
        repo_root=REPO_ROOT,
        environment="PAPER",
        ib_account=ib_account,
        sleeve_id=_require_str(request, "sleeve_id"),
    )
    canonical_truth_root = resolve_canonical_truth_root().resolve()
    sleeve_truth_root = execution_roots.execution_root_path.resolve()

    intent = build_smoke_execution_intent_v1(
        day_utc=day_utc,
        eval_time_utc=eval_time_utc,
        ib_account=ib_account,
        request_path=request_path,
        request_obj=request,
        policy=policy,
    )
    authorization_path = _write_smoke_authorization(
        canonical_truth_root=canonical_truth_root,
        day_utc=day_utc,
        intent=intent,
        request_path=request_path,
        request_obj=request,
        policy=policy,
        produced_utc=eval_time_utc,
    )
    staged = stage_candidate_from_execution_intent_v1(repo_root=REPO_ROOT, execution_intent=intent)
    candidate_path = Path(staged["candidate_path"]).resolve()
    build_result = run_execution_build_authority_v1(
        repo_root=REPO_ROOT,
        operation_type=intent.operation_type,
        candidate_path=candidate_path,
        materialize=True,
        emit_package=True,
    )
    build_obj = build_result.get("build_obj") if isinstance(build_result.get("build_obj"), dict) else {}
    if str(build_obj.get("closure_status") or "").strip().upper() != "COMPLETE":
        raise SystemExit(f"FAIL: SMOKE_EXECUTION_BUILD_NOT_COMPLETE: {build_obj.get('first_real_blocker')}")

    kernel_run_id = canonical_hash_for_c2_artifact_v1(
        {
            "paper_submit_smoke_test_request_id": request["request_id"],
            "day_utc": day_utc,
            "eval_time_utc": eval_time_utc,
            "ib_account": ib_account,
        }
    )
    kernel_result = run_execution_kernel_v1(
        repo_root=REPO_ROOT,
        truth_root=sleeve_truth_root,
        run_id=f"paper-submit-smoke:{kernel_run_id}",
        execution_intent=intent,
        produced_utc=eval_time_utc,
        eval_time_utc=eval_time_utc,
        risk_budget_path=RISK_BUDGET_PATH,
        ib_host=execution_profile.host,
        ib_port=int(execution_profile.port),
        ib_client_id=int(execution_profile.client_id_orders),
        dry_run=False,
        submissions_root_override=None,
    )

    submission_record = kernel_result.get("submission_record")
    submission_id = ""
    submission_record_path = ""
    if isinstance(submission_record, ExecutionSubmissionRecordV1):
        submission_id = submission_record.submission_id
        submission_record_path = str(
            (
                sleeve_truth_root
                / "execution_kernel_v1"
                / "submission_records"
            ).resolve()
        )
    elif submission_record is not None and hasattr(submission_record, "submission_id"):
        submission_id = str(submission_record.submission_id)
    elif kernel_result.get("submission_decision") is not None:
        decision = kernel_result["submission_decision"]
        submission_id = str(getattr(decision, "predicted_submission_id", "") or "")

    broker_submission_record_path = ""
    if submission_id:
        candidate_bsr = (
            sleeve_truth_root
            / "execution_evidence_v1"
            / "submissions"
            / day_utc
            / submission_id
            / "broker_submission_record.v2.json"
        ).resolve()
        if candidate_bsr.exists():
            broker_submission_record_path = str(candidate_bsr)

    report_obj = {
        "schema_id": "paper_submit_smoke_test",
        "schema_version": "v1",
        "day_utc": day_utc,
        "produced_utc": eval_time_utc,
        "test_only": True,
        "environment": "PAPER",
        "request_path": str(request_path.resolve()),
        "request_nonce": str(request.get("request_nonce") or ""),
        "policy_path": str(POLICY_PATH),
        "authorization_path": str(authorization_path),
        "candidate_path": str(candidate_path),
        "execution_build_path": str(build_result.get("build_path") or ""),
        "execution_package_path": str(build_result.get("package_path") or ""),
        "submission_id": submission_id,
        "broker_submission_record_path": broker_submission_record_path,
        "kernel_run_outcome": str(kernel_result.get("run_outcome") or ""),
        "reason_codes": list(kernel_result.get("reason_codes") or []),
    }
    report_path = _report_path(sleeve_truth_root=sleeve_truth_root, day_utc=day_utc)
    report_path.parent.mkdir(parents=True, exist_ok=True)
    report_path.write_text(json.dumps(report_obj, ensure_ascii=False, sort_keys=True, indent=2) + "\n", encoding="utf-8")
    return {
        "report_path": str(report_path),
        "request_path": str(request_path.resolve()),
        "authorization_path": str(authorization_path),
        "candidate_path": str(candidate_path),
        "package_path": str(build_result.get("package_path") or ""),
        "submission_id": submission_id,
        "broker_submission_record_path": broker_submission_record_path,
        "kernel_run_outcome": str(kernel_result.get("run_outcome") or ""),
        "reason_codes": list(kernel_result.get("reason_codes") or []),
    }


def main() -> int:
    ap = argparse.ArgumentParser(prog="run_paper_submit_smoke_test_v1")
    ap.add_argument("--day_utc", required=True, help="YYYY-MM-DD")
    ap.add_argument("--eval_time_utc", required=True, help="UTC ISO-8601 Z timestamp")
    ap.add_argument("--operator_input_root", required=True, help="Operator input root")
    ap.add_argument("--ib_account", required=True, help="IB paper account id (DU*)")
    ap.add_argument("--environment", required=True, choices=["PAPER", "LIVE"])
    ap.add_argument("--request_nonce", default="", help="Optional same-day smoke request nonce")
    ap.add_argument("--allow_broker_transmit", required=True, choices=["YES", "NO"])
    args = ap.parse_args()

    if str(args.environment).strip().upper() != "PAPER":
        raise SystemExit(f"FAIL: PAPER_ONLY_TOOL: environment={args.environment}")
    day_utc = _require_day(args.day_utc)
    eval_time_utc = _require_eval_time(args.eval_time_utc, day_utc=day_utc)
    operator_input_root = _require_dir(args.operator_input_root, label="operator_input_root")
    ib_account = str(args.ib_account).strip()
    if not ib_account.startswith("DU"):
        raise SystemExit(f"FAIL: IB_ACCOUNT_NOT_PAPER_ID: {ib_account}")
    try:
        request_nonce = normalize_paper_submit_smoke_test_request_nonce(request_nonce=args.request_nonce)
    except ValueError as exc:
        raise SystemExit(f"FAIL: {exc}") from exc

    result = run_paper_submit_smoke_test_v1(
        day_utc=day_utc,
        eval_time_utc=eval_time_utc,
        operator_input_root=operator_input_root,
        ib_account=ib_account,
        request_nonce=request_nonce,
        allow_broker_transmit=args.allow_broker_transmit,
    )
    print("OK: PAPER_SUBMIT_SMOKE_TEST_V1 " + " ".join(f"{k}={v}" for k, v in sorted(result.items())))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
