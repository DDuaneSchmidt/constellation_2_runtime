#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import os
import shutil
import subprocess
import sys
from pathlib import Path
from typing import Any, Dict

_THIS_FILE = Path(__file__).resolve()
REPO_ROOT = _THIS_FILE.parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from constellation_2.common.authority_registry_v1 import build_authority_registry
from constellation_2.common.advisor_bridge.promotion_candidate_v1 import PromotionCandidateV1
from constellation_2.common.advisor_bridge.promotion_manual_review_v1 import PromotionManualReviewV1
from constellation_2.common.advisor_bridge.promotion_review_v1 import PromotionReviewV1
from constellation_2.common.metadata_envelope_v1 import metadata_envelope_v1
from constellation_2.common.runtime_base_v1 import advisor_runtime_root

DEFAULT_PROOF_ROOT = Path("/tmp/constellation_2_foundation/final_readiness_proof_v1").resolve()
DEFAULT_DAY = "2026-04-02"
DEFAULT_PRODUCED_UTC = "2026-04-02T14:30:00Z"
DEFAULT_IB_ACCOUNT = "DUO847203"
PHASEC_FIXTURE = (REPO_ROOT / "_smoketest_phasec_2026_04_02").resolve()


def _write_json(path: Path, obj: Dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(obj, sort_keys=True, separators=(",", ":")) + "\n", encoding="utf-8")


def _read_json(path: Path) -> Dict[str, Any]:
    obj = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(obj, dict):
        raise SystemExit(f"FAIL: TOP_LEVEL_NOT_OBJECT: {path}")
    return obj


def _hx(char: str) -> str:
    return char * 64


def _proof_paths(proof_root: Path) -> Dict[str, Path]:
    return {
        "proof_root": proof_root.resolve(),
        "truth_root": (proof_root / "truth_sleeves" / "PRIMARY" / "PAPER").resolve(),
        "replay_root": (proof_root / "replay_truth").resolve(),
        "advisor_output_root": advisor_runtime_root(),
    }


def _phasec_plan() -> Dict[str, Any]:
    return _read_json(PHASEC_FIXTURE / "equity_order_plan.v2.json")


def _intent_snapshot_doc(*, day: str, produced_utc: str, plan: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "schema_id": "exposure_intent",
        "schema_version": "v1",
        "intent_id": str(plan.get("source_intent_id") or f"proof_intent_{day}"),
        "created_at_utc": produced_utc,
        "engine": {
            "engine_id": str(plan.get("engine_id") or "C2_TREND_EQ_PRIMARY_V1"),
            "suite": "C2_HYBRID_V1",
            "mode": "PAPER",
        },
        "underlying": {"symbol": str(plan.get("symbol") or "SPY"), "currency": str(plan.get("currency") or "USD")},
        "exposure_type": "LONG_EQUITY",
        "target_notional_pct": "0.4",
        "expected_holding_days": 3,
        "risk_class": "TREND",
        "constraints": {"max_risk_pct": "0.01"},
        "canonical_json_hash": None,
    }


def _seed_submit_prerequisites(*, truth_root: Path, day: str, produced_utc: str, ib_account: str) -> Dict[str, Any]:
    plan = _phasec_plan()
    intent_hash = str(plan.get("intent_hash") or "").strip()
    intent_sha256 = str(plan.get("intent_sha256") or intent_hash).strip()
    if not intent_sha256:
        raise SystemExit("FAIL: PHASEC_FIXTURE_MISSING_INTENT_HASH")
    snapshot = _intent_snapshot_doc(day=day, produced_utc=produced_utc, plan=plan)
    _write_json(truth_root / "intents_v1" / "snapshots" / day / f"{intent_sha256}.exposure_intent.v1.json", snapshot)
    if intent_hash and intent_hash != intent_sha256:
        _write_json(truth_root / "intents_v1" / "snapshots" / day / f"{intent_hash}.exposure_intent.v1.json", snapshot)
    _write_json(
        truth_root / "intents_v1" / "day_rollup" / day / "intents_day_rollup.v1.json",
        {
            "schema_id": "intents_day_rollup.v1",
            "day_utc": day,
            "produced_utc": produced_utc,
            "producer": {"component": "paper_day_readiness_proof_v1", "version": "v1", "git_sha": "proof"},
            "inputs": {
                "market_data_snapshot_hashes": [_hx("1")],
                "market_calendar_hash": _hx("2"),
                "engine_config_hashes": [_hx("3")],
            },
            "engines": [
                {
                    "engine_id": str(plan.get("engine_id") or "C2_TREND_EQ_PRIMARY_V1"),
                    "intent_type": "exposure_intent.v1",
                    "intent_hashes": [intent_sha256],
                    "intent_count": 1,
                }
            ],
            "rollup_sha256": _hx("4"),
        },
    )
    _write_json(
        truth_root / "run_pointer_v2" / "canonical_authority_head.v1.json",
        {
            "schema_id": "c2_run_pointer_canonical_authority_head",
            "schema_version": "v1",
            "ok": True,
            "day_utc": day,
            "status": "PASS",
            "authoritative": True,
            "points_to": str((truth_root / "reports" / "authorization_gate_verdict_v1" / day / "authorization_gate_verdict.v1.json").resolve()),
        },
    )
    _write_json(
        truth_root / "reports" / "authorization_gate_verdict_v1" / day / "authorization_gate_verdict.v1.json",
        {
            "schema_id": "authorization_gate_verdict_v1",
            "schema_version": 1,
            "day_utc": day,
            "mode": "PAPER",
            "produced_utc": produced_utc,
            "included_gates": [],
            "excluded_gates": [],
            "blocking_gates": [],
            "status": "PASS",
            "blocking_class": "NONE",
            "reason_codes": ["AUTHORIZATION_GATES_PASS"],
            "evidence_refs": [],
            "decision_ledger_ref": "proof",
        },
    )
    _write_json(
        truth_root / "risk_v1" / "kill_switch_v1" / day / "global_kill_switch_state.v1.json",
        {
            "schema_id": "global_kill_switch_state",
            "schema_version": "v1",
            "state": "INACTIVE",
            "allow_entries": True,
            "allow_exits": True,
        },
    )
    _write_json(
        truth_root / "trade_submit_readiness_c2_v1" / "status.json",
        {
            "schema_id": "trade_submit_readiness_c2",
            "schema_version": "v1",
            "ok": True,
            "state": "OK",
            "environment": "PAPER",
            "ib_account": ib_account,
            "provenance": {"truth_root": str(truth_root)},
        },
    )
    _write_json(
        truth_root / "engine_activity_v1" / "authorization_v1" / day / f"{intent_sha256}.authorization.v1.json",
        {
            "schema_id": "C2_AUTHORIZATION_V1",
            "schema_version": 1,
            "produced_utc": produced_utc,
            "day_utc": day,
            "producer": {"repo": "proof", "git_sha": "proof", "module": "run_paper_day_readiness_proof_v1.py"},
            "status": "AUTHORIZED",
            "reason_codes": [],
            "input_manifest": [
                {
                    "type": "intent",
                    "path": str((truth_root / "intents_v1" / "snapshots" / day / f"{intent_sha256}.exposure_intent.v1.json").resolve()),
                    "sha256": intent_sha256,
                    "day_utc": day,
                    "producer": "proof",
                }
            ],
            "engine_id": str(plan.get("engine_id") or "C2_TREND_EQ_PRIMARY_V1"),
            "intent_id": str(snapshot["intent_id"]),
            "intent_hash": intent_sha256,
            "authorization": {
                "decision": "AUTHORIZED",
                "authorized_quantity": int(plan.get("qty_shares") or 1),
                "constraints": [],
                "decision_hash": _hx("5"),
            },
        },
    )
    return {
        "plan": plan,
        "intent_hash": intent_hash,
        "intent_sha256": intent_sha256,
        "intent_id": str(snapshot["intent_id"]),
        "engine_id": str(plan.get("engine_id") or "C2_TREND_EQ_PRIMARY_V1"),
        "symbol": str(plan.get("symbol") or "SPY"),
    }


def _call(script: Path, args: list[str], *, env: Dict[str, str]) -> subprocess.CompletedProcess[str]:
    completed = subprocess.run(
        [sys.executable, str(script)] + args,
        check=False,
        capture_output=True,
        text=True,
        env=env,
    )
    if completed.returncode != 0:
        detail = (completed.stderr or completed.stdout).strip()
        raise SystemExit(f"FAIL: TOOL_NONZERO: {script.name}: {detail}")
    return completed


def _seed_post_submit_truth(*, truth_root: Path, day: str, produced_utc: str, submission_dir: Path, seed: Dict[str, Any]) -> Dict[str, Any]:
    broker_submission = _read_json(submission_dir / "broker_submission_record.v2.json")
    submission_id = str(broker_submission.get("submission_id") or submission_dir.name).strip()
    binding_hash = str(broker_submission.get("binding_hash") or "").strip()
    if not submission_id or not binding_hash:
        raise SystemExit("FAIL: SUBMISSION_OUTPUT_INCOMPLETE")
    _write_json(
        truth_root / "allocation_v1" / "capital_authority_allocation_v1" / day / "capital_authority_allocation.v1.json",
        {
            "schema_id": "capital_authority_allocation",
            "schema_version": "v1",
            "produced_utc": produced_utc,
            "day_utc": day,
            "producer": {"module": "run_paper_day_readiness_proof_v1.py"},
            "input_manifest": [],
            "status": "OK",
        },
    )
    _write_json(
        truth_root / "execution_stream_v1" / day / f"{_hx('6')}.execution_event_stream_record.v1.json",
        {
            "schema_id": "C2_EXECUTION_EVENT_STREAM_RECORD_V1",
            "schema_version": 1,
            "produced_utc": produced_utc,
            "day_utc": day,
            "producer": {"repo": "proof", "git_sha": "proof", "module": "run_paper_day_readiness_proof_v1.py"},
            "status": "OK",
            "reason_codes": [],
            "submission_id": submission_id,
            "binding_hash": binding_hash,
            "engine_id": seed["engine_id"],
            "source_intent_id": seed["intent_id"],
            "intent_sha256": seed["intent_sha256"],
            "broker": {"name": "INTERACTIVE_BROKERS", "environment": "PAPER"},
            "event_type": "EXEC_DETAILS",
            "event_time_utc": produced_utc,
            "observed_at_utc": produced_utc,
            "broker_ids": {"order_id": 101, "perm_id": 202},
            "order_state": {"status": "FILLED", "filled_qty": 1, "remaining_qty": 0, "avg_fill_price": "500.25"},
            "fill": {"fill_qty": 1, "fill_price": "500.25", "commission": "1.00", "currency": "USD"},
            "canonical_json_hash": _hx("7"),
        },
    )
    _write_json(
        truth_root / "fill_ledger_v1" / day / f"{submission_id}.fill_ledger.v1.json",
        {
            "schema_id": "C2_FILL_LEDGER_V1",
            "schema_version": 1,
            "produced_utc": produced_utc,
            "day_utc": day,
            "producer": {"repo": "proof", "git_sha": "proof", "module": "run_paper_day_readiness_proof_v1.py"},
            "status": "OK",
            "reason_codes": [],
            "submission_id": submission_id,
            "binding_hash": binding_hash,
            "engine_id": seed["engine_id"],
            "source_intent_id": seed["intent_id"],
            "intent_sha256": seed["intent_sha256"],
            "order_qty": 1,
            "filled_qty": 1,
            "remaining_qty": 0,
            "avg_fill_price_weighted": "500.25",
        },
    )
    _write_json(
        truth_root / "positions_v1" / "snapshots" / day / "positions_snapshot.v2.json",
        {"schema_id": "positions_snapshot", "schema_version": "v2"},
    )
    _write_json(
        truth_root / "reports" / "gate_stack_verdict_v1" / day / "gate_stack_verdict.v1.json",
        {
            "schema_id": "gate_stack_verdict",
            "schema_version": "v1",
            "produced_utc": produced_utc,
            "day_utc": day,
            "producer": {"repo": "proof", "module": "run_paper_day_readiness_proof_v1.py", "git_sha": "proof"},
            "status": "FAIL",
            "blocking_class": "CLASS1_SYSTEM_HARD_STOP",
            "reason_codes": [],
            "input_manifest": [],
            "gates": [
                {"gate_id": "feed_attestation_gate_v1", "gate_class": "CLASS1_SYSTEM_HARD_STOP", "required": True, "blocking": True, "status": "PASS", "artifact_path": "/tmp/fake/feed.json", "artifact_sha256": _hx("8"), "reason_codes": []},
                {"gate_id": "heartbeat_gate_v1", "gate_class": "CLASS1_SYSTEM_HARD_STOP", "required": True, "blocking": True, "status": "PASS", "artifact_path": "/tmp/fake/hb.json", "artifact_sha256": _hx("9"), "reason_codes": []},
                {"gate_id": "correlation_envelope_gate_v1", "gate_class": "CLASS1_SYSTEM_HARD_STOP", "required": True, "blocking": True, "status": "PASS", "artifact_path": "/tmp/fake/corr.json", "artifact_sha256": _hx("a"), "reason_codes": []},
                {"gate_id": "replay_certification_gate_v1", "gate_class": "CLASS1_SYSTEM_HARD_STOP", "required": True, "blocking": True, "status": "PASS", "artifact_path": "/tmp/fake/replay.json", "artifact_sha256": _hx("b"), "reason_codes": []},
                {"gate_id": "capital_risk_envelope_v2", "gate_class": "CLASS1_SYSTEM_HARD_STOP", "required": True, "blocking": True, "status": "FAIL", "artifact_path": "/tmp/fake/capital.json", "artifact_sha256": _hx("c"), "reason_codes": ["NAV_MISSING"]},
                {"gate_id": "liquidity_slippage_gate_v1", "gate_class": "CLASS1_SYSTEM_HARD_STOP", "required": True, "blocking": True, "status": "FAIL", "artifact_path": "/tmp/fake/liquidity.json", "artifact_sha256": _hx("d"), "reason_codes": ["LIQPOL_MISSING_NAV"]},
                {"gate_id": "operator_daily_gate_v3", "gate_class": "CLASS1_SYSTEM_HARD_STOP", "required": True, "blocking": True, "status": "FAIL", "artifact_path": "/tmp/fake/operator.json", "artifact_sha256": _hx("e"), "reason_codes": ["MISSING_RECONCILIATION_REPORT_V3"]},
            ],
        },
    )
    return {
        "submission_id": submission_id,
    }


def _write_authority_registry(*, advisor_output_root: Path, day: str, produced_utc: str) -> Path:
    env = metadata_envelope_v1(
        produced_utc=produced_utc,
        day_utc=day,
        mode="PAPER",
        source_artifact_refs=[],
        artifact_family="authority_registry_v1",
    )
    path = advisor_output_root / "PAPER" / "reports" / "authority_registry_v1" / day / "authority_registry.v1.json"
    _write_json(path, build_authority_registry(envelope=env).to_dict())
    return path


def _write_publication_inputs(root: Path, *, day: str, produced_utc: str) -> Dict[str, Path]:
    artifact_path = root / "planning_snapshot.json"
    semantic_path = root / "semantic_reconciliation_report.json"
    _write_json(
        artifact_path,
        {
            "schema_id": "planning_snapshot",
            "authority_class": "fact_authority",
            "support_status": "fully_supported",
            "planning_snapshot_id": "planning-snapshot-1",
        },
    )
    _write_json(
        semantic_path,
        {
            "schema_id": "semantic_reconciliation_report",
            "schema_version": "v1",
            "authority_class": "policy_authority",
            "support_status": "fully_supported",
            "produced_utc": produced_utc,
            "run_id": "semantic-report-1",
            "planning_snapshot_id": "planning-snapshot-1",
            "advisory_packet_id": "advisory-packet-1",
            "day_utc": day,
            "checks": [],
            "overall_status": "pass",
        },
    )
    return {"artifact": artifact_path, "semantic": semantic_path}


def _write_promotion_inputs(root: Path, *, produced_utc: str) -> Dict[str, Path]:
    candidate = PromotionCandidateV1(
        schema_id="promotion_candidate",
        schema_version="v1",
        produced_utc=produced_utc,
        run_id="candidate-run-1",
        candidate_id="candidate1",
        proposal_id="proposal1",
        planning_snapshot_id="planning-snapshot-1",
        decision_plan_id="decision-plan-1",
        candidate_status="candidate",
        candidate_class="withdrawal_candidate",
        source_account="paper-account",
        proposed_amount_cents=1000,
        periodicity="monthly",
        source_artifact_refs=("proposal_id:proposal1",),
        notes=("candidate note",),
    )
    review = PromotionReviewV1(
        schema_id="promotion_review",
        schema_version="v1",
        produced_utc=produced_utc,
        run_id="review-run-1",
        review_id="review1",
        candidate_id="candidate1",
        review_status="review_required",
        reason_codes=("REQUIRES_MANUAL_REVIEW",),
        source_artifact_refs=("candidate_id:candidate1",),
        notes=("review note",),
    )
    manual = PromotionManualReviewV1(
        schema_id="promotion_manual_review",
        schema_version="v1",
        produced_utc=produced_utc,
        run_id="manual-run-1",
        manual_review_id="manual1",
        candidate_id="candidate1",
        review_id="review1",
        manual_review_status="approved_for_future_promotion",
        operator_id="operator1",
        operator_notes="approved after review",
        source_artifact_refs=("review_id:review1",),
        selection_basis="approved_for_future_promotion",
    )
    candidate_path = root / "promotion_candidate.json"
    review_path = root / "promotion_review.json"
    manual_path = root / "promotion_manual_review.json"
    _write_json(candidate_path, candidate.to_dict())
    _write_json(review_path, review.to_dict())
    _write_json(manual_path, manual.to_dict())
    return {"candidate": candidate_path, "review": review_path, "manual": manual_path}


def run_readiness_proof(*, proof_root: Path, day: str, produced_utc: str, ib_account: str) -> Dict[str, str]:
    if not str(proof_root).startswith("/tmp/constellation_2_foundation/"):
        raise SystemExit(f"FAIL: proof_root must remain under /tmp/constellation_2_foundation: {proof_root}")
    paths = _proof_paths(proof_root)
    shutil.rmtree(paths["proof_root"], ignore_errors=True)
    paths["truth_root"].mkdir(parents=True, exist_ok=True)
    for stale_dir in [
        paths["advisor_output_root"] / "PAPER" / "reports" / "authority_registry_v1" / day,
        paths["advisor_output_root"] / "PAPER" / "publication_gate_result_v1" / day,
        paths["advisor_output_root"] / "PAPER" / "promotion_gate_result_v1" / day,
    ]:
        shutil.rmtree(stale_dir, ignore_errors=True)
    paths["advisor_output_root"].mkdir(parents=True, exist_ok=True)
    seed = _seed_submit_prerequisites(
        truth_root=paths["truth_root"],
        day=day,
        produced_utc=produced_utc,
        ib_account=ib_account,
    )
    env = os.environ.copy()
    env["C2_TRUTH_ROOT"] = str(paths["truth_root"])
    submit_script = REPO_ROOT / "constellation_2" / "phaseD" / "tools" / "c2_submit_paper_v5.py"
    _call(
        submit_script,
        [
            "--eval_time_utc",
            produced_utc,
            "--phasec_out_dir",
            str(PHASEC_FIXTURE),
            "--risk_budget",
            str((REPO_ROOT / "constellation_2" / "phaseD" / "inputs" / "sample_risk_budget.v1.json").resolve()),
            "--ib_host",
            "127.0.0.1",
            "--ib_port",
            "4002",
            "--ib_client_id",
            "7",
            "--ib_account",
            ib_account,
            "--dry_run",
            "YES",
        ],
        env=env,
    )
    submissions_day = paths["truth_root"] / "execution_evidence_v1" / "submissions" / day
    submission_dirs = sorted([p for p in submissions_day.iterdir() if p.is_dir()]) if submissions_day.exists() else []
    if len(submission_dirs) != 1:
        raise SystemExit(f"FAIL: expected exactly one submission dir in proof path: {submissions_day}")
    submission_dir = submission_dirs[0]
    if not (submission_dir / "broker_submission_record.v2.json").exists():
        raise SystemExit(f"FAIL: missing broker_submission_record.v2.json: {submission_dir}")
    post_submit = _seed_post_submit_truth(
        truth_root=paths["truth_root"],
        day=day,
        produced_utc=produced_utc,
        submission_dir=submission_dir,
        seed=seed,
    )
    plane_produced_utc = f"{day}T00:00:00Z"
    _call(REPO_ROOT / "ops" / "tools" / "run_execution_truth_plane_v1.py", ["--day_utc", day, "--truth_root", str(paths["truth_root"]), "--produced_utc", plane_produced_utc], env=env)
    _call(REPO_ROOT / "ops" / "tools" / "run_gate_authority_plane_v1.py", ["--day_utc", day, "--truth_root", str(paths["truth_root"]), "--produced_utc", plane_produced_utc, "--mode", "PAPER"], env=env)
    _call(REPO_ROOT / "ops" / "tools" / "run_runtime_trace_bundle_v1.py", ["--day_utc", day, "--truth_root", str(paths["truth_root"]), "--submission_id", post_submit["submission_id"]], env=env)
    _call(REPO_ROOT / "ops" / "tools" / "run_runtime_replay_day_v1.py", ["--day_utc", day, "--source_truth_root", str(paths["truth_root"]), "--replay_truth_root", str(paths["replay_root"]), "--submission_id", post_submit["submission_id"]], env=env)
    _write_authority_registry(advisor_output_root=paths["advisor_output_root"], day=day, produced_utc=produced_utc)
    publication_inputs = _write_publication_inputs(paths["proof_root"], day=day, produced_utc=produced_utc)
    _call(
        REPO_ROOT / "ops" / "tools" / "run_publication_gate_v1.py",
        [
            "--artifact_json",
            str(publication_inputs["artifact"]),
            "--semantic_report_json",
            str(publication_inputs["semantic"]),
            "--mode",
            "PAPER",
            "--day_utc",
            day,
            "--produced_utc",
            produced_utc,
            "--output_root",
            str(paths["advisor_output_root"]),
        ],
        env=env,
    )
    promotion_inputs = _write_promotion_inputs(paths["proof_root"], produced_utc=produced_utc)
    _call(
        REPO_ROOT / "ops" / "tools" / "run_promotion_gate_v1.py",
        [
            "--promotion_candidate_json",
            str(promotion_inputs["candidate"]),
            "--promotion_review_json",
            str(promotion_inputs["review"]),
            "--promotion_manual_review_json",
            str(promotion_inputs["manual"]),
            "--mode",
            "PAPER",
            "--day_utc",
            day,
            "--produced_utc",
            produced_utc,
            "--output_root",
            str(paths["advisor_output_root"]),
        ],
        env=env,
    )
    return {
        "proof_root": str(paths["proof_root"]),
        "truth_root": str(paths["truth_root"]),
        "replay_root": str(paths["replay_root"]),
        "advisor_output_root": str(paths["advisor_output_root"]),
        "submission_id": post_submit["submission_id"],
        "execution_truth_gap": str((paths["truth_root"] / "reports" / "execution_completion_gap_report_v1" / day / "execution_completion_gap_report.v1.json").resolve()),
        "authorization_verdict": str((paths["truth_root"] / "reports" / "authorization_gate_verdict_v1" / day / "authorization_gate_verdict.v1.json").resolve()),
        "runtime_trace_bundle": str((paths["truth_root"] / "reports" / "runtime_trace_bundle_v1" / day / f"{post_submit['submission_id']}.runtime_trace_bundle.v1.json").resolve()),
        "replay_manifest": str((paths["replay_root"] / "reports" / "replay_manifest_v1" / day / f"{post_submit['submission_id']}.replay_manifest.v1.json").resolve()),
        "publication_gate_result": str((paths["advisor_output_root"] / "PAPER" / "publication_gate_result_v1" / day / "publication_gate_result.v1.json").resolve()),
        "promotion_gate_result": str((paths["advisor_output_root"] / "PAPER" / "promotion_gate_result_v1" / day / "promotion_gate_result.v1.json").resolve()),
        "authority_registry": str((paths["advisor_output_root"] / "PAPER" / "reports" / "authority_registry_v1" / day / "authority_registry.v1.json").resolve()),
    }


def main() -> int:
    ap = argparse.ArgumentParser(prog="run_paper_day_readiness_proof_v1")
    ap.add_argument("--proof_root", default=str(DEFAULT_PROOF_ROOT))
    ap.add_argument("--day_utc", default=DEFAULT_DAY)
    ap.add_argument("--produced_utc", default=DEFAULT_PRODUCED_UTC)
    ap.add_argument("--ib_account", default=DEFAULT_IB_ACCOUNT)
    args = ap.parse_args()
    result = run_readiness_proof(
        proof_root=Path(str(args.proof_root).strip()).expanduser().resolve(),
        day=str(args.day_utc).strip(),
        produced_utc=str(args.produced_utc).strip(),
        ib_account=str(args.ib_account).strip(),
    )
    print("OK: PAPER_DAY_READINESS_PROOF_V1 " + " ".join(f"{k}={v}" for k, v in sorted(result.items())))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
