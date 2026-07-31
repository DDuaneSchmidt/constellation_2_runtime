from __future__ import annotations

import json
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[3]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from constellation_2.common.aegis_chatgpt_control_packet_v1 import build_aegis_chatgpt_control_packet_v1
from ops.aegis.runtime_truth_kernel_v1 import build_runtime_state_snapshot_v1, build_runtime_truth_kernel_v1, write_runtime_truth_kernel_reports_v1
from ops.aegis.decision_ledger_v1 import decision_trace_json_path_v1
from ops.tools.build_aegis_audit_handoff_v1 import build_handoff_text_v1
from ops.tools import build_aegis_audit_handoff_v1 as audit_cli
from ops.tools import build_aegis_chatgpt_control_packet_v1 as packet_cli
from ops.tools import capture_manual_trade_receipt_v1 as capture_cli
from ops.tools import diff_aegis_runtime_state_v1 as diff_cli
from ops.tools import replay_aegis_runtime_state_v1 as replay_cli
from ops.tools import run_aegis_runtime_truth_kernel_v1 as kernel_cli
from ops.tools import write_aegis_daily_operator_v1 as daily_cli
from ops.tools.write_aegis_runtime_evidence_completion_v1 import write_evidence_completion_report_v1


DAY = "2026-05-15"
NOW = "2026-05-15T20:55:00Z"
OLD = "2026-05-10T20:55:00Z"


def test_missing_required_artifact_causes_partial_context(tmp_path: Path) -> None:
    payload = build_runtime_truth_kernel_v1(truth_root=tmp_path, day_utc=DAY, generated_at_utc=NOW)

    assert payload["runtime_truth_classification"] == "PARTIAL_CONTEXT"
    assert any(row["artifact_id"] == "research_dataset_binding" and row["status"] == "MISSING" for row in payload["missing_or_stale_sources"])


def test_stale_artifact_causes_partial_context(tmp_path: Path) -> None:
    _write_all_kernel_artifacts(tmp_path, generated_at=OLD)

    payload = build_runtime_truth_kernel_v1(truth_root=tmp_path, day_utc=DAY, generated_at_utc=NOW)

    assert payload["runtime_truth_classification"] == "PARTIAL_CONTEXT"
    assert any(row["status"] == "STALE" for row in payload["missing_or_stale_sources"])


def test_recovery_plan_emits_exact_command(tmp_path: Path) -> None:
    payload = build_runtime_truth_kernel_v1(truth_root=tmp_path, day_utc=DAY, generated_at_utc=NOW)
    research = next(row for row in payload["recovery_plan"] if row["artifact_id"] == "research_dataset_binding")

    assert research["generated_by_command"] == f"python3 ops/tools/audit_research_dataset_bindings_v1.py --truth_root {tmp_path.resolve()} --day_utc {DAY}"
    assert research["validates_with_command"] == "npm run aegis:audit"

    feedback = next(row for row in payload["recovery_plan"] if row["artifact_id"] == "ai_feedback_review")
    assert feedback["generated_by_command"] == f"python3 ops/tools/build_ai_eod_feedback_review_v1.py --truth_root {tmp_path.resolve()} --day {DAY}"


def test_trade_advice_remains_blocked_when_research_binding_is_stale(tmp_path: Path) -> None:
    _write_all_kernel_artifacts(tmp_path)
    _write_json(
        tmp_path / f"reports/research_dataset_gap_v1/{DAY}/research_dataset_gap.v1.json",
        _artifact("research_dataset_gap", "research_dataset_gap_v1", generated_at=OLD),
    )

    payload = build_runtime_truth_kernel_v1(truth_root=tmp_path, day_utc=DAY, generated_at_utc=NOW)

    assert payload["dependency_graph"]["RESEARCH_READY"]["allowed"] is False
    assert payload["dependency_graph"]["TRADE_ADVICE_ALLOWED"]["allowed"] is False
    assert "research_dataset_binding" in payload["dependency_graph"]["RESEARCH_READY"]["missing_or_blocking_artifacts"]


def test_manual_trade_capture_does_not_require_broker_lifecycle_proof(tmp_path: Path) -> None:
    _write_all_kernel_artifacts(tmp_path, omit={"broker_lifecycle_proof"})

    payload = build_runtime_truth_kernel_v1(truth_root=tmp_path, day_utc=DAY, generated_at_utc=NOW)

    assert payload["dependency_graph"]["BROKER_LIFECYCLE_PROVEN"]["allowed"] is False
    assert payload["dependency_graph"]["BROKER_LIFECYCLE_PROVEN"]["readiness_relevant"] is False
    assert payload["dependency_graph"]["MANUAL_TRADE_CAPTURE_ALLOWED"]["allowed"] is True
    assert payload["broker_submit_required"] is False
    assert payload["autonomous_execution_allowed"] is False
    assert "Do not claim broker submit/transmit" in "\n".join(payload["do_not_claim"])


def test_claim_guard_emits_do_not_claim_items(tmp_path: Path) -> None:
    _write_all_kernel_artifacts(tmp_path, ai_used=False, omit={"alert_transport_proof", "promoted_candidate_evidence"})

    payload = build_runtime_truth_kernel_v1(truth_root=tmp_path, day_utc=DAY, generated_at_utc=NOW)
    joined = "\n".join(payload["do_not_claim"])

    assert "AI feedback is deterministic fallback only" in joined
    assert "Live email/SMS transport is not proven" in joined
    assert "No real promoted runtime actionable candidate" in joined
    assert payload["claim_violation_count"] >= 3


def test_existing_audit_and_control_packet_commands_still_run(tmp_path: Path, capsys) -> None:
    assert packet_cli.main(["--truth_root", str(tmp_path), "--day", DAY, "--generated_at_utc", NOW, "--json"]) == 0
    capsys.readouterr()

    assert audit_cli.main(["--truth_root", str(tmp_path), "--day", DAY]) == 0
    out = capsys.readouterr().out

    assert "AEGIS AUDIT HANDOFF" in out
    assert "runtime_truth_kernel_path:" in out
    assert "runtime_state_snapshot_path:" in out
    assert "runtime_state_transition_path:" in out
    assert "runtime_invalidations_path:" in out


def test_paper_review_is_distinct_from_trade_advice(tmp_path: Path) -> None:
    _write_all_kernel_artifacts(tmp_path)
    _write_json(
        tmp_path / f"reports/aegis_candidate_review_packet_v1/{DAY}/candidate_review_packet.v1.json",
        {
            "schema_id": "candidate_review_packet",
            "schema_version": "v1",
            "artifact_id": "candidate_review_packet",
            "day_utc": DAY,
            "generated_at_utc": NOW,
            "operating_mode": "HUMAN_REVIEWED_PAPER_MODE",
            "candidate_count": 1,
            "review_candidates": [{"candidate_id": "candidate-1", "paper_trade_eligible": True, "live_trade_eligible": False}],
            "safety": {"paper_only": True},
        },
    )
    _write_json(
        tmp_path / f"reports/aegis_paper_review_queue_v1/{DAY}/paper_review_queue.v1.json",
        {
            "schema_id": "paper_review_queue",
            "schema_version": "v1",
            "artifact_id": "paper_review_queue",
            "day_utc": DAY,
            "generated_at_utc": NOW,
            "operating_mode": "HUMAN_REVIEWED_PAPER_MODE",
            "rows": [{"candidate_id": "candidate-1", "status": "AWAITING_REVIEW", "operator_decision_required": True}],
            "status_counts": {"AWAITING_REVIEW": 1},
            "safety": {"paper_only": True},
        },
    )

    payload = build_runtime_truth_kernel_v1(truth_root=tmp_path, day_utc=DAY, generated_at_utc=NOW)

    assert payload["dependency_graph"]["PAPER_REVIEW_ALLOWED"]["allowed"] is True
    assert payload["dependency_graph"]["MANUAL_PAPER_RECEIPT_ALLOWED"]["allowed"] is True
    assert payload["dependency_graph"]["TRADE_ADVICE_ALLOWED"]["allowed"] is False
    assert payload["broker_submit_required"] is False
    assert payload["autonomous_execution_allowed"] is False


def _write_paper_candidate_evidence(root: Path, *, day_utc: str = DAY, generated_at: str = NOW, count: int = 17) -> None:
    candidates = [
        {
            "candidate_id": f"paper-candidate-{idx}",
            "symbol": "QQQ",
            "paper_trade_eligible": True,
            "live_trade_eligible": False,
            "operator_review_required": True,
        }
        for idx in range(count)
    ]
    _write_json(
        root / f"reports/aegis_candidate_review_packet_v1/{day_utc}/candidate_review_packet.v1.json",
        {
            "schema_id": "candidate_review_packet",
            "schema_version": "v1",
            "artifact_id": "candidate_review_packet",
            "day_utc": day_utc,
            "generated_at_utc": generated_at,
            "operating_mode": "HUMAN_REVIEWED_PAPER_MODE",
            "candidate_count": count,
            "review_candidates": candidates,
            "safety": {
                "paper_only": True,
                "trade_advice_allowed": False,
                "broker_submit_transmit_allowed": False,
                "autonomous_execution_allowed": False,
                "live_trade_eligible": False,
            },
        },
    )
    _write_json(
        root / f"reports/aegis_paper_review_queue_v1/{day_utc}/paper_review_queue.v1.json",
        {
            "schema_id": "paper_review_queue",
            "schema_version": "v1",
            "artifact_id": "paper_review_queue",
            "day_utc": day_utc,
            "generated_at_utc": generated_at,
            "operating_mode": "HUMAN_REVIEWED_PAPER_MODE",
            "rows": [{**candidate, "status": "AWAITING_REVIEW"} for candidate in candidates],
            "status_counts": {"AWAITING_REVIEW": count},
            "safety": {
                "paper_only": True,
                "trade_advice_allowed": False,
                "broker_submit_transmit_allowed": False,
                "autonomous_execution_allowed": False,
                "live_trade_eligible": False,
            },
        },
    )


def _write_paper_creation_evidence(root: Path, *, day_utc: str = DAY, generated_at: str = NOW, authorized: bool = True) -> None:
    _write_json(
        root / f"reports/paper_session_authority_v1/{day_utc}/paper_session_authority.v1.json",
        {
            "schema_id": "paper_session_authority",
            "schema_version": "v1",
            "day_utc": day_utc,
            "produced_utc": generated_at,
            "authority_status": "GRANTED" if authorized else "DENIED",
            "submission_authorized": authorized,
            "paper_open_allowed": authorized,
        },
    )
    _write_json(
        root / f"reports/paper_session_ledger_v1/{day_utc}/paper_session_ledger.v1.json",
        {
            "schema_id": "paper_session_ledger",
            "schema_version": "v1",
            "day_utc": day_utc,
            "evaluated_at_utc": generated_at,
            "submit_lifecycle": {"mode": "SIMULATED_PAPER"},
            "post_submit_lifecycle": {"receipt_required_after_submit": True},
        },
    )
    _write_json(
        root / f"reports/paper_trade_construction_v1/{day_utc}/paper_trade_construction.v1.json",
        {
            "schema_id": "paper_trade_construction",
            "schema_version": "v1",
            "artifact_id": f"paper_trade_construction_v1:{day_utc}:test",
            "source_day": day_utc,
            "generated_at_utc": generated_at,
            "paper_submit_created": authorized,
            "trade_construction_status": "complete" if authorized else "blocked_missing_market_data",
            "broker_execution_allowed": False,
            "live_trading_allowed": False,
            "order_routing_allowed": False,
            "constructed_paper_trade_count": 1 if authorized else 0,
            "constructed_paper_trades": [{"candidate_id": "paper-candidate-1", "symbol": "QQQ", "scope": "paper_only", "broker_execution_allowed": False, "live_trading_allowed": False, "order_routing_allowed": False}] if authorized else [],
            "market_data_diagnostics": [] if authorized else [{"candidate_id": "paper-candidate-1", "symbol": "QQQ", "missing_field": "market_data.value", "expected_source_artifact": "market_data_inputs_v1", "artifact_path_checked": str(root / f"reports/market_data_inputs_v1/{day_utc}/market_data_inputs.v1.json"), "status": "ABSENT"}],
        },
    )


def test_paper_creation_without_open_authorization_is_blocked_precisely(tmp_path: Path) -> None:
    _write_all_kernel_artifacts(tmp_path, omit={"manual_trade_packet", "promoted_candidate_evidence", "broker_lifecycle_proof"})
    _write_paper_candidate_evidence(tmp_path, count=17)
    _write_paper_creation_evidence(tmp_path, authorized=False)

    payload = build_runtime_truth_kernel_v1(truth_root=tmp_path, day_utc=DAY, generated_at_utc=NOW)
    blockers = payload["dependency_graph"]["PAPER_TRADE_CREATION_ALLOWED"]["missing_or_blocking_artifacts"]

    assert payload["dependency_graph"]["PAPER_TRADE_CREATION_ALLOWED"]["allowed"] is False
    assert "paper_session_authority:PAPER_OPEN_NOT_AUTHORIZED" in blockers


def test_paper_creation_with_missing_market_data_reports_symbol_field_and_path(tmp_path: Path) -> None:
    _write_all_kernel_artifacts(tmp_path, omit={"manual_trade_packet", "promoted_candidate_evidence", "broker_lifecycle_proof"})
    _write_paper_candidate_evidence(tmp_path, count=17)
    _write_paper_creation_evidence(tmp_path, authorized=True)
    construction_path = tmp_path / f"reports/paper_trade_construction_v1/{DAY}/paper_trade_construction.v1.json"
    construction = json.loads(construction_path.read_text(encoding="utf-8"))
    construction["paper_submit_created"] = False
    construction["trade_construction_status"] = "blocked_missing_market_data"
    construction["blocker_codes"] = ["MISSING_CURRENT_MARKET_DATA"]
    construction["constructed_paper_trade_count"] = 0
    construction["constructed_paper_trades"] = []
    construction["market_data_diagnostics"] = [{"candidate_id": "paper-candidate-1", "symbol": "QQQ", "missing_field": "market_data.value", "expected_source_artifact": "market_data_inputs_v1", "artifact_path_checked": str(tmp_path / f"reports/market_data_inputs_v1/{DAY}/market_data_inputs.v1.json"), "status": "ABSENT"}]
    _write_json(construction_path, construction)

    payload = build_runtime_truth_kernel_v1(truth_root=tmp_path, day_utc=DAY, generated_at_utc=NOW)
    blockers = payload["dependency_graph"]["PAPER_TRADE_CREATION_ALLOWED"]["missing_or_blocking_artifacts"]

    assert payload["dependency_graph"]["PAPER_TRADE_CREATION_ALLOWED"]["allowed"] is False
    assert any(item.startswith("paper_trade_construction:MISSING_MARKET_DATA:QQQ:market_data.value:market_data_inputs_v1:ABSENT:") for item in blockers)


def test_paper_creation_with_open_authorization_and_complete_market_data_is_allowed(tmp_path: Path) -> None:
    _write_all_kernel_artifacts(tmp_path, omit={"manual_trade_packet", "promoted_candidate_evidence", "broker_lifecycle_proof"})
    _write_paper_candidate_evidence(tmp_path, count=17)
    _write_paper_creation_evidence(tmp_path, authorized=True)

    payload = build_runtime_truth_kernel_v1(truth_root=tmp_path, day_utc=DAY, generated_at_utc=NOW)

    assert payload["dependency_graph"]["PAPER_TRADE_CREATION_ALLOWED"]["allowed"] is True
    assert payload["dependency_graph"]["TRADE_ADVICE_ALLOWED"]["allowed"] is False
    assert payload["dependency_graph"]["BROKER_SUBMIT_TRANSMIT"]["allowed"] is False
    assert payload["dependency_graph"]["AUTONOMOUS_EXECUTION_ALLOWED"]["allowed"] is False


def test_paper_candidates_ready_with_advisory_blocked_and_live_disabled(tmp_path: Path) -> None:
    _write_all_kernel_artifacts(tmp_path, omit={"manual_trade_packet", "promoted_candidate_evidence", "broker_lifecycle_proof"})
    _write_paper_candidate_evidence(tmp_path, count=17)
    _write_paper_creation_evidence(tmp_path, authorized=True)

    payload = build_runtime_truth_kernel_v1(truth_root=tmp_path, day_utc=DAY, generated_at_utc=NOW)

    assert payload["dependency_graph"]["PAPER_CANDIDATES_READY"]["allowed"] is True
    assert payload["dependency_graph"]["PAPER_CANDIDATES_READY"]["candidate_count"] == 17
    assert payload["dependency_graph"]["PAPER_TRADE_READY"]["allowed"] is True
    assert payload["dependency_graph"]["PAPER_TRADE_CREATION_ALLOWED"]["allowed"] is True
    assert payload["dependency_graph"]["TRADE_ADVICE_ALLOWED"]["allowed"] is False
    assert payload["dependency_graph"]["LIVE_TRADE_READY"]["allowed"] is False
    assert payload["dependency_graph"]["AUTONOMOUS_EXECUTION_ALLOWED"]["allowed"] is False


def test_paper_candidates_without_queue_fail_with_precise_blocker(tmp_path: Path) -> None:
    _write_all_kernel_artifacts(tmp_path, omit={"manual_trade_packet", "promoted_candidate_evidence", "broker_lifecycle_proof"})
    _write_paper_candidate_evidence(tmp_path, count=17)
    _remove_artifact(tmp_path, "paper_review_queue")

    payload = build_runtime_truth_kernel_v1(truth_root=tmp_path, day_utc=DAY, generated_at_utc=NOW)

    assert payload["dependency_graph"]["PAPER_CANDIDATES_READY"]["allowed"] is False
    assert "paper_review_queue" in payload["dependency_graph"]["PAPER_CANDIDATES_READY"]["missing_or_blocking_artifacts"]
    assert payload["dependency_graph"]["PAPER_TRADE_READY"]["allowed"] is False


def test_advisory_remains_blocked_while_paper_creation_is_allowed(tmp_path: Path) -> None:
    _write_all_kernel_artifacts(tmp_path, omit={"manual_trade_packet", "promoted_candidate_evidence"})
    _write_paper_candidate_evidence(tmp_path, count=17)
    _write_paper_creation_evidence(tmp_path, authorized=True)

    payload = build_runtime_truth_kernel_v1(truth_root=tmp_path, day_utc=DAY, generated_at_utc=NOW)

    assert payload["dependency_graph"]["PAPER_TRADE_CREATION_ALLOWED"]["allowed"] is True
    assert payload["dependency_graph"]["TRADE_ADVICE_ALLOWED"]["allowed"] is False
    assert "TRADE_ADVICE_ALLOWED" not in payload["dependency_graph"]["PAPER_TRADE_CREATION_ALLOWED"]["depends_on_capabilities"]


def test_live_broker_and_autonomous_execution_stay_disabled_in_paper_mode(tmp_path: Path) -> None:
    _write_all_kernel_artifacts(tmp_path, omit={"manual_trade_packet", "promoted_candidate_evidence"})
    _write_paper_candidate_evidence(tmp_path, count=17)
    _write_paper_creation_evidence(tmp_path, authorized=True)

    payload = build_runtime_truth_kernel_v1(truth_root=tmp_path, day_utc=DAY, generated_at_utc=NOW)

    assert payload["dependency_graph"]["LIVE_TRADE_READY"]["allowed"] is False
    assert payload["dependency_graph"]["LIVE_TRADE_READY"]["policy_status"] == "DISABLED_BY_POLICY"
    assert payload["dependency_graph"]["BROKER_SUBMIT_TRANSMIT"]["allowed"] is False
    assert payload["dependency_graph"]["BROKER_SUBMIT_TRANSMIT"]["policy_status"] == "DISABLED_BY_POLICY"
    assert payload["dependency_graph"]["AUTONOMOUS_EXECUTION_ALLOWED"]["allowed"] is False
    assert payload["dependency_graph"]["AUTONOMOUS_EXECUTION_ALLOWED"]["policy_status"] == "DISABLED_BY_POLICY"


def test_no_broker_submit_or_transmit_capability_is_introduced(tmp_path: Path) -> None:
    _write_all_kernel_artifacts(tmp_path)
    payload = build_runtime_truth_kernel_v1(truth_root=tmp_path, day_utc=DAY, generated_at_utc=NOW)
    paths = write_runtime_truth_kernel_reports_v1(truth_root=tmp_path, payload=payload)

    assert "BROKER_SUBMIT" not in payload["dependency_graph"]
    assert payload["dependency_graph"]["BROKER_SUBMIT_TRANSMIT"]["allowed"] is False
    assert payload["dependency_graph"]["BROKER_SUBMIT_TRANSMIT"]["policy_status"] == "DISABLED_BY_POLICY"
    assert payload["dependency_graph"]["AUTONOMOUS_EXECUTION_ALLOWED"]["allowed"] is False
    assert payload["dependency_graph"]["AUTONOMOUS_EXECUTION_ALLOWED"]["policy_status"] == "DISABLED_BY_POLICY"
    assert payload["safety_assertions"]["broker_submit_required"] is False
    assert payload["safety_assertions"]["autonomous_execution_allowed"] is False
    assert Path(paths["runtime_truth_kernel"]).exists()


def test_kernel_cli_writes_reports(tmp_path: Path, capsys) -> None:
    assert kernel_cli.main(["--truth_root", str(tmp_path), "--day", DAY, "--generated_at_utc", NOW, "--json"]) == 0
    out = json.loads(capsys.readouterr().out)

    assert Path(out["report_paths"]["runtime_truth_kernel"]).exists()
    assert Path(out["report_paths"]["missing_stale_sources"]).exists()
    assert Path(out["report_paths"]["recovery_plan"]).exists()
    assert Path(out["report_paths"]["readiness_dependencies"]).exists()
    assert Path(out["report_paths"]["runtime_state_snapshot"]).exists()
    assert Path(out["report_paths"]["runtime_state_transitions"]).exists()
    assert Path(out["report_paths"]["runtime_invalidations"]).exists()
    trace_path = decision_trace_json_path_v1(truth_root=tmp_path, day_utc=DAY)
    trace = json.loads(trace_path.read_text(encoding="utf-8"))
    evaluation = json.loads(Path(out["report_paths"]["runtime_evaluation"]).read_text(encoding="utf-8"))
    assert trace["evaluation_hash"] == evaluation["deterministic_output_hash"]
    assert trace["decision_trace"] == evaluation["decision_trace"]


def test_snapshot_is_persisted_for_kernel_evaluation(tmp_path: Path) -> None:
    _write_all_kernel_artifacts(tmp_path)
    payload = build_runtime_truth_kernel_v1(truth_root=tmp_path, day_utc=DAY, generated_at_utc=NOW)
    paths = write_runtime_truth_kernel_reports_v1(truth_root=tmp_path, payload=payload)

    snapshot = json.loads(Path(paths["runtime_state_snapshot"]).read_text(encoding="utf-8"))

    assert snapshot["evaluation_id"]
    assert snapshot["runtime_truth_classification"] == payload["runtime_truth_classification"]
    assert snapshot["highest_readiness_layer"] == payload["highest_readiness_layer"]
    assert isinstance(snapshot["allowed_capabilities"], list)
    assert snapshot["input_evidence_paths"]
    assert snapshot["evidence_hashes"]


def test_missing_stale_and_corrupt_artifacts_create_invalidation_events(tmp_path: Path) -> None:
    cases = [
        ("missing", "research_dataset_binding", lambda root: _remove_artifact(root, "research_dataset_binding"), "MISSING"),
        ("stale", "research_dataset_binding", lambda root: _make_artifact_stale(root, "research_dataset_binding"), "STALE"),
        ("corrupt", "event_market_snapshot", lambda root: _corrupt_artifact(root, "event_market_snapshot"), "INVALID"),
    ]
    for name, artifact_id, mutate, expected_type in cases:
        root = tmp_path / name
        _write_all_kernel_artifacts(root)
        mutate(root)
        payload = build_runtime_truth_kernel_v1(truth_root=root, day_utc=DAY, generated_at_utc=NOW)
        paths = write_runtime_truth_kernel_reports_v1(truth_root=root, payload=payload)

        invalidations = json.loads(Path(paths["runtime_invalidations"]).read_text(encoding="utf-8"))
        event = next(row for row in invalidations["events"] if row["artifact_id"] == artifact_id)

        assert event["invalidation_type"] == expected_type
        assert event["blocked_capabilities"]
        assert event["claim_implications"]
        assert event["recovery_command"]
        assert event["validation_command"]


def test_transition_log_detects_capability_downgrade_and_recovery(tmp_path: Path) -> None:
    _write_all_kernel_artifacts(tmp_path)
    first = build_runtime_truth_kernel_v1(truth_root=tmp_path, day_utc=DAY, generated_at_utc=NOW)
    write_runtime_truth_kernel_reports_v1(truth_root=tmp_path, payload=first)

    _remove_artifact(tmp_path, "research_dataset_binding")
    blocked = build_runtime_truth_kernel_v1(truth_root=tmp_path, day_utc=DAY, generated_at_utc="2026-05-15T21:00:00Z")
    blocked_paths = write_runtime_truth_kernel_reports_v1(truth_root=tmp_path, payload=blocked)
    blocked_transitions = json.loads(Path(blocked_paths["runtime_state_transitions"]).read_text(encoding="utf-8"))

    assert isinstance(blocked_transitions["summary"]["capabilities_lost"], list)

    _write_all_kernel_artifacts(tmp_path)
    recovered = build_runtime_truth_kernel_v1(truth_root=tmp_path, day_utc=DAY, generated_at_utc="2026-05-15T21:05:00Z")
    recovered_paths = write_runtime_truth_kernel_reports_v1(truth_root=tmp_path, payload=recovered)
    recovered_transitions = json.loads(Path(recovered_paths["runtime_state_transitions"]).read_text(encoding="utf-8"))

    assert isinstance(recovered_transitions["summary"]["capabilities_gained"], list)


def test_replay_reconstructs_prior_state_from_snapshot(tmp_path: Path, capsys) -> None:
    _write_all_kernel_artifacts(tmp_path)
    payload = build_runtime_truth_kernel_v1(truth_root=tmp_path, day_utc=DAY, generated_at_utc=NOW)
    write_runtime_truth_kernel_reports_v1(truth_root=tmp_path, payload=payload)

    assert replay_cli.main(["--truth_root", str(tmp_path), "--day", DAY, "--json"]) == 0
    replay = json.loads(capsys.readouterr().out)

    assert replay["what_aegis_believed"]["runtime_truth_classification"] == payload["runtime_truth_classification"]
    assert isinstance(replay["authorized"]["allowed_capabilities"], list)
    assert set(replay["blocked"]["blocked_capabilities"]).issubset(set(payload["blocked_capabilities"]))
    assert replay["claims_forbidden"] == payload["do_not_claim"]
    assert replay["safety"]["broker_submit_required"] is False


def test_diff_identifies_gained_and_lost_capabilities(tmp_path: Path, capsys) -> None:
    day_one = "2026-05-14"
    day_two = DAY
    _write_all_kernel_artifacts(tmp_path, day_utc=day_one)
    _remove_artifact(tmp_path, "research_dataset_binding", day_utc=day_one)
    first = build_runtime_truth_kernel_v1(truth_root=tmp_path, day_utc=day_one, generated_at_utc="2026-05-14T20:55:00Z")
    write_runtime_truth_kernel_reports_v1(truth_root=tmp_path, payload=first)

    _write_all_kernel_artifacts(tmp_path, day_utc=day_two)
    second = build_runtime_truth_kernel_v1(truth_root=tmp_path, day_utc=day_two, generated_at_utc=NOW)
    write_runtime_truth_kernel_reports_v1(truth_root=tmp_path, payload=second)

    assert diff_cli.main(["--truth_root", str(tmp_path), "--from-day", day_one, "--to-day", day_two, "--json"]) == 0
    diff = json.loads(capsys.readouterr().out)

    assert isinstance(diff["capabilities_gained"], list)
    assert "research_dataset_binding" in diff["artifacts_fixed"]
    assert diff["safety"]["autonomous_execution_allowed"] is False


def test_runtime_state_snapshot_uses_runtime_evaluation_capability_authority() -> None:
    snapshot = build_runtime_state_snapshot_v1({
        "day_utc": DAY,
        "generated_at_utc": NOW,
        "target_operating_mode": "HUMAN_APPROVED_ADVISORY_RUNTIME",
        "runtime_truth_classification": "REAL_RUNTIME",
        "highest_readiness_layer": "MANUAL_TRADE_CAPTURE_ALLOWED",
        "layers": {},
        "disabled_or_optional_layers": {},
        "artifact_statuses": [],
        "dependency_graph": {
            "MANUAL_TRADE_CAPTURE_ALLOWED": {"allowed": False},
            "TRADE_ADVICE_ALLOWED": {"allowed": False},
        },
        "runtime_evaluation": {
            "capabilities": {
                "MANUAL_TRADE_CAPTURE_ALLOWED": {"allowed": True},
                "TRADE_ADVICE_ALLOWED": {"allowed": False},
            }
        },
        "do_not_claim": [],
        "recovery_plan": [],
    })

    assert "MANUAL_TRADE_CAPTURE_ALLOWED" in snapshot["allowed_capabilities"]
    assert "MANUAL_TRADE_CAPTURE_ALLOWED" not in snapshot["blocked_capabilities"]
    assert "TRADE_ADVICE_ALLOWED" in snapshot["blocked_capabilities"]
    assert snapshot["dependency_statuses"]["MANUAL_TRADE_CAPTURE_ALLOWED"] is True
    assert snapshot["layers"]["MANUAL_TRADE_CAPTURE_ALLOWED"] is True



def test_kernel_authority_summary_reports_low_drift_after_refactor(tmp_path: Path) -> None:
    payload = build_runtime_truth_kernel_v1(truth_root=tmp_path, day_utc=DAY, generated_at_utc=NOW)

    authority = payload["kernel_authority"]
    assert authority["sole_authority_for"] == [
        "readiness",
        "capability_governance",
        "trade_advice_permission",
        "manual_capture_permission",
        "claim_governance",
        "recovery_planning",
    ]
    assert authority["legacy_readiness_logic_detected"] is False
    assert authority["truth_drift_risk"] == "LOW"


def test_control_packet_uses_kernel_derived_permissions(tmp_path: Path) -> None:
    _write_all_kernel_artifacts(tmp_path, omit={"research_dataset_binding"})

    kernel = build_runtime_truth_kernel_v1(truth_root=tmp_path, day_utc=DAY, generated_at_utc=NOW)
    packet = build_aegis_chatgpt_control_packet_v1(truth_root=tmp_path, day_utc=DAY, generated_at_utc=NOW)

    assert kernel["dependency_graph"]["TRADE_ADVICE_ALLOWED"]["allowed"] is False
    assert packet["runtime_truth_classification"] == kernel["runtime_truth_classification"]
    assert packet["trade_advice_allowed"] is False
    assert packet["manual_trade_capture_allowed"] is False
    assert packet["readiness_state"]["kernel_authority"] == "aegis_runtime_truth_kernel_v1"


def test_audit_handoff_uses_kernel_derived_claims_and_readiness(tmp_path: Path) -> None:
    _write_all_kernel_artifacts(tmp_path, omit={"alert_transport_proof"})
    packet = build_aegis_chatgpt_control_packet_v1(truth_root=tmp_path, day_utc=DAY, generated_at_utc=NOW)

    text = build_handoff_text_v1(
        packet={**packet, "do_not_claim": ["LEGACY_PACKET_CLAIM"], "runtime_truth_classification": "REAL_RUNTIME"},
        packet_path=tmp_path / "packet.json",
        truth_root=tmp_path,
        day_utc=DAY,
    )

    assert "kernel_authority" in text
    assert "Live email/SMS transport is not proven" in text
    assert "live_broker_trading_policy: DISABLED_BY_DESIGN" in text
    assert "broker_submit_transmit_policy: DISABLED_BY_DESIGN" in text
    assert "LEGACY_PACKET_CLAIM" not in text


def test_ui_api_route_is_kernel_backed_and_read_only() -> None:
    server_source = (REPO_ROOT / "constellation_2/phaseL/ui/server/run_ops_dashboard_v1.py").read_text(encoding="utf-8")
    pages_source = (REPO_ROOT / "constellation_2/phaseL/ui/static/operator_shell/pages/index.js").read_text(encoding="utf-8")
    route_source = (REPO_ROOT / "constellation_2/phaseL/ui/static/operator_shell/pages/route_metadata.js").read_text(encoding="utf-8")

    assert 'path == "/api/aegis/runtime-truth"' in server_source
    assert "build_runtime_truth_kernel_v1(truth_root=GLOBAL_TRUTH_ROOT" in server_source
    assert 'path: "/aegis-runtime-truth"' in route_source
    assert "fetchAegisRuntimeTruth()" in pages_source
    assert "HUMAN_APPROVED_ADVISORY_RUNTIME" in pages_source
    assert "DISABLED_BY_DESIGN" in pages_source
    assert "JOURNALING_AUDIT_ONLY" in pages_source
    assert "broker_submit_required" not in pages_source[pages_source.index("async function renderAegisRuntimeTruthPage") : pages_source.index("async function renderAegisLiteQueuePage")]
    assert "postJson" not in pages_source[pages_source.index("async function renderAegisRuntimeTruthPage") : pages_source.index("async function renderAegisLiteQueuePage")]


def test_legacy_packet_cannot_allow_trade_or_manual_capture_when_kernel_blocks(tmp_path: Path) -> None:
    _write_all_kernel_artifacts(tmp_path, omit={"research_dataset_binding", "broker_lifecycle_proof"})
    kernel = build_runtime_truth_kernel_v1(truth_root=tmp_path, day_utc=DAY, generated_at_utc=NOW)
    write_runtime_truth_kernel_reports_v1(truth_root=tmp_path, payload=kernel)

    packet = build_aegis_chatgpt_control_packet_v1(truth_root=tmp_path, day_utc=DAY, generated_at_utc=NOW)

    assert packet["trade_advice_allowed"] is False
    assert packet["manual_trade_capture_allowed"] is False
    assert "KERNEL_BLOCKED_TRADE_ADVICE" in packet["reason_if_blocked"]


def test_runtime_invalidation_cases_emit_capability_claim_and_recovery(tmp_path: Path) -> None:
    cases = [
        ("research_dataset_binding", "missing", {"omit": {"research_dataset_binding"}}, "RESEARCH_READY", "Research dataset binding must be current."),
        ("research_dataset_binding", "stale", {"stale": "research_dataset_binding"}, "RESEARCH_READY", "Research dataset binding must be current."),
        ("alert_transport_proof", "missing", {"omit": {"alert_transport_proof"}}, "ALERT_TRANSPORT_PROVEN", "Live email/SMS transport is not proven"),
        ("ai_feedback_review", "missing", {"omit": {"ai_feedback_review"}}, "FEEDBACK_READY", "AI feedback is deterministic fallback only"),
        ("event_market_snapshot", "corrupt", {"corrupt": "event_market_snapshot"}, "DATA_READY", "Trade advice remains forbidden"),
    ]
    for artifact_id, issue, mutation, capability, claim in cases:
        root = tmp_path / f"{artifact_id}_{issue}"
        _write_all_kernel_artifacts(root)
        if mutation.get("omit"):
            for omitted in mutation["omit"]:
                _remove_artifact(root, omitted)
        if mutation.get("stale"):
            _make_artifact_stale(root, str(mutation["stale"]))
        if mutation.get("corrupt"):
            _corrupt_artifact(root, str(mutation["corrupt"]))

        payload = build_runtime_truth_kernel_v1(truth_root=root, day_utc=DAY, generated_at_utc=NOW)

        assert payload["dependency_graph"][capability]["allowed"] is False
        assert claim in "\n".join(payload["do_not_claim"])
        recovery = next(row for row in payload["recovery_plan"] if row["artifact_id"] == artifact_id)
        assert recovery["generated_by_command"]
        assert recovery["why_it_matters"]
        assert recovery["downstream_capabilities_expected_to_recover"]


def test_broker_lifecycle_missing_is_optional_for_target_mode(tmp_path: Path) -> None:
    _write_all_kernel_artifacts(tmp_path, omit={"broker_lifecycle_proof"})

    payload = build_runtime_truth_kernel_v1(truth_root=tmp_path, day_utc=DAY, generated_at_utc=NOW)

    assert payload["runtime_truth_classification"] == "REAL_RUNTIME"
    assert payload["dependency_graph"]["BROKER_PAPER_LIFECYCLE_PROVEN"]["readiness_relevant"] is False
    assert "broker_lifecycle_proof" not in [row["artifact_id"] for row in payload["missing_or_stale_sources"]]
    assert "BROKER_PAPER_LIFECYCLE_PROVEN" in payload["optional_not_required_capabilities"]


def test_placeholder_broker_and_transport_proof_do_not_clear_safety_gates(tmp_path: Path) -> None:
    _write_all_kernel_artifacts(tmp_path)
    _write_json(
        _artifact_path(tmp_path, "broker_lifecycle_proof"),
        {"schema_id": "broker_event_day_manifest", "day_utc": DAY, "status": "FAIL", "produced_utc": NOW},
    )
    _write_json(
        _artifact_path(tmp_path, "alert_transport_proof"),
        _artifact("trade_capture_alert_ledger", "trade_capture_alert_ledger_v1", generated_at=NOW, alert_attempts=[{"delivery_status": "GATE_ONLY_NO_TRANSPORT"}]),
    )

    payload = build_runtime_truth_kernel_v1(truth_root=tmp_path, day_utc=DAY, generated_at_utc=NOW)

    assert payload["dependency_graph"]["BROKER_LIFECYCLE_PROVEN"]["allowed"] is False
    assert payload["dependency_graph"]["ALERT_TRANSPORT_PROVEN"]["allowed"] is False
    assert payload["dependency_graph"]["TRADE_ADVICE_ALLOWED"]["allowed"] is False
    assert payload["safety_assertions"]["broker_submit_required"] is False
    assert payload["safety_assertions"]["autonomous_execution_allowed"] is False


def test_event_validity_no_event_packet_clears_missing_but_not_actionability(tmp_path: Path) -> None:
    _write_all_kernel_artifacts(tmp_path)
    _write_json(
        _artifact_path(tmp_path, "event_validity_gate"),
        _artifact(
            "event_validity_gate",
            "event_validity_gate_v1",
            generated_at=NOW,
            evaluated=True,
            event_packet_present=False,
            event_packet_path=None,
            source_event_snapshot_path="",
            validity_status="NO_EVENT_PACKET",
            reason="No event packet exists.",
            evidence_hash="hash",
            generated_by_command="test",
            validation_command="test",
        ),
    )

    payload = build_runtime_truth_kernel_v1(truth_root=tmp_path, day_utc=DAY, generated_at_utc=NOW)

    assert next(row for row in payload["artifact_statuses"] if row["artifact_id"] == "event_validity_gate")["status"] == "OK"
    assert payload["dependency_graph"]["EVENT_READY"]["allowed"] is True
    assert payload["dependency_graph"]["TRADE_ADVICE_ALLOWED"]["allowed"] is False
    assert "event_validity_gate:NO_EVENT_PACKET" in payload["dependency_graph"]["TRADE_ADVICE_ALLOWED"]["missing_or_blocking_artifacts"]
    assert payload["advisory_status"] == "NO_ACTIONABLE_CANDIDATES"
    assert payload["human_approved_advisory_runtime_ready"] is True
    assert payload["layers"]["HUMAN_APPROVED_ADVISORY_RUNTIME_READY"] is True


def test_event_validity_valid_requires_real_packet_reference(tmp_path: Path) -> None:
    _write_all_kernel_artifacts(tmp_path)
    _write_json(
        _artifact_path(tmp_path, "event_validity_gate"),
        _artifact(
            "event_validity_gate",
            "event_validity_gate_v1",
            generated_at=NOW,
            evaluated=True,
            event_packet_present=False,
            event_packet_path=None,
            source_event_snapshot_path="",
            validity_status="VALID",
            reason="Invalid fixture.",
            evidence_hash="hash",
            generated_by_command="test",
            validation_command="test",
        ),
    )

    payload = build_runtime_truth_kernel_v1(truth_root=tmp_path, day_utc=DAY, generated_at_utc=NOW)

    assert next(row for row in payload["artifact_statuses"] if row["artifact_id"] == "event_validity_gate")["status"] == "INVALID"
    assert payload["dependency_graph"]["EVENT_READY"]["allowed"] is False


def test_alert_gate_and_dry_run_do_not_satisfy_live_transport(tmp_path: Path) -> None:
    for mode, result, confirmed, dry_run, expected_gate, expected_dry in [
        ("GATE_ONLY", "GATE_ONLY_NO_TRANSPORT", False, False, True, False),
        ("DRY_RUN", "DRY_RUN_CONFIRMED", True, True, True, True),
    ]:
        root = tmp_path / mode
        _write_all_kernel_artifacts(root)
        _write_json(
            _artifact_path(root, "alert_transport_proof"),
            _artifact(
                "alert_transport_proof",
                "alert_transport_proof_v1",
                generated_at=NOW,
                transport_mode=mode,
                evaluated=True,
                delivery_attempted=mode == "DRY_RUN",
                delivery_confirmed=confirmed,
                channel="test" if mode == "DRY_RUN" else None,
                dry_run=dry_run,
                provider_message_id=None,
                destination_redacted=None,
                result=result,
                evidence_hash="hash",
                generated_by_command="test",
                validation_command="test",
            ),
        )

        payload = build_runtime_truth_kernel_v1(truth_root=root, day_utc=DAY, generated_at_utc=NOW)

        assert payload["dependency_graph"]["ALERT_GATE_PROVEN"]["allowed"] is expected_gate
        assert payload["dependency_graph"]["ALERT_DRY_RUN_PROVEN"]["allowed"] is expected_dry
        assert payload["dependency_graph"]["ALERT_TRANSPORT_PROVEN"]["allowed"] is False
        assert payload["dependency_graph"]["TRADE_ADVICE_ALLOWED"]["allowed"] is False
        assert "manual_trade_packet:NO_ACTIONABLE_CANDIDATES" in payload["dependency_graph"]["TRADE_ADVICE_ALLOWED"]["missing_or_blocking_artifacts"]
        assert payload["layers"]["HUMAN_APPROVED_ADVISORY_RUNTIME_READY"] is True
        assert payload["advisory_status"] == "NO_ACTIONABLE_CANDIDATES"


def test_live_alert_transport_requires_confirmed_live_delivery(tmp_path: Path) -> None:
    _write_all_kernel_artifacts(tmp_path)
    payload = build_runtime_truth_kernel_v1(truth_root=tmp_path, day_utc=DAY, generated_at_utc=NOW)

    assert payload["dependency_graph"]["ALERT_TRANSPORT_PROVEN"]["allowed"] is True

    _write_json(
        _artifact_path(tmp_path, "alert_transport_proof"),
        _artifact(
            "alert_transport_proof",
            "alert_transport_proof_v1",
            generated_at=NOW,
            transport_mode="LIVE",
            evaluated=True,
            delivery_attempted=True,
            delivery_confirmed=False,
            channel="email",
            dry_run=False,
            provider_message_id=None,
            destination_redacted="operator@example.invalid",
            result="LIVE_CONFIRMED",
            evidence_hash="hash",
            generated_by_command="test",
            validation_command="test",
        ),
    )
    blocked = build_runtime_truth_kernel_v1(truth_root=tmp_path, day_utc=DAY, generated_at_utc=NOW)

    assert next(row for row in blocked["artifact_statuses"] if row["artifact_id"] == "alert_transport_proof")["status"] == "INVALID"
    assert blocked["dependency_graph"]["ALERT_TRANSPORT_PROVEN"]["allowed"] is False


def test_broker_simulation_does_not_enable_paper_or_live_readiness(tmp_path: Path) -> None:
    _write_all_kernel_artifacts(tmp_path)
    _write_json(
        _artifact_path(tmp_path, "broker_lifecycle_proof"),
        _artifact(
            "broker_lifecycle_proof",
            "broker_lifecycle_proof_v1",
            generated_at=NOW,
            lifecycle_mode="SIMULATED",
            evaluated=True,
            broker_connected=False,
            order_created=True,
            order_submitted=False,
            order_acknowledged=False,
            order_filled=False,
            order_cancelled=False,
            account_type=None,
            broker=None,
            evidence_paths=[],
            external_ids_redacted=[],
            result="SIMULATED_CONFIRMED",
            evidence_hash="hash",
        ),
    )

    payload = build_runtime_truth_kernel_v1(truth_root=tmp_path, day_utc=DAY, generated_at_utc=NOW)

    assert payload["dependency_graph"]["BROKER_SIMULATION_PROVEN"]["allowed"] is True
    assert payload["dependency_graph"]["BROKER_PAPER_LIFECYCLE_PROVEN"]["allowed"] is False
    assert payload["dependency_graph"]["BROKER_LIVE_LIFECYCLE_PROVEN"]["allowed"] is False
    assert payload["dependency_graph"]["PAPER_TRADE_READY"]["allowed"] is False


def test_manual_receipt_none_declared_clears_missing_without_fill_proof(tmp_path: Path) -> None:
    _write_all_kernel_artifacts(tmp_path)
    _write_json(
        _artifact_path(tmp_path, "manual_execution_receipt"),
        _artifact(
            "manual_execution_receipt",
            "manual_execution_receipt_v1",
            generated_at=NOW,
            receipt_type="NONE_DECLARED",
            operator_declared_no_manual_execution=True,
            manual_fill_present=False,
            fill_details_present=False,
            source="operator_declaration",
            trade_ids=[],
            evidence_paths=[],
            evidence_hash="hash",
            result="NO_MANUAL_EXECUTION_DECLARED",
        ),
    )

    payload = build_runtime_truth_kernel_v1(truth_root=tmp_path, day_utc=DAY, generated_at_utc=NOW)

    receipt = next(row for row in payload["artifact_statuses"] if row["artifact_id"] == "manual_execution_receipt")
    assert receipt["status"] == "OK"
    assert payload["dependency_graph"]["MANUAL_TRADE_CAPTURE_ALLOWED"]["allowed"] is True


def test_manual_fill_recorded_rejects_placeholder_fill(tmp_path: Path) -> None:
    _write_all_kernel_artifacts(tmp_path)
    _write_json(
        _artifact_path(tmp_path, "manual_execution_receipt"),
        _artifact(
            "manual_execution_receipt",
            "manual_execution_receipt_v1",
            generated_at=NOW,
            receipt_type="MANUAL_FILL_RECORDED",
            operator_declared_no_manual_execution=False,
            manual_fill_present=True,
            fill_details_present=False,
            source="manual_entry",
            trade_ids=[],
            evidence_paths=[],
            evidence_hash="hash",
            result="MANUAL_FILL_RECEIPT_VALID",
        ),
    )

    payload = build_runtime_truth_kernel_v1(truth_root=tmp_path, day_utc=DAY, generated_at_utc=NOW)

    assert next(row for row in payload["artifact_statuses"] if row["artifact_id"] == "manual_execution_receipt")["status"] == "INVALID"
    assert payload["dependency_graph"]["MANUAL_TRADE_CAPTURE_ALLOWED"]["allowed"] is False


def test_target_operating_mode_disables_live_and_autonomous_without_lowering_readiness(tmp_path: Path) -> None:
    _write_all_kernel_artifacts(tmp_path, omit={"broker_lifecycle_proof"})
    _write_json(
        _artifact_path(tmp_path, "alert_transport_proof"),
        _artifact(
            "alert_transport_proof",
            "alert_transport_proof_v1",
            generated_at=NOW,
            transport_mode="GATE_ONLY",
            evaluated=True,
            delivery_attempted=False,
            delivery_confirmed=False,
            channel=None,
            dry_run=False,
            result="GATE_ONLY_NO_TRANSPORT",
            evidence_hash="hash",
            generated_by_command="test",
            validation_command="test",
        ),
    )

    payload = build_runtime_truth_kernel_v1(truth_root=tmp_path, day_utc=DAY, generated_at_utc=NOW)

    assert payload["target_operating_mode"] == "HUMAN_APPROVED_ADVISORY_RUNTIME"
    assert payload["live_broker_trading_policy"] == "DISABLED_BY_DESIGN"
    assert payload["autonomous_execution_policy"] == "DISABLED_BY_DESIGN"
    assert payload["dependency_graph"]["LIVE_TRADE_READY"]["allowed"] is False
    assert payload["dependency_graph"]["LIVE_TRADE_READY"]["policy_status"] == "DISABLED_BY_POLICY"
    assert payload["dependency_graph"]["AUTONOMOUS_EXECUTION_ALLOWED"]["allowed"] is False
    assert payload["dependency_graph"]["BROKER_SUBMIT_TRANSMIT"]["allowed"] is False
    assert "LIVE_TRADE_READY" not in payload["blocked_capabilities"]
    assert "AUTONOMOUS_EXECUTION_ALLOWED" not in payload["blocked_capabilities"]
    assert payload["dependency_graph"]["TRADE_ADVICE_ALLOWED"]["allowed"] is False
    assert "manual_trade_packet:NO_ACTIONABLE_CANDIDATES" in payload["dependency_graph"]["TRADE_ADVICE_ALLOWED"]["missing_or_blocking_artifacts"]
    assert payload["human_approved_advisory_runtime_ready"] is True


def test_manual_receipt_cli_fails_closed_without_runtime_evaluation_and_rejects_unsafe_inputs(tmp_path: Path, capsys) -> None:
    assert (
        capture_cli.main(
            [
                "--truth_root",
                str(tmp_path),
                "--symbol",
                "SPY",
                "--side",
                "BUY",
                "--quantity",
                "10",
                "--price",
                "500.25",
                "--trade-date",
                DAY,
                "--strategy-or-sleeve",
                "core",
                "--operator-attestation",
                "true",
            ]
        )
        == 3
    )
    blocked = json.loads(capsys.readouterr().err)
    assert blocked["validation_status"] == "BLOCKED_BY_RUNTIME_EVALUATION"

    assert capture_cli.main(["--truth_root", str(tmp_path), "--symbol", "SPY", "--side", "BUY", "--quantity", "1", "--trade-date", DAY, "--operator-attestation", "true"]) == 2
    capsys.readouterr()
    assert capture_cli.main(["--truth_root", str(tmp_path), "--symbol", "SPY", "--side", "BUY", "--quantity", "-1", "--price", "500", "--trade-date", DAY, "--operator-attestation", "true"]) == 2
    capsys.readouterr()
    assert capture_cli.main(["--truth_root", str(tmp_path), "--symbol", "SPY", "--side", "BUY", "--quantity", "1", "--price", "500", "--trade-date", DAY, "--operator-attestation", "true", "--autonomous-execution", "true"]) == 2
    capsys.readouterr()
    assert capture_cli.main(["--truth_root", str(tmp_path), "--symbol", "SPY", "--side", "BUY", "--quantity", "1", "--price", "500", "--trade-date", DAY]) == 2
    capsys.readouterr()
    assert (
        capture_cli.main(
            [
                "--truth_root",
                str(tmp_path),
                "--symbol",
                "QQQ",
                "--side",
                "SELL",
                "--quantity",
                "2",
                "--price",
                "400",
                "--trade-date",
                DAY,
                "--operator-attestation",
                "true",
                "--dry-run",
            ]
        )
        == 0
    )
    dry = json.loads(capsys.readouterr().out)
    assert dry["dry_run"] is True
    assert dry["receipt"]["broker_submission_by_aegis"] is False
    assert dry["receipt"]["autonomous_execution"] is False


def test_daily_operator_report_is_generated_and_frames_policy_disabled(tmp_path: Path, capsys) -> None:
    _write_all_kernel_artifacts(tmp_path)
    _write_json(
        _artifact_path(tmp_path, "event_validity_gate"),
        _artifact(
            "event_validity_gate",
            "event_validity_gate_v1",
            generated_at=NOW,
            evaluated=True,
            event_packet_present=False,
            event_packet_path=None,
            source_event_snapshot_path="",
            validity_status="NO_EVENT_PACKET",
            reason="No event packet exists.",
            evidence_hash="hash",
            generated_by_command="test",
            validation_command="test",
        ),
    )

    assert daily_cli.main(["--truth_root", str(tmp_path), "--day", DAY, "--generated_at_utc", NOW]) == 0
    out = json.loads(capsys.readouterr().out)
    report = json.loads(Path(out["path"]).read_text(encoding="utf-8"))
    summary = Path(out["summary_path"]).read_text(encoding="utf-8")

    assert report["target_mode_ready"] is True
    assert report["advisory_status"] == "NO_ACTIONABLE_CANDIDATES"
    assert report["manual_capture_available"] is False
    assert report["disabled_by_policy"]["broker_submit_transmit"] == "DISABLED_BY_DESIGN"
    assert "Do not expect Aegis to submit/transmit broker orders" in summary


def test_ai_feedback_claim_remains_blocked_unless_ai_used_true(tmp_path: Path) -> None:
    _write_all_kernel_artifacts(tmp_path, ai_used=False)

    payload = build_runtime_truth_kernel_v1(truth_root=tmp_path, day_utc=DAY, generated_at_utc=NOW)

    assert payload["dependency_graph"]["FEEDBACK_READY"]["allowed"] is True
    assert "AI feedback is deterministic fallback only" in "\n".join(payload["do_not_claim"])


def test_evidence_completion_report_records_before_after_readiness(tmp_path: Path) -> None:
    _write_all_kernel_artifacts(tmp_path, omit={"research_dataset_binding"})
    before_payload = build_runtime_truth_kernel_v1(truth_root=tmp_path, day_utc=DAY, generated_at_utc=NOW)
    before_paths = write_runtime_truth_kernel_reports_v1(truth_root=tmp_path, payload=before_payload)
    before_snapshot = json.loads(Path(before_paths["runtime_state_snapshot"]).read_text(encoding="utf-8"))

    _write_all_kernel_artifacts(tmp_path)
    after_payload = build_runtime_truth_kernel_v1(truth_root=tmp_path, day_utc=DAY, generated_at_utc="2026-05-15T21:05:00Z")
    after_paths = write_runtime_truth_kernel_reports_v1(truth_root=tmp_path, payload=after_payload)
    after_snapshot = json.loads(Path(after_paths["runtime_state_snapshot"]).read_text(encoding="utf-8"))

    paths = write_evidence_completion_report_v1(
        truth_root=tmp_path,
        day_utc=DAY,
        before_snapshot=before_snapshot,
        after_snapshot=after_snapshot,
        commands_run=["python3 ops/tools/audit_research_dataset_bindings_v1 --truth_root TEST --day_utc 2026-05-15"],
        validations_run=["npm run aegis:truth-kernel"],
    )
    report = json.loads(Path(paths["evidence_completion"]).read_text(encoding="utf-8"))

    assert report["safety_gates_unchanged"] is True
    assert "research_dataset_binding" in report["artifacts_repaired"]
    assert isinstance(report["capabilities_gained"], list)
    assert report["readiness_before"]["runtime_truth_classification"] == "PARTIAL_CONTEXT"
    assert report["readiness_after"]["runtime_truth_classification"] == after_payload["runtime_truth_classification"]


def _write_all_kernel_artifacts(
    root: Path,
    *,
    day_utc: str = DAY,
    generated_at: str = NOW,
    ai_used: bool = True,
    omit: set[str] | None = None,
) -> None:
    omit = omit or set()
    artifacts = {
        "aegis_lite_operating_status": (f"reports/aegis_lite_operating_status_v1/{day_utc}/aegis_lite_operating_status.v1.json", _artifact("aegis_lite_operating_status", "aegis_lite_operating_status_v1", day_utc=day_utc, generated_at=generated_at, broker_mode="MANUAL_ONLY")),
        "aegis_lite_eod_report": (f"reports/aegis_lite_eod_report_v1/{day_utc}/aegis_lite_eod_report.v1.json", _artifact("aegis_lite_eod_report", "aegis_lite_eod_report_v1", day_utc=day_utc, generated_at=generated_at)),
        "operator_execution_queue": (f"reports/operator_execution_queue_v1/{day_utc}/operator_execution_queue.v1.json", _artifact("operator_execution_queue", "operator_execution_queue_v1", day_utc=day_utc, generated_at=generated_at, execution_queue=[])),
        "market_data_inputs": (f"reports/market_data_inputs_v1/{day_utc}/market_data_inputs.v1.json", _artifact("market_data_inputs", "market_data_inputs_v1", day_utc=day_utc, generated_at=generated_at, status="CURRENT", broker_execution_allowed=False, autonomous_execution_allowed=False)),
        "manual_trade_packet": (f"reports/manual_trade_packet_v1/{day_utc}/run/manual_trade_packet.v1.json", _artifact("manual_trade_packet", "manual_trade_packet_v1", day_utc=day_utc, generated_at=generated_at, date=day_utc)),
        "manual_execution_receipt": (
            f"reports/manual_execution_receipt_v1/{day_utc}/run/manual_execution_receipt.v1.json",
            _artifact(
                "manual_execution_receipt",
                "manual_execution_receipt_v1",
                day_utc=day_utc,
                generated_at=generated_at,
                receipt_type="MANUAL_FILL_RECORDED",
                operator_declared_no_manual_execution=False,
                manual_fill_present=True,
                fill_details_present=True,
                source="manual_entry",
                trade_ids=["trade-1"],
                evidence_paths=[],
                evidence_hash="hash",
                result="MANUAL_FILL_RECEIPT_VALID",
                broker_submission_by_aegis=False,
                autonomous_execution=False,
            ),
        ),
        "broker_lifecycle_proof": (
            f"reports/broker_lifecycle_proof_v1/{day_utc}/run/broker_lifecycle_proof.v1.json",
            _artifact(
                "broker_lifecycle_proof",
                "broker_lifecycle_proof_v1",
                day_utc=day_utc,
                generated_at=generated_at,
                lifecycle_mode="PAPER",
                evaluated=True,
                broker_connected=True,
                order_created=True,
                order_submitted=True,
                order_acknowledged=True,
                order_filled=False,
                order_cancelled=True,
                account_type="paper",
                broker="IBKR",
                evidence_paths=[],
                external_ids_redacted=[],
                result="PAPER_LIFECYCLE_CONFIRMED",
                evidence_hash="hash",
            ),
        ),
        "promoted_candidate_evidence": ("reports/promoted_sleeve_library_v1/current/promoted_sleeve_library.v1.json", _artifact("promoted_sleeve_library", "promoted_sleeve_library_v1", day_utc=day_utc, generated_at=generated_at)),
        "event_monitoring_status": (f"reports/event_monitoring_status_v1/{day_utc}/run/event_monitoring_status.v1.json", _artifact("event_monitoring_status", "event_monitoring_status_v1", day_utc=day_utc, generated_at=generated_at)),
        "event_rules_registry": (f"reports/event_rules_registry_v1/{day_utc}/event_rules_registry.v1.json", _artifact("event_rules_registry", "event_rules_registry_v1", day_utc=day_utc, generated_at=generated_at)),
        "event_market_snapshot": (f"reports/event_market_snapshot_v1/{day_utc}/event_market_snapshot.v1.json", _artifact("event_market_snapshot", "event_market_snapshot_v1", day_utc=day_utc, generated_at=generated_at)),
        "event_validity_gate": (
            f"reports/event_validity_gate_v1/{day_utc}/run/event_validity_gate.v1.json",
            _artifact(
                "event_validity_gate",
                "event_validity_gate_v1",
                day_utc=day_utc,
                generated_at=generated_at,
                evaluated=True,
                event_packet_present=True,
                event_packet_path="/tmp/event_packet.v1.json",
                source_event_snapshot_path="",
                validity_status="VALID",
                reason="Event packet exists.",
                evidence_hash="hash",
                generated_by_command="test",
                validation_command="test",
            ),
        ),
        "alert_transport_proof": (
            f"reports/alert_transport_proof_v1/{day_utc}/run/alert_transport_proof.v1.json",
            _artifact(
                "alert_transport_proof",
                "alert_transport_proof_v1",
                day_utc=day_utc,
                generated_at=generated_at,
                transport_mode="LIVE",
                evaluated=True,
                delivery_attempted=True,
                delivery_confirmed=True,
                channel="email",
                dry_run=False,
                provider_message_id="redacted",
                destination_redacted="operator@example.invalid",
                result="LIVE_CONFIRMED",
                evidence_hash="hash",
                generated_by_command="test",
                validation_command="test",
            ),
        ),
        "ai_feedback_review": (f"reports/ai_feedback_review_v1/EOD/{day_utc}/ai_feedback_review.v1.json", _artifact("ai_feedback_review", "ai_feedback_review_v1", day_utc=day_utc, generated_at=generated_at, ai_used=ai_used)),
        "research_dataset_binding": (f"reports/research_dataset_gap_v1/{day_utc}/research_dataset_gap.v1.json", _artifact("research_dataset_gap", "research_dataset_gap_v1", day_utc=day_utc, generated_at=generated_at)),
        "research_task_queue": (f"research_lab/research_task_queue_v1/{day_utc}/index/research_task_queue.v1.json", _artifact("research_task_queue", "research_task_queue_v1", day_utc=day_utc, generated_at=generated_at)),
    }
    for artifact_id, (relative, payload) in artifacts.items():
        if artifact_id not in omit:
            _write_json(root / relative, payload)


def _artifact_path(root: Path, artifact_id: str, *, day_utc: str = DAY) -> Path:
    paths = {
        "research_dataset_binding": root / f"reports/research_dataset_gap_v1/{day_utc}/research_dataset_gap.v1.json",
        "alert_transport_proof": root / f"reports/alert_transport_proof_v1/{day_utc}/run/alert_transport_proof.v1.json",
        "broker_lifecycle_proof": root / f"reports/broker_lifecycle_proof_v1/{day_utc}/run/broker_lifecycle_proof.v1.json",
        "ai_feedback_review": root / f"reports/ai_feedback_review_v1/EOD/{day_utc}/ai_feedback_review.v1.json",
        "event_market_snapshot": root / f"reports/event_market_snapshot_v1/{day_utc}/event_market_snapshot.v1.json",
        "event_validity_gate": root / f"reports/event_validity_gate_v1/{day_utc}/run/event_validity_gate.v1.json",
        "manual_execution_receipt": root / f"reports/manual_execution_receipt_v1/{day_utc}/run/manual_execution_receipt.v1.json",
        "candidate_review_packet": root / f"reports/aegis_candidate_review_packet_v1/{day_utc}/candidate_review_packet.v1.json",
        "paper_review_queue": root / f"reports/aegis_paper_review_queue_v1/{day_utc}/paper_review_queue.v1.json",
        "market_data_inputs": root / f"reports/market_data_inputs_v1/{day_utc}/market_data_inputs.v1.json",
        "paper_session_authority": root / f"reports/paper_session_authority_v1/{day_utc}/paper_session_authority.v1.json",
        "paper_session_ledger": root / f"reports/paper_session_ledger_v1/{day_utc}/paper_session_ledger.v1.json",
        "paper_trade_construction": root / f"reports/paper_trade_construction_v1/{day_utc}/paper_trade_construction.v1.json",
    }
    return paths[artifact_id]


def _remove_artifact(root: Path, artifact_id: str, *, day_utc: str = DAY) -> None:
    path = _artifact_path(root, artifact_id, day_utc=day_utc)
    if path.exists():
        path.unlink()


def _make_artifact_stale(root: Path, artifact_id: str, *, day_utc: str = DAY) -> None:
    path = _artifact_path(root, artifact_id, day_utc=day_utc)
    payload = json.loads(path.read_text(encoding="utf-8"))
    payload["generated_at_utc"] = OLD
    path.write_text(json.dumps(payload, sort_keys=True) + "\n", encoding="utf-8")


def _corrupt_artifact(root: Path, artifact_id: str, *, day_utc: str = DAY) -> None:
    path = _artifact_path(root, artifact_id, day_utc=day_utc)
    path.write_text("{not json\n", encoding="utf-8")


def _artifact(schema_id: str, artifact_id: str, *, day_utc: str = DAY, generated_at: str = NOW, **extra: object) -> dict[str, object]:
    return {
        "schema_id": schema_id,
        "artifact_id": artifact_id,
        "day_utc": day_utc,
        "generated_at_utc": generated_at,
        **extra,
    }


def _write_json(path: Path, payload: dict[str, object]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, sort_keys=True) + "\n", encoding="utf-8")
