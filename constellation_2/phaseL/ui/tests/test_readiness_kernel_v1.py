from __future__ import annotations

import json
from pathlib import Path
from constellation_2.phaseL.ui.tests.operator_shell_test_sources import pages_source_v1

from constellation_2.phaseL.ui_api.readiness_kernel_v1 import build_readiness_kernel_v1


DAY = "2026-04-29"
COMMIT = "a" * 40


def _write_json(root: Path, relative: str, payload: dict) -> Path:
    path = root / relative
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, sort_keys=True), encoding="utf-8")
    return path


def _write_packet(root: Path, mode: str = "PRODUCTION", commit: str = COMMIT) -> None:
    path = root / "exports/aegis_state/latest/chatgpt_aegis_packet.md"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        "\n".join(
            [
                "# Aegis Packet",
                "- generated_at_utc: 2026-04-29T13:00:00Z",
                f"- runtime_mode: {mode}",
                f"- git_commit: {commit}",
                "- git_dirty_status: CLEAN",
            ]
        ),
        encoding="utf-8",
    )


def _write_runtime_evaluation(root: Path, *, day: str = DAY, hash_value: str = "b" * 64, trade_allowed: bool = False, manual_allowed: bool = False) -> Path:
    return _write_json(
        root,
        f"reports/aegis_runtime_truth_kernel_v1/{day}/runtime_evaluation.v1.json",
        {
            "schema_id": "aegis_runtime_evaluation",
            "schema_version": "v1",
            "day_utc": day,
            "generated_at_utc": "2026-04-29T13:00:00Z",
            "deterministic_output_hash": hash_value,
            "runtime_truth_classification": "REAL_RUNTIME",
            "highest_readiness_layer": "BLOCKED" if not trade_allowed else "ADVISORY_READY",
            "capabilities": {
                "TRADE_ADVICE_ALLOWED": {"allowed": trade_allowed, "reason": "TEST_BLOCKED" if not trade_allowed else "TEST_ALLOWED"},
                "MANUAL_TRADE_CAPTURE_ALLOWED": {"allowed": manual_allowed, "reason": "TEST_ALLOWED" if manual_allowed else "TEST_BLOCKED"},
            },
        },
    )


def _write_control_packet_json(root: Path, *, day: str = DAY, hash_value: str = "b" * 64, readiness: str = "READY") -> Path:
    return _write_json(
        root,
        f"reports/aegis_chatgpt_control_packet_v1/{day}/aegis_chatgpt_control_packet.v1.json",
        {
            "schema_id": "aegis_chatgpt_control_packet",
            "schema_version": "v1",
            "artifact_id": "aegis_chatgpt_control_packet_v1",
            "day_utc": day,
            "generated_at": "2026-04-29T13:00:00Z",
            "packet_generated_at_utc": "2026-04-29T13:00:00Z",
            "packet_day_utc": day,
            "packet_freshness_status": "CURRENT",
            "packet_version": "aegis_chatgpt_control_packet.v1",
            "runtime_truth_classification": "REAL_RUNTIME",
            "runtime_evaluation_hash": hash_value,
            "runtime_evaluation_path": str((root / f"reports/aegis_runtime_truth_kernel_v1/{day}/runtime_evaluation.v1.json").resolve()),
            "readiness_state": {"classification": readiness},
            "trade_advice_allowed": True,
            "manual_trade_capture_allowed": True,
            "canonical_json_hash": "c" * 64,
        },
    )


def _seed_control_plane(root: Path, mode: str = "PRODUCTION") -> None:
    _write_json(
        root,
        f"reports/aegis_control_plane_v1/{DAY}/control_plane.v1.json",
        {
            "day_utc": DAY,
            "runtime_mode": mode,
            "final_status": "NOT_READY",
            "current_phase": "SESSION_AUTHORITY",
            "canonical_blocker": "HIDDEN_DEPENDENCY_DETECTED",
            "blocker_owner": "session_authority",
            "recovery_action": "Resolve hidden dependency, then rerun governed session authority.",
            "recovery_commands": [
                f'PYTHONPATH="$PWD" python3 ops/tools/run_session_authority_v1.py --target_day {DAY} --truth_root {root} --environment PAPER --phase all'
            ],
            "evidence_paths": [str(root / f"target_day_admission_v1/{DAY}.json")],
            "deferred_phases": ["BROKER_HEALTH", "BOD_INPUTS", "SUBMIT_BOUNDARY"],
            "phase_results": [
                {"phase_id": "SOURCE_INTEGRITY", "status": "PASS", "blocker_codes": [], "evidence_paths": []},
                {
                    "phase_id": "SESSION_AUTHORITY",
                    "status": "BLOCKING_CURRENT_RUN",
                    "blocker_codes": ["HIDDEN_DEPENDENCY_DETECTED"],
                    "evidence_paths": [str(root / f"target_day_admission_v1/{DAY}.json")],
                    "recovery_action": "Resolve hidden dependency, then rerun governed session authority.",
                    "recovery_commands": ["session command"],
                },
                {"phase_id": "BROKER_HEALTH", "status": "DEFERRED_BY_UPSTREAM_BLOCKER", "blocker_codes": [], "evidence_paths": []},
            ],
        },
    )
    _write_json(root, f"reports/aegis_day_run_v1/{DAY}/day_run.v1.json", {"day_utc": DAY, "final_status": "PAPER_READY", "canonical_blocker": ""})
    _write_json(root, f"reports/aegis_requirement_graph_v1/{DAY}/requirement_graph.v1.json", {"day_utc": DAY, "status": "PASS"})
    _write_json(root, f"reports/unified_truth_kernel_v1/{DAY}/unified_truth_kernel.v1.json", {"day_utc": DAY, "final_status": "READY"})
    _write_json(root, f"reports/action_validity_v1/{DAY}/action_validity.v1.json", {"action_rules": [{"action_id": "submit_paper_order", "status": "FORBIDDEN"}]})
    _write_packet(root, mode=mode)


def test_ui_readiness_uses_runtime_evaluation_as_primary_authority(tmp_path: Path, monkeypatch) -> None:
    import ops.tools.aegis_submit_enforcement_v1 as enforcement

    monkeypatch.setattr(enforcement, "_git_commit", lambda: COMMIT)
    monkeypatch.setattr(enforcement, "_git_dirty_status", lambda: "CLEAN")
    truth_root = tmp_path / "production_truth"
    sleeve_root = tmp_path / "sleeve"
    _seed_control_plane(truth_root)
    _write_json(
        truth_root,
        "governance/production_version.v1.json",
        {"schema_version": "production_version.v1", "promoted_commit": COMMIT, "status": "ACTIVE"},
    )

    payload = build_readiness_kernel_v1(DAY, truth_root=truth_root, sleeve_truth_root=sleeve_root)

    assert payload["primary_ui_authority"] == "RuntimeEvaluation"
    assert payload["primary_authority_path"].endswith("runtime_evaluation.v1.json")
    assert payload["final_readiness_authority"] == "RuntimeEvaluation"
    assert payload["current_phase"] == "SESSION_AUTHORITY"
    assert payload["canonical_blocker"] == "RUNTIME_EVALUATION_MISSING"
    assert payload["blocker_owner"] == "session_authority"
    assert payload["recovery_commands"]
    assert payload["evidence_paths"] == [str(truth_root / f"target_day_admission_v1/{DAY}.json")]
    assert payload["deferred_phases"] == ["BROKER_HEALTH", "BOD_INPUTS", "SUBMIT_BOUNDARY"]
    assert payload["supporting_evidence_only_paths"]["requirement_graph"].endswith("requirement_graph.v1.json")
    assert payload["supporting_evidence_only_paths"]["unified_truth_kernel"].endswith("unified_truth_kernel.v1.json")


def test_missing_production_version_is_submit_blocker(tmp_path: Path, monkeypatch) -> None:
    import ops.tools.aegis_submit_enforcement_v1 as enforcement

    monkeypatch.setattr(enforcement, "_git_commit", lambda: COMMIT)
    monkeypatch.setattr(enforcement, "_git_dirty_status", lambda: "CLEAN")
    truth_root = tmp_path / "production_truth"
    _seed_control_plane(truth_root)

    payload = build_readiness_kernel_v1(DAY, truth_root=truth_root, sleeve_truth_root=tmp_path / "sleeve")

    assert payload["runtime_mode"] == "PRODUCTION"
    assert payload["production_version_status"] == "MISSING"
    assert payload["submit_status"] == "BLOCKED"
    assert payload["submit_canonical_blocker"] == "CONTROL_PLANE_NOT_READY"


def test_candidate_mode_ui_submit_disabled(tmp_path: Path, monkeypatch) -> None:
    import ops.tools.aegis_submit_enforcement_v1 as enforcement

    monkeypatch.setattr(enforcement, "_git_commit", lambda: COMMIT)
    monkeypatch.setattr(enforcement, "_git_dirty_status", lambda: "CLEAN")
    truth_root = tmp_path / "candidate_truth"
    _seed_control_plane(truth_root, mode="CANDIDATE")

    payload = build_readiness_kernel_v1(DAY, truth_root=truth_root, sleeve_truth_root=tmp_path / "sleeve")

    assert payload["runtime_mode"] == "CANDIDATE"
    assert payload["production_version_status"] == "NOT_REQUIRED_FOR_CANDIDATE"
    assert payload["submit_status"] == "BLOCKED"
    assert payload["submit_canonical_blocker"] == "CONTROL_PLANE_NOT_READY"


def test_requirement_graph_and_kernel_cannot_override_control_plane(tmp_path: Path, monkeypatch) -> None:
    import ops.tools.aegis_submit_enforcement_v1 as enforcement

    monkeypatch.setattr(enforcement, "_git_commit", lambda: COMMIT)
    monkeypatch.setattr(enforcement, "_git_dirty_status", lambda: "CLEAN")
    truth_root = tmp_path / "production_truth"
    _seed_control_plane(truth_root)
    _write_json(truth_root, f"reports/aegis_requirement_graph_v1/{DAY}/requirement_graph.v1.json", {"status": "PASS", "canonical_blocker": ""})
    _write_json(truth_root, f"reports/unified_truth_kernel_v1/{DAY}/unified_truth_kernel.v1.json", {"final_status": "READY", "canonical_blocker": ""})

    payload = build_readiness_kernel_v1(DAY, truth_root=truth_root, sleeve_truth_root=tmp_path / "sleeve")

    assert payload["overall_status"] == "BLOCKED"
    assert payload["canonical_blocker"] == "RUNTIME_EVALUATION_MISSING"
    assert payload["truth_resolution_source_path"] == ""


def test_legacy_ready_surfaces_cannot_override_control_plane_submit_block(tmp_path: Path, monkeypatch) -> None:
    import ops.tools.aegis_submit_enforcement_v1 as enforcement

    monkeypatch.setattr(enforcement, "_git_commit", lambda: COMMIT)
    monkeypatch.setattr(enforcement, "_git_dirty_status", lambda: "CLEAN")
    truth_root = tmp_path / "production_truth"
    sleeve_root = tmp_path / "sleeve"
    _seed_control_plane(truth_root)
    _write_json(
        truth_root,
        "governance/production_version.v1.json",
        {"schema_version": "production_version.v1", "promoted_commit": COMMIT, "status": "ACTIVE"},
    )
    _write_json(
        truth_root,
        f"reports/submit_boundary_status_v1/{DAY}/submit_boundary_status.v1.json",
        {"status": "PASS", "submit_allowed": True, "submission_authorized": True},
    )
    _write_json(
        truth_root,
        f"reports/action_validity_v1/{DAY}/action_validity.v1.json",
        {"action_rules": [{"action_id": "submit_paper_order", "status": "ALLOWED"}]},
    )
    _write_json(
        truth_root,
        f"reports/truth_freshness_v1/{DAY}/truth_freshness.v1.json",
        {
            "freshness_records": [
                {"artifact_type": "aegis_day_run_v1", "freshness_status": "FRESH"},
                {"artifact_type": "submit_boundary_status_v1", "freshness_status": "FRESH"},
                {"artifact_type": "action_validity_v1", "freshness_status": "FRESH"},
            ]
        },
    )
    _write_json(
        sleeve_root,
        f"risk_v1/kill_switch_v1/{DAY}/global_kill_switch_state.v1.json",
        {"state": "INACTIVE", "active": False},
    )

    payload = build_readiness_kernel_v1(DAY, truth_root=truth_root, sleeve_truth_root=sleeve_root)

    assert payload["overall_status"] == "BLOCKED"
    assert payload["canonical_blocker"] == "RUNTIME_EVALUATION_MISSING"
    assert payload["submit_status"] == "BLOCKED"
    assert payload["submit_canonical_blocker"] == "CONTROL_PLANE_NOT_READY"


def test_server_and_ui_are_wired_to_control_plane_readiness() -> None:
    repo = Path(__file__).resolve().parents[4]
    server = (repo / "constellation_2/phaseL/ui/server/run_ops_dashboard_v1.py").read_text(encoding="utf-8")
    client = (
        repo / "constellation_2/phaseL/ui/static/operator_shell/domain_client/index.js"
    ).read_text(encoding="utf-8")
    pages = (repo / "constellation_2/phaseL/ui/static/operator_shell/pages/index.js").read_text(encoding="utf-8")

    assert 'path == "/api/readiness-kernel"' in server
    assert "build_readiness_kernel_v1" in server
    assert "fetchReadinessKernel" in client
    assert 'query("/api/readiness-kernel"' in client
    assert "fetchReadinessKernel()" in pages
    assert "renderReadinessKernelLadder(readinessKernel, state)" in pages
    assert "Phase-Controlled Readiness" in pages
    assert "aegis_control_plane_v1" in pages
    assert "safeList(operations.readiness_ladder)" not in pages


def test_stale_control_packet_current_runtime_evaluation_uses_runtime_authority(tmp_path: Path, monkeypatch) -> None:
    import ops.tools.aegis_submit_enforcement_v1 as enforcement

    monkeypatch.setattr(enforcement, "_git_commit", lambda: COMMIT)
    monkeypatch.setattr(enforcement, "_git_dirty_status", lambda: "CLEAN")
    truth_root = tmp_path / "production_truth"
    _seed_control_plane(truth_root)
    _write_runtime_evaluation(truth_root, hash_value="b" * 64, trade_allowed=False, manual_allowed=False)
    _write_control_packet_json(truth_root, day="2026-04-28", hash_value="b" * 64, readiness="READY")

    payload = build_readiness_kernel_v1(DAY, truth_root=truth_root, sleeve_truth_root=tmp_path / "sleeve")

    assert payload["primary_ui_authority"] == "RuntimeEvaluation"
    assert payload["runtime_evaluation_hash"] == "b" * 64
    assert payload["overall_status"] == "BLOCKED"
    assert payload["packet"]["packet_freshness_status"] in {"MISSING", "STALE"}


def test_hash_mismatch_packet_is_explanatory_and_marked_mismatch(tmp_path: Path, monkeypatch) -> None:
    import ops.tools.aegis_submit_enforcement_v1 as enforcement

    monkeypatch.setattr(enforcement, "_git_commit", lambda: COMMIT)
    monkeypatch.setattr(enforcement, "_git_dirty_status", lambda: "CLEAN")
    truth_root = tmp_path / "production_truth"
    _seed_control_plane(truth_root)
    _write_runtime_evaluation(truth_root, hash_value="b" * 64, trade_allowed=False, manual_allowed=False)
    _write_control_packet_json(truth_root, hash_value="d" * 64, readiness="READY")

    payload = build_readiness_kernel_v1(DAY, truth_root=truth_root, sleeve_truth_root=tmp_path / "sleeve")

    assert payload["packet"]["packet_freshness_status"] == "HASH_MISMATCH"
    assert payload["packet"]["readiness_usage"] == "EXPLANATORY_ONLY"
    assert payload["overall_status"] == "BLOCKED"


def test_missing_packet_still_projects_from_runtime_evaluation(tmp_path: Path, monkeypatch) -> None:
    import ops.tools.aegis_submit_enforcement_v1 as enforcement

    monkeypatch.setattr(enforcement, "_git_commit", lambda: COMMIT)
    monkeypatch.setattr(enforcement, "_git_dirty_status", lambda: "CLEAN")
    truth_root = tmp_path / "production_truth"
    _seed_control_plane(truth_root)
    packet_path = truth_root / "exports/aegis_state/latest/chatgpt_aegis_packet.md"
    packet_path.unlink()
    _write_runtime_evaluation(truth_root, hash_value="b" * 64, trade_allowed=False, manual_allowed=False)

    payload = build_readiness_kernel_v1(DAY, truth_root=truth_root, sleeve_truth_root=tmp_path / "sleeve")

    assert payload["packet"]["packet_freshness_status"] == "MISSING"
    assert payload["runtime_evaluation_hash"] == "b" * 64
    assert payload["overall_status"] == "BLOCKED"


def test_packet_ready_cannot_override_blocked_runtime_evaluation(tmp_path: Path, monkeypatch) -> None:
    import ops.tools.aegis_submit_enforcement_v1 as enforcement

    monkeypatch.setattr(enforcement, "_git_commit", lambda: COMMIT)
    monkeypatch.setattr(enforcement, "_git_dirty_status", lambda: "CLEAN")
    truth_root = tmp_path / "production_truth"
    _seed_control_plane(truth_root)
    _write_runtime_evaluation(truth_root, hash_value="b" * 64, trade_allowed=False, manual_allowed=False)
    _write_control_packet_json(truth_root, hash_value="b" * 64, readiness="READY")

    payload = build_readiness_kernel_v1(DAY, truth_root=truth_root, sleeve_truth_root=tmp_path / "sleeve")

    assert payload["packet"]["packet_freshness_status"] == "CURRENT"
    assert payload["overall_status"] == "BLOCKED"
    assert payload["canonical_blocker"] == "MANUAL_TRADE_CAPTURE_ALLOWED,TRADE_ADVICE_ALLOWED"
    assert payload["trade_advice_allowed"] is False


def test_runtime_manual_capture_allowed_survives_stale_packet_warning(tmp_path: Path, monkeypatch) -> None:
    import ops.tools.aegis_submit_enforcement_v1 as enforcement

    monkeypatch.setattr(enforcement, "_git_commit", lambda: COMMIT)
    monkeypatch.setattr(enforcement, "_git_dirty_status", lambda: "CLEAN")
    truth_root = tmp_path / "production_truth"
    _seed_control_plane(truth_root)
    _write_runtime_evaluation(truth_root, hash_value="b" * 64, trade_allowed=False, manual_allowed=True)
    _write_control_packet_json(truth_root, hash_value="d" * 64, readiness="READY")

    payload = build_readiness_kernel_v1(DAY, truth_root=truth_root, sleeve_truth_root=tmp_path / "sleeve")

    assert payload["manual_trade_capture_allowed"] is True
    assert payload["trade_advice_allowed"] is False
    assert payload["operator_mode"] == "manual_capture_only"
    assert payload["platform_capture_capability"] == "READY"
    assert payload["capture_ticket_count"] == 0
    assert payload["capture_ticket_status"] == "NONE_AVAILABLE"
    assert payload["manual_capture_summary"] == [
        "Manual capture capability: READY",
        "IB capture tickets: 0",
        "Capture ticket status: NONE_AVAILABLE",
        "No action required",
        "Submit-boundary VALIDATED",
        "Broker submit DISABLED",
        "IB handshake NOT REQUIRED FOR MANUAL CAPTURE",
    ]
    assert payload["broker_submit_enabled"] is False
    assert payload["paper_broker_simulation_enabled"] is False
    assert payload["autonomous_execution_allowed"] is False
    assert payload["packet"]["packet_freshness_status"] == "HASH_MISMATCH"
    assert payload["overall_status"] == "BLOCKED"
