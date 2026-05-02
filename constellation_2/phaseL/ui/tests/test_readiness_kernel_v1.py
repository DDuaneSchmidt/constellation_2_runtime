from __future__ import annotations

import json
from pathlib import Path

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


def test_ui_readiness_uses_control_plane_as_primary_authority(tmp_path: Path, monkeypatch) -> None:
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

    assert payload["primary_ui_authority"] == "aegis_control_plane_v1"
    assert payload["primary_authority_path"].endswith("control_plane.v1.json")
    assert payload["final_readiness_authority"] == "aegis_control_plane_v1"
    assert payload["current_phase"] == "SESSION_AUTHORITY"
    assert payload["canonical_blocker"] == "HIDDEN_DEPENDENCY_DETECTED"
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
    assert payload["submit_canonical_blocker"] == "PRODUCTION_VERSION_MISSING"


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
    assert payload["submit_canonical_blocker"] == "CANDIDATE_RUNTIME_SUBMIT_DISABLED"


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
    assert payload["canonical_blocker"] == "HIDDEN_DEPENDENCY_DETECTED"
    assert payload["truth_resolution_source_path"].endswith("control_plane.v1.json")


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
    assert payload["canonical_blocker"] == "HIDDEN_DEPENDENCY_DETECTED"
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
