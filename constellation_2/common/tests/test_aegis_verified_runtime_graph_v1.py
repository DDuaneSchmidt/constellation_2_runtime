from __future__ import annotations

import json
import sys
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[3]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from ops.aegis.verified_runtime_graph_v1 import (
    build_chatgpt_hydrate_packet_v1,
    build_evidence_ledger_v1,
    build_portal_runtime_model_v1,
    build_verified_runtime_graph_v1,
    discover_manifests_v1,
    graph_staleness_warnings_v1,
    render_graph_diff_v1,
    render_query_response_v1,
    validate_evidence_id_contract_v1,
    validate_manifest_v1,
    write_verified_runtime_graph_outputs_v1,
)


DAY = "2026-05-22"


def _write_json(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, sort_keys=True) + "\n", encoding="utf-8")


def _kernel_payload(day: str = DAY) -> dict:
    return {
        "schema_id": "aegis_runtime_truth_kernel",
        "schema_version": "v1",
        "artifact_id": "aegis_runtime_truth_kernel_v1",
        "day_utc": day,
        "generated_at_utc": f"{day}T21:00:00Z",
        "runtime_truth_classification": "REAL_RUNTIME",
        "highest_readiness_layer": "HUMAN_APPROVED_ADVISORY_RUNTIME_READY",
        "blocked_capabilities": [],
        "allowed_capabilities": ["DATA_READY"],
        "trade_advice_allowed": False,
        "manual_trade_capture_allowed": False,
        "broker_submit_transmit_policy": "DISABLED_BY_DESIGN",
        "autonomous_execution_policy": "DISABLED_BY_DESIGN",
        "do_not_claim": ["kernel do-not-claim propagated"],
        "runtime_evaluation": {"capabilities": {}},
    }


def _seed_kernel(root: Path, day: str = DAY) -> None:
    base = root / "reports" / "aegis_runtime_truth_kernel_v1" / day
    _write_json(base / "runtime_truth_kernel.v1.json", _kernel_payload(day))
    _write_json(
        base / "runtime_evaluation.v1.json",
        {
            "schema_id": "runtime_evaluation",
            "schema_version": "v1",
            "day_utc": day,
            "generated_at": f"{day}T21:00:00Z",
            "deterministic_output_hash": "runtime-eval-hash",
        },
    )


def _seed_control_packet(root: Path, day: str = DAY) -> None:
    _write_json(
        root / "reports" / "aegis_chatgpt_control_packet_v1" / day / "aegis_chatgpt_control_packet.v1.json",
        {
            "schema_id": "aegis_chatgpt_control_packet",
            "schema_version": "v1",
            "day_utc": day,
            "generated_at": f"{day}T21:01:00Z",
            "do_not_claim": ["control packet do-not-claim propagated"],
        },
    )


def _write_manifest(
    modules_root: Path,
    *,
    module_id: str = "unit_module",
    capability_id: str = "unit_capability",
    evidence_path: str = "reports/aegis_runtime_truth_kernel_v1/{day}/runtime_truth_kernel.v1.json",
    evidence_id: str = "unit_evidence",
    route_capability_id: str | None = None,
    tests: list[dict] | None = None,
) -> Path:
    manifest = {
        "module_id": module_id,
        "display_name": "Unit Module",
        "type": "test",
        "owner": "tests",
        "capabilities": [
            {
                "capability_id": capability_id,
                "display_name": "Unit Capability",
                "required_evidence": [evidence_id],
                "allowed_actions": ["READ_ONLY"],
            }
        ],
        "inputs": [],
        "outputs": [],
        "commands": [{"name": "unit", "command": "pytest"}],
        "tests": tests if tests is not None else [{"path": "constellation_2/common/tests/test_aegis_verified_runtime_graph_v1.py"}],
        "evidence_artifacts": [
            {
                "evidence_id": evidence_id,
                "artifact_type": "unit",
                "path": evidence_path,
                "producer": "tests",
                "day_scoped": True,
            }
        ],
        "ui_surfaces": [
            {
                "surface_id": "unit_surface",
                "route": "/unit",
                "capability_id": route_capability_id or capability_id,
            }
        ],
        "policies": ["read only"],
        "freshness_requirements": [{"evidence_id": evidence_id, "day_scoped": True}],
    }
    path = modules_root / module_id / "aegis.module.yaml"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(manifest, indent=2) + "\n", encoding="utf-8")
    return path


def test_manifest_schema_validation_blocks_invalid_manifest(tmp_path: Path) -> None:
    root = tmp_path / "truth"
    modules = tmp_path / "modules"
    _seed_kernel(root)
    (modules / "bad").mkdir(parents=True)
    (modules / "bad" / "aegis.module.yaml").write_text(json.dumps({"module_id": "bad"}) + "\n", encoding="utf-8")

    graph = build_verified_runtime_graph_v1(truth_root=root, day_utc=DAY, modules_root=modules, generated_at="2026-05-22T22:00:00Z")

    assert graph["graph_status"] == "BLOCKED"
    assert any("missing required manifest keys" in blocker for blocker in graph["audit_blockers"])


def test_missing_evidence_blocks_verification(tmp_path: Path) -> None:
    root = tmp_path / "truth"
    modules = tmp_path / "modules"
    _seed_kernel(root)
    _write_manifest(modules, evidence_path="reports/missing/{day}/missing.v1.json")

    graph = build_verified_runtime_graph_v1(truth_root=root, day_utc=DAY, modules_root=modules, generated_at="2026-05-22T22:00:00Z")

    capability = graph["verified_capabilities"][0]
    assert capability["state"] == "BLOCKED"
    assert "REQUIRED_EVIDENCE_MISSING:unit_evidence" in capability["blockers"]


def test_stale_evidence_blocks_readiness(tmp_path: Path) -> None:
    root = tmp_path / "truth"
    modules = tmp_path / "modules"
    _seed_kernel(root)
    stale_path = root / "reports" / "unit" / DAY / "unit.v1.json"
    _write_json(stale_path, {"schema_id": "unit", "schema_version": "v1", "day_utc": "2026-05-21"})
    _write_manifest(modules, evidence_path="reports/unit/{day}/unit.v1.json")

    graph = build_verified_runtime_graph_v1(truth_root=root, day_utc=DAY, modules_root=modules, generated_at="2026-05-22T22:00:00Z")

    capability = graph["verified_capabilities"][0]
    assert capability["state"] == "BLOCKED"
    assert "REQUIRED_EVIDENCE_STALE:unit_evidence" in capability["blockers"]


def test_invalid_portal_binding_blocks_portal_readiness(tmp_path: Path) -> None:
    root = tmp_path / "truth"
    modules = tmp_path / "modules"
    _seed_kernel(root)
    _write_manifest(modules, route_capability_id="missing_capability")

    graph = build_verified_runtime_graph_v1(truth_root=root, day_utc=DAY, modules_root=modules, generated_at="2026-05-22T22:00:00Z")

    assert graph["portal_bindings"]["status"] == "BLOCKED"
    assert any("PORTAL_BINDING_MISSING_CAPABILITY:missing_capability" in blocker for blocker in graph["audit_blockers"])


def test_hydrate_refuses_stale_graph(tmp_path: Path) -> None:
    root = tmp_path / "truth"
    modules = tmp_path / "modules"
    _seed_kernel(root)
    _write_manifest(modules)
    graph = build_verified_runtime_graph_v1(truth_root=root, day_utc=DAY, modules_root=modules, generated_at="2026-05-22T22:00:00Z")

    with pytest.raises(ValueError, match="stale graph"):
        build_chatgpt_hydrate_packet_v1(graph=graph, truth_root=root, day_utc="2026-05-23")


def test_query_output_is_deterministic(tmp_path: Path) -> None:
    root = tmp_path / "truth"
    modules = tmp_path / "modules"
    _seed_kernel(root)
    _write_manifest(modules, evidence_path="reports/missing/{day}/missing.v1.json")
    graph = build_verified_runtime_graph_v1(truth_root=root, day_utc=DAY, modules_root=modules, generated_at="2026-05-22T22:00:00Z")

    first = render_query_response_v1(graph=graph, query="why blocked?")
    second = render_query_response_v1(graph=graph, query="why blocked?")

    assert first == second
    assert "REQUIRED_EVIDENCE_MISSING" in first


def test_do_not_claim_rules_propagate_from_control_packet(tmp_path: Path) -> None:
    root = tmp_path / "truth"
    modules = tmp_path / "modules"
    _seed_kernel(root)
    _seed_control_packet(root)
    _write_manifest(modules)
    graph = build_verified_runtime_graph_v1(truth_root=root, day_utc=DAY, modules_root=modules, generated_at="2026-05-22T22:00:00Z")

    packet = build_chatgpt_hydrate_packet_v1(graph=graph, truth_root=root, day_utc=DAY, generated_at="2026-05-22T22:05:00Z")

    assert "kernel do-not-claim propagated" in graph["do_not_claim"]
    assert "control packet do-not-claim propagated" in graph["do_not_claim"]
    assert "control packet do-not-claim propagated" in packet


def test_outputs_include_portal_model_and_no_execution_claims(tmp_path: Path) -> None:
    root = tmp_path / "truth"
    modules = tmp_path / "modules"
    _seed_kernel(root)
    _write_manifest(modules)
    graph = build_verified_runtime_graph_v1(truth_root=root, day_utc=DAY, modules_root=modules, generated_at="2026-05-22T22:00:00Z")

    paths = write_verified_runtime_graph_outputs_v1(truth_root=root, graph=graph)
    portal = json.loads(Path(paths["portal_runtime_model"]).read_text(encoding="utf-8"))

    assert Path(paths["verified_runtime_graph"]).exists()
    assert Path(paths["evidence_ledger"]).exists()
    assert portal["safety_invariants"]["broker_submit_transmit_allowed"] is False
    assert portal["safety_invariants"]["autonomous_execution_allowed"] is False


def test_schema_files_exist_for_generated_artifacts() -> None:
    schema_dir = REPO_ROOT / "governance" / "04_DATA" / "SCHEMAS" / "C2" / "REPORTS"
    for name in (
        "verified_runtime_graph.v1.schema.json",
        "evidence_ledger.v1.schema.json",
        "portal_runtime_model.v1.schema.json",
        "chatgpt_hydrate_packet.v1.schema.json",
    ):
        payload = json.loads((schema_dir / name).read_text(encoding="utf-8"))
        assert payload["$schema"]
        assert payload["type"] == "object"


def test_ledger_detects_artifact_hash_mismatch(tmp_path: Path) -> None:
    root = tmp_path / "truth"
    modules = tmp_path / "modules"
    _seed_kernel(root)
    _write_manifest(modules)
    graph = build_verified_runtime_graph_v1(truth_root=root, day_utc=DAY, modules_root=modules, generated_at="2026-05-22T22:00:00Z")
    kernel_path = root / "reports" / "aegis_runtime_truth_kernel_v1" / DAY / "runtime_truth_kernel.v1.json"
    _write_json(kernel_path, {**_kernel_payload(DAY), "generated_at_utc": "2026-05-22T23:00:00Z"})

    ledger = build_evidence_ledger_v1(graph=graph)

    assert ledger["hash_verification_status"] == "BLOCKED"
    assert any(entry["hash_verification_status"] == "MISMATCH" for entry in ledger["entries"])


def test_graph_diff_reports_state_and_hash_changes(tmp_path: Path) -> None:
    root = tmp_path / "truth"
    modules = tmp_path / "modules"
    _seed_kernel(root, "2026-05-21")
    _seed_kernel(root, DAY)
    _write_manifest(modules)
    from_graph = build_verified_runtime_graph_v1(truth_root=root, day_utc="2026-05-21", modules_root=modules, generated_at="2026-05-21T22:00:00Z")
    to_graph = build_verified_runtime_graph_v1(truth_root=root, day_utc=DAY, modules_root=modules, generated_at="2026-05-22T22:00:00Z")

    diff = render_graph_diff_v1(from_graph=from_graph, to_graph=to_graph)

    assert "AEGIS VERIFIED RUNTIME GRAPH DIFF v1" in diff
    assert "Evidence Hash Changes:" in diff
    assert "runtime_readiness_status:" in diff


def test_stale_graph_detection_warns_and_hydrate_refuses_kernel_newer(tmp_path: Path) -> None:
    root = tmp_path / "truth"
    modules = tmp_path / "modules"
    _seed_kernel(root)
    _write_manifest(modules)
    graph = build_verified_runtime_graph_v1(truth_root=root, day_utc=DAY, modules_root=modules, generated_at="2026-05-22T22:00:00Z")
    kernel_path = root / "reports" / "aegis_runtime_truth_kernel_v1" / DAY / "runtime_truth_kernel.v1.json"
    _write_json(kernel_path, {**_kernel_payload(DAY), "generated_at_utc": "2026-05-22T23:00:00Z"})

    warnings = graph_staleness_warnings_v1(graph=graph, truth_root=root, day_utc=DAY)

    assert any("GRAPH_STALE_KERNEL" in warning for warning in warnings)
    with pytest.raises(ValueError, match="hydrate refuses stale graph"):
        build_chatgpt_hydrate_packet_v1(graph=graph, truth_root=root, day_utc=DAY)


def test_portal_runtime_model_is_graph_derived_only(tmp_path: Path) -> None:
    root = tmp_path / "truth"
    modules = tmp_path / "modules"
    _seed_kernel(root)
    _write_manifest(modules)
    graph = build_verified_runtime_graph_v1(truth_root=root, day_utc=DAY, modules_root=modules, generated_at="2026-05-22T22:00:00Z")

    portal = build_portal_runtime_model_v1(graph=graph)

    assert portal["derivation_source"] == "verified_runtime_graph.v1.json"
    assert portal["readiness_inference_policy"] == "NO_INDEPENDENT_PORTAL_READINESS_INFERENCE"
    graph_states = {(row["module_id"], row["capability_id"]): row["state"] for row in graph["verified_capabilities"]}
    portal_states = {(row["module_id"], row["capability_id"]): row["state"] for row in portal["capabilities"]}
    assert portal_states == graph_states
    assert "runtime_truth" in portal
    assert "runtime_evaluation" not in portal


def test_manifest_negative_cases_are_blocked() -> None:
    valid = {
        "module_id": "bad_manifest",
        "display_name": "Bad Manifest",
        "type": "test",
        "owner": "tests",
        "capabilities": [{"capability_id": "cap", "required_evidence": ["ev"]}],
        "inputs": [],
        "outputs": [],
        "commands": [{"name": "unit", "command": "pytest"}],
        "tests": [{"path": "constellation_2/common/tests/test_aegis_verified_runtime_graph_v1.py"}],
        "evidence_artifacts": [{"evidence_id": "ev", "path": "reports/unit/{day}/unit.v1.json"}],
        "ui_surfaces": [{"surface_id": "surface", "route": "/unit", "capability_id": "cap"}],
        "policies": ["read only"],
        "freshness_requirements": [{"evidence_id": "ev"}],
    }
    assert any("missing required manifest keys" in error for error in validate_manifest_v1({"module_id": "bad"}))
    assert "registered module has no declared commands" in validate_manifest_v1({**valid, "commands": []})
    assert "registered module has no validation tests" in validate_manifest_v1({**valid, "tests": []})
    assert "registered module has no evidence artifacts" in validate_manifest_v1({**valid, "evidence_artifacts": []})
    assert any("required_evidence must be a list" in error for error in validate_manifest_v1({**valid, "capabilities": [{"capability_id": "cap", "required_evidence": "ev"}]}))


def test_required_evidence_without_graph_visible_artifact_is_graph_blocker(tmp_path: Path) -> None:
    root = tmp_path / "truth"
    modules = tmp_path / "modules"
    _seed_kernel(root)
    _write_manifest(modules, evidence_id="declared_evidence")
    manifest_path = modules / "unit_module" / "aegis.module.yaml"
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    manifest["capabilities"][0]["required_evidence"] = ["undeclared_evidence"]
    manifest_path.write_text(json.dumps(manifest, indent=2) + "\n", encoding="utf-8")

    graph = build_verified_runtime_graph_v1(truth_root=root, day_utc=DAY, modules_root=modules, generated_at="2026-05-22T22:00:00Z")

    assert graph["graph_status"] == "BLOCKED"
    assert any(
        "unit_module:unit_capability:required_evidence unresolved by graph-visible evidence_artifacts:undeclared_evidence"
        in blocker
        for blocker in graph["audit_blockers"]
    )


def test_current_aegis_required_evidence_contracts_resolve_trend_diagnostic() -> None:
    manifests = discover_manifests_v1(REPO_ROOT / "aegis" / "modules")

    assert validate_evidence_id_contract_v1(manifests) == []

    trend_requirements: dict[str, set[str]] = {}
    for manifest in manifests:
        module_id = str(manifest.get("module_id") or "")
        for capability in manifest.get("capabilities") or []:
            if not isinstance(capability, dict):
                continue
            if capability.get("capability_id") == "trend_eq_realized_vs_unrealized_diagnostic_v1":
                trend_requirements[module_id] = {str(item) for item in capability.get("required_evidence") or []}

    assert trend_requirements["operator_portal"] >= {
        "aegis_paper_position_ledger_v1",
        "aegis_sleeve_performance_truth_v1",
    }
    assert trend_requirements["runtime_truth_kernel"] >= {
        "aegis_paper_position_ledger_v1",
        "aegis_sleeve_performance_truth_v1",
    }


def test_manifest_invalid_ui_binding_is_graph_blocker(tmp_path: Path) -> None:
    root = tmp_path / "truth"
    modules = tmp_path / "modules"
    _seed_kernel(root)
    _write_manifest(modules, route_capability_id="not_a_capability")

    graph = build_verified_runtime_graph_v1(truth_root=root, day_utc=DAY, modules_root=modules, generated_at="2026-05-22T22:00:00Z")

    assert any("PORTAL_BINDING_MISSING_CAPABILITY:not_a_capability" in blocker for blocker in graph["audit_blockers"])


def test_claim_to_evidence_lookup_returns_policy_and_kernel_evidence(tmp_path: Path) -> None:
    root = tmp_path / "truth"
    modules = tmp_path / "modules"
    _seed_kernel(root)
    _write_manifest(modules)
    graph = build_verified_runtime_graph_v1(truth_root=root, day_utc=DAY, modules_root=modules, generated_at="2026-05-22T22:00:00Z")

    output = render_query_response_v1(graph=graph, query="claim: trade advice allowed")

    assert "Claim Lookup:" in output
    assert "- allowed: false" in output
    assert "kernel_evidence:" in output
    assert "policy_evidence:" in output
    assert "Trade advice" in output or "trade advice" in output



def _write_operator_portal_manifest(modules_root: Path) -> None:
    manifest = {
        "module_id": "operator_portal",
        "display_name": "Operator Portal",
        "type": "portal_adapter",
        "owner": "tests",
        "capabilities": [
            {
                "capability_id": "portal_runtime_model",
                "display_name": "Portal runtime model",
                "required_evidence": ["runtime_truth_kernel", "chatgpt_control_packet", "canonical_operator_state", "aegis_candidate_state"],
                "allowed_actions": ["READ_ONLY"],
            },
            {
                "capability_id": "change_control_intelligence_layer_v1",
                "display_name": "Change Control Intelligence",
                "required_evidence": ["aegis_change_control_evidence_snapshot_v1", "aegis_change_control_advisor_score_v1", "aegis_change_control_ai_review_v1"],
                "allowed_actions": ["READ_ONLY"],
            },
        ],
        "inputs": [],
        "outputs": [],
        "commands": [{"name": "audit", "command": "npm run aegis:audit"}],
        "tests": [{"path": "constellation_2/common/tests/test_aegis_verified_runtime_graph_v1.py"}],
        "evidence_artifacts": [
            {"evidence_id": "runtime_truth_kernel", "artifact_type": "runtime_truth_kernel", "path": "reports/aegis_runtime_truth_kernel_v1/{day}/runtime_truth_kernel.v1.json", "producer": "tests", "day_scoped": True},
            {"evidence_id": "chatgpt_control_packet", "artifact_type": "control_packet", "path": "reports/aegis_chatgpt_control_packet_v1/{day}/aegis_chatgpt_control_packet.v1.json", "producer": "tests", "day_scoped": True},
            {"evidence_id": "canonical_operator_state", "artifact_type": "canonical_operator_state", "path": "reports/aegis_canonical_operator_state_v1/{day}/canonical_operator_state.v1.json", "producer": "ops.tools.build_aegis_canonical_operator_state_v1", "day_scoped": True},
            {"evidence_id": "aegis_candidate_state", "artifact_type": "aegis_candidate_state", "path": "reports/aegis_candidate_state_v1/{day}/candidate_state.v1.json", "producer": "ops.tools.roll_aegis_candidate_state_v1", "day_scoped": True},
            {"evidence_id": "aegis_change_control_evidence_snapshot_v1", "artifact_type": "change_control_evidence_snapshot", "path": "reports/aegis_change_control_evidence_snapshot_v1/{day}/change_control_evidence_snapshot.v1.json", "producer": "ops.tools.run_aegis_change_control_intelligence_v1", "day_scoped": True},
            {"evidence_id": "aegis_change_control_advisor_score_v1", "artifact_type": "change_control_advisor_score", "path": "reports/aegis_change_control_advisor_score_v1/{day}/change_control_advisor_score.v1.json", "producer": "ops.tools.run_aegis_change_control_intelligence_v1", "day_scoped": True},
            {"evidence_id": "aegis_change_control_ai_review_v1", "artifact_type": "change_control_ai_review", "path": "reports/aegis_change_control_ai_review_v1/{day}/change_control_ai_review.v1.json", "producer": "ops.tools.run_aegis_change_control_intelligence_v1", "day_scoped": True},
        ],
        "ui_surfaces": [{"surface_id": "research_portfolio", "route": "/aegis-research-portfolio", "capability_id": "portal_runtime_model"}],
        "policies": ["read_only"],
        "freshness_requirements": [],
    }
    path = modules_root / "operator_portal" / "aegis.module.yaml"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(manifest, indent=2) + "\n", encoding="utf-8")


def _seed_operator_portal_required_evidence(root: Path, day: str = DAY) -> None:
    _seed_kernel(root, day)
    _seed_control_packet(root, day)
    for family, filename, schema in (
        ("aegis_canonical_operator_state_v1", "canonical_operator_state.v1.json", "canonical_operator_state"),
        ("aegis_candidate_state_v1", "candidate_state.v1.json", "aegis_candidate_state"),
        ("aegis_change_control_evidence_snapshot_v1", "change_control_evidence_snapshot.v1.json", "aegis_change_control_evidence_snapshot_v1"),
        ("aegis_change_control_advisor_score_v1", "change_control_advisor_score.v1.json", "aegis_change_control_advisor_score_v1"),
        ("aegis_change_control_ai_review_v1", "change_control_ai_review.v1.json", "aegis_change_control_ai_review_v1"),
    ):
        _write_json(root / "reports" / family / day / filename, {"schema_id": schema, "schema_version": "v1", "day_utc": day, "target_day": day, "generated_at": f"{day}T22:00:00Z"})


def test_verified_graph_consumes_change_control_and_portal_runtime_evidence(tmp_path: Path) -> None:
    root = tmp_path / "truth"
    modules = tmp_path / "modules"
    _seed_operator_portal_required_evidence(root)
    _write_operator_portal_manifest(modules)

    graph = build_verified_runtime_graph_v1(truth_root=root, day_utc=DAY, modules_root=modules, generated_at="2026-05-22T22:05:00Z")

    states = {row["capability_id"]: row for row in graph["verified_capabilities"]}
    assert graph["graph_status"] == "READY"
    assert states["portal_runtime_model"]["state"] in {"VERIFIED", "ALLOWED"}
    assert states["change_control_intelligence_layer_v1"]["state"] in {"VERIFIED", "ALLOWED"}
    consumed = {entry["evidence_id"]: entry for entry in graph["evidence_links"]}
    assert consumed["canonical_operator_state"]["freshness_status"] == "CURRENT"
    assert consumed["aegis_change_control_ai_review_v1"]["validation_status"] == "VALID"


def test_portal_runtime_model_generation_exposes_current_operator_surfaces(tmp_path: Path) -> None:
    root = tmp_path / "truth"
    modules = tmp_path / "modules"
    _seed_operator_portal_required_evidence(root)
    _write_operator_portal_manifest(modules)
    graph = build_verified_runtime_graph_v1(truth_root=root, day_utc=DAY, modules_root=modules, generated_at="2026-05-22T22:05:00Z")
    portal = build_portal_runtime_model_v1(graph=graph)

    assert portal["schema_id"] == "portal_runtime_model"
    assert portal["portal_status"] == "READY"
    assert any(row["route"] == "/aegis-research-portfolio" and row["status"] == "READY" for row in portal["surfaces"])
    assert portal["capabilities"][0]["state"] in {"ALLOWED", "VERIFIED"}


def test_audit_runs_portal_and_change_control_producers_before_verified_graph() -> None:
    package = json.loads((REPO_ROOT / "package.json").read_text(encoding="utf-8"))
    audit = package["scripts"]["aegis:audit"]
    assert audit.index("aegis:canonical-operator-state") < audit.index("aegis:verified-graph")
    assert audit.index("aegis:roll-candidate-state") < audit.index("aegis:verified-graph")
    assert audit.index("aegis:change-control-intelligence") < audit.index("aegis:verified-graph")
