from __future__ import annotations

import json
import subprocess
from pathlib import Path
from types import SimpleNamespace

from ops.aegis.verified_runtime_graph_v1 import build_portal_runtime_model_v1
from constellation_2.phaseL.ui.server.run_ops_dashboard_v1 import (
    _load_verified_runtime_recent_actions_v1,
    _portal_action_audit_path_v1,
    _run_verified_runtime_action_v1,
)


ROOT = Path(__file__).resolve().parents[4]
SERVER = ROOT / "constellation_2/phaseL/ui/server/run_ops_dashboard_v1.py"
CLIENT = ROOT / "constellation_2/phaseL/ui/static/operator_shell/domain_client/index.js"
PAGES = ROOT / "constellation_2/phaseL/ui/static/operator_shell/pages/index.js"
MAIN = ROOT / "constellation_2/phaseL/ui/static/operator_shell/main.js"
NAV = ROOT / "constellation_2/phaseL/ui/static/operator_shell/navigation_schema.js"
ROUTES = ROOT / "constellation_2/phaseL/ui/static/operator_shell/pages/route_metadata.js"


def _minimal_graph() -> dict:
    return {
        "schema_id": "aegis_verified_runtime_graph",
        "schema_version": "v1",
        "run_id": "test-run",
        "day_utc": "2026-05-22",
        "generated_at": "2026-05-22T22:00:00Z",
        "generator_name": "build_aegis_verified_runtime_graph_v1",
        "generator_version": "aegis_verified_runtime_graph.v1",
        "git_commit_hash": "UNKNOWN",
        "truth_root": "/tmp/aegis-truth",
        "input_artifact_hashes": {},
        "output_hash": "graph-hash",
        "graph_status": "READY",
        "runtime_readiness_status": "BLOCKED",
        "audit_blockers": [],
        "runtime_blockers": ["runtime_truth_kernel:TRADE_ADVICE_ALLOWED:BLOCKED_BY_KERNEL"],
        "portal_bindings": {"status": "READY", "surfaces": []},
        "readiness_linkage_to_runtime_truth_kernel": {
            "runtime_truth_classification": "PARTIAL_CONTEXT",
            "highest_readiness_layer": "BLOCKED",
            "runtime_truth_kernel_path": "/tmp/kernel.json",
            "runtime_evaluation_path": "/tmp/runtime_evaluation.json",
            "blocked_capabilities": ["TRADE_ADVICE_ALLOWED"],
            "allowed_capabilities": [],
        },
        "verified_capabilities": [],
        "evidence_links": [],
        "safety_invariants": {
            "broker_submit_transmit_allowed": False,
            "autonomous_execution_allowed": False,
            "human_approval_gates_removed": False,
        },
        "do_not_claim": ["Trade advice remains forbidden until kernel allows it."],
    }


def test_portal_runtime_model_contains_graph_derived_status_fields() -> None:
    model = build_portal_runtime_model_v1(graph=_minimal_graph())

    assert model["derivation_source"] == "verified_runtime_graph.v1.json"
    assert model["readiness_inference_policy"] == "NO_INDEPENDENT_PORTAL_READINESS_INFERENCE"
    assert model["graph_status"] == "READY"
    assert model["runtime_readiness_status"] == "BLOCKED"
    assert model["runtime_truth"]["runtime_truth_classification"] == "PARTIAL_CONTEXT"
    assert model["top_blockers"] == ["runtime_truth_kernel:TRADE_ADVICE_ALLOWED:BLOCKED_BY_KERNEL"]
    assert model["forbidden_actions"]
    assert model["do_not_claim"]


def test_verified_runtime_portal_route_and_api_are_registered() -> None:
    server = SERVER.read_text(encoding="utf-8")
    client = CLIENT.read_text(encoding="utf-8")
    nav = NAV.read_text(encoding="utf-8")
    routes = ROUTES.read_text(encoding="utf-8")

    assert '"/aegis-verified-runtime"' in server
    assert '"/api/aegis/verified-runtime/portal-model"' in server
    assert '"/api/aegis/verified-runtime/action"' in server
    assert "fetchAegisVerifiedRuntimePortalModel" in client
    assert "executeAegisVerifiedRuntimeAction" in client
    assert 'route: "/aegis-verified-runtime"' in nav
    assert 'path: "/aegis-verified-runtime"' in routes


def test_verified_runtime_page_reads_portal_model_not_kernel_readiness() -> None:
    pages = PAGES.read_text(encoding="utf-8")
    start = pages.index("async function renderAegisVerifiedRuntimePage")
    end = pages.index("async function renderAegisRuntimeTruthPage")
    page_source = pages[start:end]

    assert "fetchAegisVerifiedRuntimePortalModel" in page_source
    assert "fetchAegisRuntimeTruth" not in page_source
    assert "model.graph_status" in page_source
    assert "model.runtime_readiness_status" in page_source
    assert "model.portal_status" in page_source
    assert "NO_INDEPENDENT_PORTAL_READINESS_INFERENCE" in page_source
    assert "Graph READY means graph/evidence validation passed." in page_source
    assert "GRAPH_READY_RUNTIME_BLOCKED" in page_source
    assert "Graph verification" in page_source
    assert "ACTIVE_MODE_READINESS" in page_source
    assert "FULL_PLATFORM_READINESS" in page_source
    assert "DISABLED_BY_POLICY" in page_source
    assert "Active Mode READY means HUMAN_REVIEWED_PAPER_MODE can operate" in page_source
    assert "Full Platform BLOCKED means unrelated optional/full-platform subsystems are missing" in page_source


def test_verified_runtime_actions_use_existing_npm_interfaces_only() -> None:
    server = SERVER.read_text(encoding="utf-8")
    main = MAIN.read_text(encoding="utf-8")
    pages = PAGES.read_text(encoding="utf-8")

    for script in [
        "aegis:audit",
        "aegis:query",
        "aegis:graph-diff",
        "aegis:chatgpt:hydrate",
    ]:
        assert script in server
    assert "data-aegis-verified-runtime-action" in main
    assert "executeAegisVerifiedRuntimeAction" in main
    assert "broker_submit_transmit_allowed" in server
    assert "autonomous_execution_allowed" in server
    assert "trade_advice_allowed" in server
    assert "PORTAL_VERIFIED_RUNTIME_ACTION_ALLOWLIST_V1" in server
    assert '"run_audit"' in pages
    assert '"explain_blockers"' in pages
    assert '"claim_lookup"' in pages
    assert "requested_day" in main
    assert "operator_context" in main



def _ready_loader(_day: str) -> dict:
    return {"model_status": "READY"}


def _stale_loader(_day: str) -> dict:
    return {"model_status": "STALE"}


def test_unknown_verified_runtime_action_is_rejected_and_logged(tmp_path: Path) -> None:
    result = _run_verified_runtime_action_v1(
        {"action_id": "delete_everything", "requested_day": "2026-05-22"},
        command_runner=lambda *_args, **_kwargs: (_ for _ in ()).throw(AssertionError("runner must not be called")),
        portal_model_loader=_ready_loader,
        truth_root=tmp_path,
    )

    assert result["status"] == "REJECTED"
    assert result["exit_code"] == 2
    assert result["safety_policy_summary"]["broker_submit_transmit_allowed"] is False
    audit_path = _portal_action_audit_path_v1("2026-05-22", truth_root=tmp_path)
    row = json.loads(audit_path.read_text(encoding="utf-8").strip())
    assert row["action_id"] == "delete_everything"
    assert row["command_argv"] == []


def test_invalid_verified_runtime_action_args_are_rejected(tmp_path: Path) -> None:
    result = _run_verified_runtime_action_v1(
        {"action_id": "graph_diff", "requested_day": "2026-05-22", "args": {"to_day": "2026-05-22"}},
        command_runner=lambda *_args, **_kwargs: (_ for _ in ()).throw(AssertionError("runner must not be called")),
        portal_model_loader=_ready_loader,
        truth_root=tmp_path,
    )

    assert result["status"] == "REJECTED"
    assert "INVALID_GRAPH_DIFF_DAYS" in result["stderr_excerpt"]


def test_stale_portal_model_blocks_non_audit_actions(tmp_path: Path) -> None:
    result = _run_verified_runtime_action_v1(
        {"action_id": "explain_blockers", "requested_day": "2026-05-22"},
        command_runner=lambda *_args, **_kwargs: (_ for _ in ()).throw(AssertionError("runner must not be called")),
        portal_model_loader=_stale_loader,
        truth_root=tmp_path,
    )

    assert result["status"] == "BLOCKED"
    assert result["exit_code"] == 3
    assert "run_audit is the only enabled action" in result["stderr_excerpt"]


def test_verified_runtime_action_uses_argv_constrained_env_and_writes_log(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.setenv("TIINGO_API_KEY", "must-not-leak")
    captured: dict = {}

    def runner(argv, **kwargs):
        captured["argv"] = argv
        captured["kwargs"] = kwargs
        return SimpleNamespace(returncode=0, stdout="allowed=false", stderr="")

    result = _run_verified_runtime_action_v1(
        {"action_id": "claim_lookup", "requested_day": "2026-05-22", "args": {"claim": "trade advice allowed"}},
        command_runner=runner,
        portal_model_loader=_ready_loader,
        truth_root=tmp_path,
    )

    assert result["status"] == "PASS"
    assert isinstance(captured["argv"], list)
    assert captured["argv"] == ["npm", "run", "aegis:query", "--", "claim: trade advice allowed"]
    assert captured["kwargs"]["shell"] is False
    assert captured["kwargs"]["cwd"]
    assert captured["kwargs"]["timeout"] == 120
    assert captured["kwargs"]["env"]["TARGET_DAY"] == "2026-05-22"
    assert captured["kwargs"]["env"]["AEGIS_TRUTH_ROOT"] == str(tmp_path.resolve())
    assert "TIINGO_API_KEY" not in captured["kwargs"]["env"]
    audit_path = _portal_action_audit_path_v1("2026-05-22", truth_root=tmp_path)
    row = json.loads(audit_path.read_text(encoding="utf-8").strip())
    assert row["command_argv"] == captured["argv"]
    assert row["result_envelope"]["run_id"] == result["run_id"]



def test_repair_context_readiness_verified_action_uses_argv_and_safety_policy(tmp_path: Path) -> None:
    captured: dict = {}

    def runner(argv, **kwargs):
        captured["argv"] = argv
        captured["kwargs"] = kwargs
        return SimpleNamespace(returncode=0, stdout="context repaired", stderr="")

    result = _run_verified_runtime_action_v1(
        {"action_id": "repair_context_readiness", "requested_day": "2026-05-22"},
        command_runner=runner,
        portal_model_loader=_stale_loader,
        truth_root=tmp_path,
    )

    assert result["status"] == "PASS"
    assert captured["argv"] == ["npm", "run", "aegis:repair-context-readiness"]
    assert result["command_argv"] == captured["argv"]
    assert captured["kwargs"]["shell"] is False
    assert result["safety_policy_summary"]["trade_advice_allowed"] is False
    assert result["safety_policy_summary"]["broker_submit_transmit_allowed"] is False
    assert result["safety_policy_summary"]["autonomous_execution_allowed"] is False

def test_verified_runtime_action_timeout_returns_envelope_and_log(tmp_path: Path) -> None:
    def runner(argv, **kwargs):
        raise subprocess.TimeoutExpired(argv, kwargs["timeout"], output="partial out", stderr="partial err")

    result = _run_verified_runtime_action_v1(
        {"action_id": "explain_blockers", "requested_day": "2026-05-22"},
        command_runner=runner,
        portal_model_loader=_ready_loader,
        truth_root=tmp_path,
    )

    assert result["status"] == "TIMEOUT"
    assert result["exit_code"] == 124
    assert "partial out" in result["stdout_excerpt"]
    assert "partial err" in result["stderr_excerpt"]
    audit_path = _portal_action_audit_path_v1("2026-05-22", truth_root=tmp_path)
    assert "TIMEOUT" in audit_path.read_text(encoding="utf-8")


def test_verified_runtime_action_result_envelope_shape_and_safety_policy(tmp_path: Path) -> None:
    result = _run_verified_runtime_action_v1(
        {"action_id": "explain_blockers", "requested_day": "2026-05-22"},
        command_runner=lambda *_args, **_kwargs: SimpleNamespace(returncode=0, stdout="blockers", stderr=""),
        portal_model_loader=_ready_loader,
        truth_root=tmp_path,
    )

    for key in [
        "action_id",
        "run_id",
        "requested_day",
        "started_at",
        "completed_at",
        "exit_code",
        "status",
        "stdout_excerpt",
        "stderr_excerpt",
        "generated_artifact_paths",
        "safety_policy_summary",
    ]:
        assert key in result
    assert result["safety_policy_summary"]["trade_advice_allowed"] is False
    assert result["safety_policy_summary"]["autonomous_execution_allowed"] is False


def test_claim_lookup_trade_advice_remains_not_allowed(tmp_path: Path) -> None:
    result = _run_verified_runtime_action_v1(
        {"action_id": "claim_lookup", "requested_day": "2026-05-22", "args": {"claim": "trade advice allowed"}},
        command_runner=lambda *_args, **_kwargs: SimpleNamespace(returncode=0, stdout="- allowed: false", stderr=""),
        portal_model_loader=_ready_loader,
        truth_root=tmp_path,
    )

    assert result["status"] == "PASS"
    assert result["trade_advice_allowed"] is False
    assert result["safety_policy_summary"]["trade_advice_allowed"] is False
    assert "allowed: false" in result["stdout_excerpt"]


def test_verified_runtime_page_disables_non_audit_when_model_not_ready() -> None:
    pages = PAGES.read_text(encoding="utf-8")
    start = pages.index("async function renderAegisVerifiedRuntimePage")
    end = pages.index("async function renderAegisRuntimeTruthPage")
    page_source = pages[start:end]

    assert 'const modelReady = envelope.model_status === "READY"' in page_source
    assert 'renderVerifiedRuntimeActionButton("run_audit"' in page_source
    assert 'disabled: !modelReady' in page_source
    assert "Policy Claims" in page_source
    assert "trade advice allowed" in page_source
    assert "broker submit/transmit" in page_source



def test_recent_action_history_reads_append_only_jsonl(tmp_path: Path) -> None:
    _run_verified_runtime_action_v1(
        {"action_id": "explain_blockers", "requested_day": "2026-05-22"},
        command_runner=lambda *_args, **_kwargs: SimpleNamespace(returncode=0, stdout="blockers", stderr=""),
        portal_model_loader=_ready_loader,
        truth_root=tmp_path,
    )

    history = _load_verified_runtime_recent_actions_v1("2026-05-22", truth_root=tmp_path)

    assert history["status"] == "CURRENT"
    assert history["path"].endswith("portal_actions.v1.jsonl")
    assert history["rows"][0]["action_id"] == "explain_blockers"
    assert history["rows"][0]["result_envelope"]["status"] == "PASS"


def test_verified_runtime_page_has_operator_usability_controls_without_readiness_inference() -> None:
    pages = PAGES.read_text(encoding="utf-8")
    main = MAIN.read_text(encoding="utf-8")
    start = pages.index("async function renderAegisVerifiedRuntimePage")
    end = pages.index("async function renderAegisRuntimeTruthPage")
    page_source = pages[start:end]

    assert "What Should I Do Next?" in page_source
    assert "recovery_plan" in page_source
    assert "Copy hydrate packet" in page_source
    assert "Copy blocker summary" in page_source
    assert "Copy latest action result" in page_source
    assert "Freshness And Verification" in page_source
    assert "Recent Portal Actions" in page_source
    assert "Run Graph Diff" in page_source
    assert "Policy Claim Presets" in page_source
    for claim in [
        "trade advice allowed",
        "manual trade capture allowed",
        "broker submit transmit allowed",
        "autonomous execution allowed",
        "portal state current",
    ]:
        assert claim in page_source
    assert "verifiedRuntimeActionValue" in main
    assert "from-day-source" in page_source
    assert "to-day-source" in page_source
    assert "fetchAegisRuntimeTruth" not in page_source
