from __future__ import annotations

from pathlib import Path
from constellation_2.phaseL.ui.tests.operator_shell_test_sources import pages_source_v1

from ops.aegis.research_lab import research_lab_routes as routes


ROOT = Path(__file__).resolve().parents[4]
PAGES = ROOT / "constellation_2/phaseL/ui/static/operator_shell/pages/index.js"
DOMAIN = ROOT / "constellation_2/phaseL/ui/static/operator_shell/domain_client/index.js"
SERVER = ROOT / "constellation_2/phaseL/ui/server/run_ops_dashboard_v1.py"
MAIN = ROOT / "constellation_2/phaseL/ui/static/operator_shell/main.js"


def test_research_os_status_panel_is_read_only_and_visible() -> None:
    pages = pages_source_v1(ROOT)
    domain = DOMAIN.read_text(encoding="utf-8")
    server = SERVER.read_text(encoding="utf-8")

    assert "Research OS Status" in pages
    assert "fetchResearchLabStatus" in pages
    assert 'query("/api/research-lab/status/latest", params)' in domain
    assert 'path == "/api/research-lab/status"' in server
    assert 'path == "/api/research-lab/status-reports"' in server
    assert 'path == "/api/research-lab/paper-trial-proposals"' in server
    assert 'path == "/api/research-lab/paper-trial-proposals/latest"' in server
    assert "Overall status" in pages
    assert "Readiness" in pages
    assert "Integrity report" in pages
    assert "Duplicate registry issues" in pages
    assert "Legacy lineage warnings" in pages
    assert "Latest decision" in pages
    assert "No promote, open trade, allocate, repair automatically, or paper-trial creation actions" in pages
    assert "Paper Trial Proposals" in pages
    assert "Observation Candidates" in pages
    assert "Observation Clusters" in pages
    assert "Hypothesis Proposals" in pages
    assert "Hypothesis Proposal Reviews" in pages
    assert "Research Hypothesis Intake" in pages
    assert "not trading signals" in pages
    assert "not hypotheses or trading signals" in pages
    assert "not active hypotheses or trading signals" in pages
    assert "not strategies or trading signals" in pages
    assert "fetchResearchLabObservationCandidates" in pages
    assert "fetchResearchLabObservationClusters" in pages
    assert "fetchResearchLabHypothesisProposals" in pages
    assert "fetchResearchLabHypothesisProposalReviews" in pages
    assert "fetchResearchLabHypothesisIntake" in pages
    assert "Proposal blocked" in pages
    assert "Proposal needs review" in pages
    assert "Proposal eligible" in pages
    assert "paper_trial_created" in pages
    assert 'path == "/api/research-lab/observation-candidates"' in server
    assert 'path == "/api/research-lab/observation-candidate-batches/latest"' in server
    assert 'path == "/api/research-lab/observation-clusters"' in server
    assert 'path == "/api/research-lab/observation-cluster-batches/latest"' in server
    assert 'path == "/api/research-lab/hypothesis-proposals"' in server
    assert 'path == "/api/research-lab/hypothesis-proposal-batches/latest"' in server
    assert 'path == "/api/research-lab/hypothesis-proposal-reviews"' in server
    assert 'path == "/api/research-lab/hypothesis-proposal-review-batches/latest"' in server
    assert 'path == "/api/research-lab/hypothesis-intake-decisions"' in server
    assert 'path == "/api/research-lab/hypothesis-intake-batches/latest"' in server
    assert 'path == "/api/research-lab/research-hypotheses"' in server
    assert "fetchResearchLabPaperTrialProposal" in pages


def test_research_os_status_panel_does_not_add_mutating_actions() -> None:
    pages = pages_source_v1(ROOT)
    panel_block = pages.split("function renderResearchOSStatusPanel", 1)[1].split("async function renderResearchLabPage", 1)[0]

    for forbidden in [
        "postJson",
        "patchJson",
        "promote_challenger",
        "allocate_capital",
        "execute_trade",
        "create paper trial",
        "create hypothesis",
        "open paper trial",
        "repair automatically</button>",
    ]:
        assert forbidden not in panel_block


def test_packet_23_to_29_routes_return_safe_empty_payloads_when_artifacts_missing(tmp_path: Path) -> None:
    store_root = tmp_path / "research_store"

    payloads = [
        routes.research_lab_status_latest_v1(store_root=store_root),
        routes.research_lab_paper_trial_proposal_latest_v1(store_root=store_root),
        routes.research_lab_observation_candidate_batch_latest_v1(store_root=store_root),
        routes.research_lab_observation_cluster_batch_latest_v1(store_root=store_root),
        routes.research_lab_hypothesis_proposal_batch_latest_v1(store_root=store_root),
        routes.research_lab_hypothesis_proposal_review_batch_latest_v1(store_root=store_root),
        routes.research_lab_hypothesis_intake_batch_latest_v1(store_root=store_root),
        routes.research_lab_hypothesis_intake_decision_latest_v1(store_root=store_root),
        routes.research_lab_research_hypothesis_latest_v1(store_root=store_root),
    ]

    for payload in payloads:
        assert payload["ok"] is False
        assert payload["read_only"] is True
        assert "error" in payload


def test_packet_23_to_29_routes_do_not_raise_on_malformed_registries(tmp_path: Path) -> None:
    store_root = tmp_path / "research_store"
    registry_dir = store_root / "registries"
    registry_dir.mkdir(parents=True)
    for filename in [
        "research_os_status_report_registry.json",
        "paper_trial_proposal_registry.json",
        "observation_candidate_batch_registry.json",
        "observation_cluster_batch_registry.json",
        "hypothesis_proposal_batch_registry.json",
        "hypothesis_proposal_review_batch_registry.json",
        "hypothesis_intake_batch_registry.json",
        "hypothesis_intake_decision_registry.json",
        "research_hypothesis_registry.json",
    ]:
        (registry_dir / filename).write_text("{not-json}\n", encoding="utf-8")

    payloads = [
        routes.research_lab_status_latest_v1(store_root=store_root),
        routes.research_lab_paper_trial_proposal_latest_v1(store_root=store_root),
        routes.research_lab_observation_candidate_batch_latest_v1(store_root=store_root),
        routes.research_lab_observation_cluster_batch_latest_v1(store_root=store_root),
        routes.research_lab_hypothesis_proposal_batch_latest_v1(store_root=store_root),
        routes.research_lab_hypothesis_proposal_review_batch_latest_v1(store_root=store_root),
        routes.research_lab_hypothesis_intake_batch_latest_v1(store_root=store_root),
        routes.research_lab_hypothesis_intake_decision_latest_v1(store_root=store_root),
        routes.research_lab_research_hypothesis_latest_v1(store_root=store_root),
    ]

    for payload in payloads:
        assert payload["ok"] is False
        assert payload["read_only"] is True
        assert payload["error_type"] == "JSONDecodeError"


def test_dashboard_shell_has_implemented_packet29_hooks() -> None:
    pages = pages_source_v1(ROOT)
    domain = DOMAIN.read_text(encoding="utf-8")
    server = SERVER.read_text(encoding="utf-8")

    assert "fetchResearchLabHypothesisIntake" in pages
    assert 'query("/api/research-lab/hypothesis-intake-batches/latest", params)' in domain
    assert "/api/research-lab/hypothesis-intake-decisions" in server
    assert "/api/research-lab/hypothesis-intake-batches" in server
    assert "/api/research-lab/research-hypotheses" in server
    assert "research_lab_hypothesis_intake_batch_latest_v1" in server
    assert "research_lab_research_hypothesis_latest_v1" in server
    panel_block = pages.split("function renderResearchHypothesisIntakePanel", 1)[1].split("async function renderResearchLabPage", 1)[0].lower()
    assert "activate hypothesis</button>" not in panel_block
    assert "create challenger</button>" not in panel_block
    assert "create sleeve</button>" not in panel_block
    assert "open paper trial</button>" not in panel_block
    assert "allocate</button>" not in panel_block
    assert "promote</button>" not in panel_block


def test_dashboard_server_imports_with_packet_23_to_29_panels_enabled() -> None:
    import constellation_2.phaseL.ui.server.run_ops_dashboard_v1 as dashboard

    assert hasattr(dashboard, "OpsHandler")
    server = SERVER.read_text(encoding="utf-8")
    assert "research_lab_route_unavailable" in server
    assert "Hypothesis Proposal Reviews" in pages_source_v1(ROOT)
    assert "Research Hypothesis Intake" in pages_source_v1(ROOT)


def test_topbar_runtime_mode_hydrates_from_runtime_status_fallback() -> None:
    main = MAIN.read_text(encoding="utf-8")

    assert "fetchRuntimeStatus" in main
    assert "state.shell.runtimeStatus = runtimeStatus || {}" in main
    assert "runtimeStatus.runtime_mode" in main
