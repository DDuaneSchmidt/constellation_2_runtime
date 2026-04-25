from __future__ import annotations

import hashlib
from pathlib import Path

import pytest

from constellation_2.phaseL.ui_api.configuration_workflow_v1 import (
    ConfigurationWorkflowApiError,
    activate_configuration_draft_v1,
    build_configuration_catalog_v1,
    create_configuration_draft_v1,
    get_configuration_draft_v1,
    reject_configuration_draft_v1,
    review_configuration_draft_v1,
    validate_configuration_draft_v1,
)


ROOT = Path(__file__).resolve().parents[4]


def _sha256(path: Path) -> str:
    h = hashlib.sha256()
    h.update(path.read_bytes())
    return h.hexdigest()


def _configure_runtime_roots(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    monkeypatch.setenv("C2_UI_CONFIGURATION_WORKFLOW_ROOT", str((tmp_path / "runtime").resolve()))
    monkeypatch.setenv("C2_UI_CONFIGURATION_TRUTH_ROOT", str((tmp_path / "truth").resolve()))


def _valid_payload() -> dict[str, object]:
    return {
        "scenario": "florida",
        "include_inheritance": False,
        "horizon_months": 24,
        "start_month": "2026-05",
    }


def test_configuration_route_and_endpoints_are_wired() -> None:
    pages = (ROOT / "constellation_2/phaseL/ui/static/operator_shell/pages/index.js").read_text(encoding="utf-8")
    server = (ROOT / "constellation_2/phaseL/ui/server/run_ops_dashboard_v1.py").read_text(encoding="utf-8")
    domain_client = (
        ROOT / "constellation_2/phaseL/ui/static/operator_shell/domain_client/index.js"
    ).read_text(encoding="utf-8")

    assert 'path: "/configuration"' in pages
    assert 'case "configuration":' in pages
    assert "renderConfigurationPage" in pages
    assert "executeConfigurationWorkflow" in pages
    assert 'name="horizon_months"' in pages
    assert 'name="start_month"' in pages

    for endpoint in [
        '"/api/configuration/catalog"',
        '"/api/configuration/current"',
        '"/api/configuration/drafts"',
        'action == "validate"',
        'action == "review"',
        'action == "activate"',
        'action == "reject"',
    ]:
        assert endpoint in server

    for client_call in [
        'query("/api/configuration/catalog")',
        'query("/api/configuration/current")',
        'postJson("/api/configuration/drafts"',
        "/validate`",
        "/review`",
        "/activate`",
        "/reject`",
    ]:
        assert client_call in domain_client


def test_catalog_contains_capital_cashflow_fields_and_locked_markers() -> None:
    payload = build_configuration_catalog_v1()
    editable = {row["parameter_name"]: row for row in payload["editable_fields"]}
    locked = {row["parameter_name"]: row for row in payload["locked_fields"]}

    for field in [
        "capital_cashflow.scenario",
        "capital_cashflow.include_inheritance",
        "capital_cashflow.horizon_months",
        "capital_cashflow.start_month",
    ]:
        assert field in editable
        assert editable[field]["state"] == "editable"

    assert locked["kill_switch.state"]["lock_class"] == "LOCKED_BY_DESIGN"
    assert locked["broker.transmit_arming"]["lock_class"] == "LOCKED_BY_DESIGN"
    assert locked["submission_authorization_status"]["lock_class"] == "DERIVED_READONLY"
    assert locked["trade_submit_readiness.attestation_outputs"]["lock_class"] == "DERIVED_READONLY"


def test_draft_creation_and_readback_work(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    _configure_runtime_roots(monkeypatch, tmp_path)
    created = create_configuration_draft_v1(_valid_payload())
    draft_id = created["draft"]["draft_id"]
    loaded = get_configuration_draft_v1(draft_id)
    assert loaded["draft"]["draft_id"] == draft_id
    assert loaded["draft"]["status"] == "DRAFT"


def test_invalid_draft_fails_validation(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    _configure_runtime_roots(monkeypatch, tmp_path)
    created = create_configuration_draft_v1(
        {
            "scenario": "bad_scenario",
            "include_inheritance": "false",
            "horizon_months": 0,
            "start_month": "2026/05",
        }
    )
    validated = validate_configuration_draft_v1(created["draft"]["draft_id"])
    assert validated["validation"]["status"] == "FAIL"
    assert "CAPITAL_CASHFLOW_SCENARIO_INVALID" in validated["validation"]["reason_codes"]
    assert "CAPITAL_CASHFLOW_HORIZON_MONTHS_OUT_OF_RANGE" in validated["validation"]["reason_codes"]
    assert validated["draft"]["status"] == "DRAFT"


def test_valid_draft_passes_validation(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    _configure_runtime_roots(monkeypatch, tmp_path)
    created = create_configuration_draft_v1(_valid_payload())
    validated = validate_configuration_draft_v1(created["draft"]["draft_id"])
    assert validated["validation"]["status"] == "PASS"
    assert validated["draft"]["status"] == "VALIDATED"


def test_review_requires_validation(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    _configure_runtime_roots(monkeypatch, tmp_path)
    created = create_configuration_draft_v1(_valid_payload())
    with pytest.raises(ConfigurationWorkflowApiError) as exc:
        review_configuration_draft_v1(created["draft"]["draft_id"])
    assert "CONFIGURATION_REVIEW_REQUIRES_VALIDATION_PASS" in exc.value.reason_codes


def test_activation_requires_review(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    _configure_runtime_roots(monkeypatch, tmp_path)
    created = create_configuration_draft_v1(_valid_payload())
    validate_configuration_draft_v1(created["draft"]["draft_id"])
    with pytest.raises(ConfigurationWorkflowApiError) as exc:
        activate_configuration_draft_v1(created["draft"]["draft_id"])
    assert "CONFIGURATION_ACTIVATE_REQUIRES_REVIEWED_DRAFT" in exc.value.reason_codes


def test_activation_writes_audit_evidence_and_preserves_unrelated_registry(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    _configure_runtime_roots(monkeypatch, tmp_path)
    registry_path = ROOT / "governance/02_REGISTRIES/C2_ARTIFACT_AUTHORITY_REGISTRY_V1.json"
    before_registry_sha = _sha256(registry_path)

    created = create_configuration_draft_v1(_valid_payload())
    draft_id = created["draft"]["draft_id"]
    validate_configuration_draft_v1(draft_id)
    review_configuration_draft_v1(draft_id)
    activated = activate_configuration_draft_v1(draft_id)

    activation = activated["activation"]
    audit_path = Path(str(activation["audit_evidence_path"])).resolve()
    assert activated["draft"]["status"] == "ACTIVATED"
    assert audit_path.exists()
    assert audit_path.is_file()
    assert Path(str(activation["configuration_state_path"])).exists()
    assert _sha256(registry_path) == before_registry_sha


def test_reject_endpoint_sets_rejected_state(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    _configure_runtime_roots(monkeypatch, tmp_path)
    created = create_configuration_draft_v1(_valid_payload())
    draft_id = created["draft"]["draft_id"]
    rejected = reject_configuration_draft_v1(draft_id, {"reason": "test_reject"})
    assert rejected["draft"]["status"] == "REJECTED"
    assert rejected["rejection"]["reason"] == "test_reject"


def test_locked_fields_are_not_exposed_as_editable_inputs() -> None:
    pages = (ROOT / "constellation_2/phaseL/ui/static/operator_shell/pages/index.js").read_text(encoding="utf-8")
    assert 'name="kill_switch.state"' not in pages
    assert 'name="broker.transmit_arming"' not in pages
    assert "Locked-By-Design Safety Fields" in pages


def test_existing_operational_routes_still_present_in_server() -> None:
    server = (ROOT / "constellation_2/phaseL/ui/server/run_ops_dashboard_v1.py").read_text(encoding="utf-8")
    for endpoint in ['"/api/operations"', '"/api/orders"', '"/api/positions"', '"/api/reconciliation"']:
        assert endpoint in server
