from __future__ import annotations

import hashlib
import json
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
    navigation = (ROOT / "constellation_2/phaseL/ui/static/operator_shell/navigation_schema.js").read_text(encoding="utf-8")
    server = (ROOT / "constellation_2/phaseL/ui/server/run_ops_dashboard_v1.py").read_text(encoding="utf-8")
    domain_client = (
        ROOT / "constellation_2/phaseL/ui/static/operator_shell/domain_client/index.js"
    ).read_text(encoding="utf-8")

    assert 'id: "command_configuration"' in navigation
    assert 'label: "Configuration"' in navigation
    assert 'route: "/configuration"' in navigation
    assert 'truthOwner: "configuration_activation_authority_v1"' in navigation
    assert 'path: "/configuration"' in pages
    assert 'case "configuration":' in pages
    assert "renderConfigurationPage" in pages
    assert "executeConfigurationWorkflow" in pages
    assert 'name="${escapeHtml(key)}"' in pages
    assert 'name="operator_parameters_text"' in pages
    assert "Operator Configuration Parameters" in pages
    assert "Editable Catalog Values" in pages

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
        "scenario",
        "include_inheritance",
        "horizon_months",
        "start_month",
        "discovery_headroom_multiplier_bp",
        "max_sleeve_headroom_cents",
        "liquidity_max_notional_per_symbol_usd",
        "risk_trend_eq_primary_per_trade_notional_pct_max",
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
    assert "CONFIGURATION_ENUM_INVALID:scenario" in validated["validation"]["reason_codes"]
    assert "CONFIGURATION_RANGE_LOW:horizon_months" in validated["validation"]["reason_codes"]
    assert validated["draft"]["status"] == "DRAFT"


def test_valid_draft_passes_validation(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    _configure_runtime_roots(monkeypatch, tmp_path)
    created = create_configuration_draft_v1(_valid_payload())
    validated = validate_configuration_draft_v1(created["draft"]["draft_id"])
    assert validated["validation"]["status"] == "PASS"
    assert validated["draft"]["status"] == "VALIDATED"
    assert validated["validation"]["normalized_values"]["discovery_headroom_multiplier_bp"] == 10000
    assert validated["validation"]["normalized_values"]["max_sleeve_headroom_cents"] == 100000
    assert "parameter_refs_used" in validated["validation"]


def test_operator_configuration_parameters_are_captured_in_draft_source_values(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    _configure_runtime_roots(monkeypatch, tmp_path)
    created = create_configuration_draft_v1(
        {
            **_valid_payload(),
            "discovery_headroom_multiplier_bp": 8500,
            "max_sleeve_headroom_cents": 2500000,
            "operator_parameters": [
                {
                    "parameter_name": "headroom.daily_cap",
                    "value_kind": "money",
                    "value_text": "$25000",
                    "notes": "operator captured",
                },
                {
                    "parameter_name": "aegis.threshold",
                    "value_kind": "number",
                    "value_text": "42",
                },
            ],
        }
    )
    validated = validate_configuration_draft_v1(created["draft"]["draft_id"])
    assert validated["validation"]["status"] == "PASS"
    assert validated["validation"]["normalized_values"]["discovery_headroom_multiplier_bp"] == 8500
    assert validated["validation"]["normalized_values"]["max_sleeve_headroom_cents"] == 2500000
    assert validated["draft"]["advisory_unmapped_parameters"] == [
        {
            "parameter_name": "aegis.threshold",
            "value_kind": "number",
            "value_text": "42",
            "notes": "",
        },
        {
            "parameter_name": "headroom.daily_cap",
            "value_kind": "money",
            "value_text": "$25000",
            "notes": "operator captured",
        },
    ]


def test_invalid_operator_configuration_parameter_blocks_validation(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    _configure_runtime_roots(monkeypatch, tmp_path)
    with pytest.raises(ConfigurationWorkflowApiError) as exc:
        create_configuration_draft_v1(
            {
                **_valid_payload(),
                "operator_parameters": [
                    {"parameter_name": "aegis threshold", "value_kind": "number", "value_text": "42"},
                    {"parameter_name": "aegis.threshold", "value_kind": "unknown", "value_text": "42"},
                ],
            }
        )
    assert "OPERATOR_PARAMETER_NAME_INVALID" in exc.value.reason_codes
    assert "OPERATOR_PARAMETER_VALUE_KIND_INVALID" in exc.value.reason_codes


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
    assert Path(str(activation["active_configuration_path"])).exists()
    assert Path(str(activation["active_configuration_current_path"])).exists()
    assert activation["config_version"]
    assert activation["materialized_policy_refs"]
    assert _sha256(registry_path) == before_registry_sha


def test_uncataloged_parameter_cannot_activate(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    _configure_runtime_roots(monkeypatch, tmp_path)
    created = create_configuration_draft_v1({**_valid_payload(), "production_drawdown_block_limit": "-0.500000"})
    validated = validate_configuration_draft_v1(created["draft"]["draft_id"])
    assert validated["validation"]["status"] == "FAIL"
    assert (
        "CONFIGURATION_PARAMETER_NOT_CATALOGED:production_drawdown_block_limit"
        in validated["validation"]["reason_codes"]
    )
    with pytest.raises(ConfigurationWorkflowApiError) as exc:
        review_configuration_draft_v1(created["draft"]["draft_id"])
    assert "CONFIGURATION_REVIEW_REQUIRES_VALIDATION_PASS" in exc.value.reason_codes


def test_only_activated_config_materializes_policy_and_preserves_production_limits(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    _configure_runtime_roots(monkeypatch, tmp_path)
    truth_root = (tmp_path / "truth").resolve()
    created = create_configuration_draft_v1(
        {
            **_valid_payload(),
            "liquidity_max_notional_per_symbol_usd": "100",
            "paper_bootstrap_max_trade_risk_pct_nav": "0.002000",
        }
    )
    draft_id = created["draft"]["draft_id"]
    validate_configuration_draft_v1(draft_id)
    review_configuration_draft_v1(draft_id)
    assert not (truth_root / "active_configuration_v1").exists()
    assert not (truth_root / "materialized_policy_artifacts_v1").exists()

    activated = activate_configuration_draft_v1(draft_id)
    activation = activated["activation"]
    active_config = json.loads(Path(str(activation["active_configuration_path"])).read_text(encoding="utf-8"))
    assert active_config["config_version"] == activation["config_version"]

    materialized = {
        row["target_policy_artifact"]: json.loads(Path(str(row["path"])).read_text(encoding="utf-8"))
        for row in activation["materialized_policy_refs"]
    }
    liquidity = materialized["C2_LIQUIDITY_SLIPPAGE_POLICY_V1"]
    assert liquidity["config_version_used"] == activation["config_version"]
    assert liquidity["effective_policy"]["defaults"]["max_notional_per_symbol_usd"] == "100"
    assert {
        row["parameter_key"] for row in liquidity["parameter_refs_used"]
    } >= {"liquidity_max_notional_per_symbol_usd"}

    bundle_c = materialized["C2_BUNDLE_C_DRAWDOWN_POLICY_V1"]
    assert bundle_c["effective_policy"]["profiles"]["PAPER_BOOTSTRAP"]["max_trade_risk_pct_nav"] == "0.002000"
    assert bundle_c["effective_policy"]["profiles"]["PRODUCTION"]["max_trade_risk_pct_nav"] == "0.010000"


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
