from __future__ import annotations

import json
from pathlib import Path

import pytest

from constellation_2.common.constitutional_runtime_v1 import validate_governed_artifact_payload_v1
from constellation_2.common.session_authority_v1 import (
    derive_active_session_payload_v1,
    derive_target_day_admission_payload_v1,
    derive_target_day_build_payload_v1,
    write_active_session_v1,
    write_target_day_admission_v1,
    write_target_day_build_v1,
)
from constellation_2.common.subsystem_authority_v1 import (
    RC_ACCOUNT_TRADING_POLICY_AUTHORITY_AMBIGUOUS,
    RC_EXECUTION_IDENTITY_CLIENT_ID_ORDERS_MISMATCH,
    RC_EXECUTION_ROOT_PATH_MISMATCH,
    RC_EXECUTION_ROOT_SLEEVE_ID_MISSING,
    STATE_AMBIGUOUS_AUTHORITY,
    STATE_BLOCKED,
    STATE_CLEAR,
    STATE_READY,
    STATE_RESOLVED,
    SUBSYSTEM_ID_EXECUTION_AUTHORITY,
    build_account_trading_policy_dossier_v1,
    build_execution_dossier_v1,
    build_execution_identity_dossier_v1,
    build_execution_profile_dossier_v1,
    build_operator_summary_dossier_v1,
    build_subsystem_authority_payloads_v1,
)


DAY = "2026-04-14"
SOURCE_ROOT = Path(__file__).resolve().parents[3]


def _write_text(path: Path, content: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(content, encoding="utf-8")


def _write_json(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def _seed_minimal_repo(tmp_path: Path) -> Path:
    repo_root = tmp_path / "repo"
    _write_text(
        repo_root / "governance/05_CONTRACTS/C2/execution_root_authority_v1.contract.md",
        "canonical owner: sleeve_execution_root_v1\ncanonical root: truth/sleeves/<sleeve_id>/<mode>/...\n",
    )
    _write_text(
        repo_root / "governance/05_CONTRACTS/C2/truth_partitioning_by_sleeve_v1.contract.md",
        "All sleeve writes MUST be scoped to truth_sleeves/<sleeve_id>/<mode>/...",
    )
    _write_text(
        repo_root / "governance/05_CONTRACTS/C2/execution_identity_binding_v1.contract.md",
        "Execution identity is canonical only from sleeve_id + environment + account_id + client_id_orders.",
    )
    _write_text(
        repo_root / "governance/05_CONTRACTS/C2/execution_profile_authority_v1.contract.md",
        "PRIMARY/PAPER profile binds DUO847203 client_id_orders=7 client_id_observer=179 host=127.0.0.1 port=4002",
    )
    _write_text(
        repo_root / "governance/05_CONTRACTS/C2/multi_account_topology_v1.contract.md",
        "Truth partition: constellation_2/runtime/truth_sleeves/<sleeve_id>/<mode>/...",
    )
    _write_text(
        repo_root / "governance/03_CONTRACTS/C2_TRUE_EVIDENCE_SPINE_V2.md",
        "The authoritative output surface for active execution evidence is truth_sleeves/<sleeve_id>/<mode>/execution_evidence_v1/",
    )
    _write_text(
        repo_root / "governance/05_CONTRACTS/C2/trade_submit_readiness_c2_v1.contract.md",
        "This contract introduces a C2-native readiness spine written ONLY under truth_sleeves/<sleeve_id>/<mode>/trade_submit_readiness_c2_v1/status.json",
    )
    _write_text(
        repo_root / "governance/05_CONTRACTS/C2/startup_materialization_v1.contract.md",
        "requires same-day phase-C preflight under truth_sleeves/<sleeve_id>/<mode>/phaseC_preflight_v1/<DAY>/",
    )
    _write_text(
        repo_root / "constellation_2/common/sleeve_execution_root_v1.py",
        "EXECUTION_ROOT_AUTHORITY_OWNER = 'sleeve_execution_root_v1'\n",
    )
    _write_text(
        repo_root / "constellation_2/phaseC/tools/run_phaseC_preflight_day_v2.py",
        "documents/writes truth_sleeves/<sleeve_id>/<mode>/phaseC_preflight_v1/<day>/",
    )
    _write_text(
        repo_root / "constellation_2/common/paper_execution_authority_v1.py",
        "resolve_governed_paper_execution_roots = sleeve_execution_root_v1\n",
    )
    _write_text(
        repo_root / "governance/05_CONTRACTS/C2/sleeve_registry_v1.contract.md",
        "sleeve registry is authoritative for IB gateway connection profile binding",
    )
    _write_text(
        repo_root / "governance/05_CONTRACTS/C2/paper_day_readiness_runbook_v1.contract.md",
        "--ib_host 127.0.0.1\n--ib_port 4002\n--ib_client_id 7\n",
    )
    _write_text(
        repo_root / "ops/tools/run_c2_paper_day_orchestrator_v2.py",
        'str(env.get("C2_IB_HOST") or "127.0.0.1").strip()\n'
        'str(env.get("C2_IB_PORT") or "4002").strip()\n'
        'str(env.get("C2_IB_CLIENT_ID") or "7").strip()\n',
    )
    _write_text(
        repo_root / "governance/05_CONTRACTS/C2/trading_symbol_policy_authority_v1.contract.md",
        "Trading symbol policy is authoritative only from:\nENGINE_MODEL_REGISTRY_V1.json\n",
    )
    _write_text(
        repo_root / "governance/05_CONTRACTS/C2/ib_account_registry_v1.contract.md",
        "Phase D submission boundary MUST NOT use IB account registry `allowed_symbols` as the trading-symbol authority.",
    )
    _write_text(
        repo_root / "constellation_2/phaseD/lib/submit_boundary_paper_v4.py",
        "_enforce_engine_symbol_policy(\n_read_engine_model_registry(\n",
    )
    _write_json(
        repo_root / "governance/02_REGISTRIES/C2_ARTIFACT_AUTHORITY_REGISTRY_V1.json",
        json.loads(
            (
                SOURCE_ROOT / "governance/02_REGISTRIES/C2_ARTIFACT_AUTHORITY_REGISTRY_V1.json"
            ).read_text(encoding="utf-8")
        ),
    )
    _write_json(
        repo_root / "governance/04_DATA/SCHEMAS/C2/RUNTIME/constitutional_artifact_authority_registry.v1.schema.json",
        json.loads(
            (
                SOURCE_ROOT
                / "governance/04_DATA/SCHEMAS/C2/RUNTIME/constitutional_artifact_authority_registry.v1.schema.json"
            ).read_text(encoding="utf-8")
        ),
    )
    _write_json(
        repo_root / "governance/02_REGISTRIES/C2_SLEEVE_REGISTRY_V1.json",
        {
            "schema_id": "c2_sleeve_registry",
            "schema_version": "v1",
            "sleeves": [
                {
                    "sleeve_id": "PRIMARY",
                    "enabled": True,
                    "mode": "PAPER",
                    "ib_account": "DUO847203",
                    "truth_partition": "truth_sleeves/PRIMARY/PAPER",
                    "ib_gateway_profile": {
                        "host": "127.0.0.1",
                        "port": 4002,
                        "client_id_orders": 7,
                        "client_id_observer": 179,
                    },
                }
            ],
        },
    )
    _write_json(
        repo_root / "governance/02_REGISTRIES/C2_IB_ACCOUNT_REGISTRY_V1.json",
        {
            "schema_id": "c2_ib_account_registry",
            "schema_version": "v1",
            "accounts": [
                {
                    "account_id": "DUO847203",
                    "environment": "PAPER",
                    "enabled_for_submission": True,
                    "allowed_engine_ids": ["C2_MEAN_REVERSION_EQ_V1"],
                    "allowed_sleeve_ids": ["PRIMARY"],
                    "allowed_symbols": None,
                    "notes": [],
                }
            ],
        },
    )
    _write_json(
        repo_root / "governance/02_REGISTRIES/ENGINE_MODEL_REGISTRY_V1.json",
        {
            "schema_id": "engine_model_registry",
            "schema_version": "v1",
            "engines": [
                {
                    "engine_id": "C2_MEAN_REVERSION_EQ_V1",
                    "allowed_symbols": ["SPY"],
                }
            ],
        },
    )
    return repo_root


def _seed_minimal_truth(tmp_path: Path) -> Path:
    truth_root = tmp_path / "truth"
    execution_truth_root = tmp_path / "truth_sleeves" / "PRIMARY" / "PAPER"
    (execution_truth_root / "run_pointer_v2").mkdir(parents=True, exist_ok=True)
    _write_json(
        execution_truth_root / "run_pointer_v2" / "canonical_authority_head.v1.json",
        {
            "schema_id": "canonical_authority_head",
            "day_utc": DAY,
            "points_to": str(
                (
                    execution_truth_root
                    / "reports"
                    / "authorization_gate_verdict_v1"
                    / DAY
                    / "authorization_gate_verdict.v1.json"
                ).resolve()
            ),
        },
    )
    _write_json(
        execution_truth_root / "reports" / "authorization_gate_verdict_v1" / DAY / "authorization_gate_verdict.v1.json",
        {"schema_id": "authorization_gate_verdict_v1", "schema_version": "v1", "day_utc": DAY, "status": "PASS"},
    )
    build_ref = write_target_day_build_v1(
        truth_root=truth_root,
        payload=derive_target_day_build_payload_v1(
            truth_root=truth_root,
            target_day=DAY,
            artifact_results=[
                {
                    "artifact_id": "paper_policy_verdict_v1",
                    "artifact_name": "paper_policy_verdict_v1",
                    "required": True,
                    "role_class": "REQUIRED_DERIVED_GATE",
                    "classification": "REQUIRED_ARTIFACT_BLOCKER",
                    "canonical_path": str((truth_root / "reports" / "paper_policy_verdict_v1" / DAY / "paper_policy_verdict.v1.json").resolve()),
                    "path_family": "canonical_truth",
                    "observed_status": "PRESENT",
                    "result_status": "PASS",
                    "schema_status": "VALID",
                    "schema_ref": "",
                    "freshness_status": "CURRENT",
                    "target_day_expected": DAY,
                    "target_day_observed": DAY,
                    "date_binding_status": "MATCH",
                    "provenance_required": False,
                    "provenance_summary": {"required": False, "present": True, "fields_present": [], "source": ""},
                    "closure_status": "CLOSED",
                    "producer": {"module": "pytest", "git_sha": "test"},
                    "source_refs": [],
                    "observed_dependency_artifacts": [],
                }
            ],
            source_refs=[],
        ),
    )
    admission_ref = write_target_day_admission_v1(
        truth_root=truth_root,
        payload=derive_target_day_admission_payload_v1(
            truth_root=truth_root,
            target_day=DAY,
            build_ref=build_ref,
        ),
    )
    write_active_session_v1(
        truth_root=truth_root,
        payload=derive_active_session_payload_v1(
            truth_root=truth_root,
            target_day=DAY,
            admission_ref=admission_ref,
        ),
    )
    _write_json(
        truth_root / "reports" / "submit_boundary_status_v1" / DAY / "submit_boundary_status.v1.json",
        {
            "schema_id": "submit_boundary_status",
            "schema_version": "v1",
            "authority_scope": "NON_AUTHORITY_FACT",
            "day_utc": DAY,
            "session_id": f"paper_session:{DAY}:PAPER",
            "submission_authorized": True,
            "boundary_status": "AUTHORIZED",
            "required_boundary_checks": [],
            "failed_checks": [],
            "blocking_codes": [],
            "producer": {"repo": "constellation", "module": "pytest", "git_sha": "test"},
            "produced_at_utc": f"{DAY}T00:00:00Z",
            "freshness_verdict": "CURRENT",
            "linkage_verdict": "LINKED",
            "paper_account": "DUO847203",
        },
    )
    _write_json(
        truth_root / "reports" / "capability_state_v1" / DAY / "capability_state.v1.json",
        {"overall_status": "FAIL"},
    )
    _write_json(
        truth_root / "reports" / "paper_policy_verdict_v1" / DAY / "paper_policy_verdict.v1.json",
        {"overall_status": "FAIL"},
    )
    _write_json(
        truth_root / "reports" / "gate_stack_verdict_v1" / DAY / "gate_stack_verdict.v1.json",
        {"status": "PASS"},
    )
    _write_json(
        truth_root / "reports" / "operator_summary_v1" / DAY / "operator_summary.v1.json",
        {"summary_state": "BLOCKED_VALID"},
    )
    return truth_root


def test_execution_dossier_resolves_sleeve_execution_root(tmp_path: Path) -> None:
    repo_root = _seed_minimal_repo(tmp_path)
    truth_root = _seed_minimal_truth(tmp_path)
    payload = build_execution_dossier_v1(repo_root=repo_root, truth_root=truth_root, day_utc=DAY)
    assert payload["subsystem_id"] == SUBSYSTEM_ID_EXECUTION_AUTHORITY
    assert payload["current_state"] == STATE_RESOLVED
    assert payload["canonical_owner_status"] == "sleeve_execution_root_v1"
    assert payload["ambiguity_state"]["status"] == STATE_CLEAR
    assert payload["sleeve_id"] == "PRIMARY"
    assert payload["mode"] == "PAPER"
    assert payload["execution_root_path"].endswith("/truth_sleeves/PRIMARY/PAPER")


def test_execution_dossier_blocks_when_sleeve_id_missing(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    repo_root = _seed_minimal_repo(tmp_path)
    truth_root = _seed_minimal_truth(tmp_path)
    monkeypatch.setattr("constellation_2.common.subsystem_authority_v1._active_paper_sleeve_row", lambda _: {})
    payload = build_execution_dossier_v1(repo_root=repo_root, truth_root=truth_root, day_utc=DAY)
    assert payload["current_state"] == STATE_BLOCKED
    assert payload["first_blocker"]["reason_code"] == RC_EXECUTION_ROOT_SLEEVE_ID_MISSING
    assert payload["ambiguity_state"]["status"] == STATE_CLEAR


def test_execution_dossier_blocks_on_root_mismatch(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    repo_root = _seed_minimal_repo(tmp_path)
    truth_root = _seed_minimal_truth(tmp_path)
    monkeypatch.setattr(
        "constellation_2.common.subsystem_authority_v1._active_paper_sleeve_row",
        lambda _: {
            "sleeve_id": "PRIMARY",
            "mode": "PAPER",
            "ib_account": "DUO847203",
            "truth_partition": "truth_sleeves/WRONG/PAPER",
        },
    )
    payload = build_execution_dossier_v1(repo_root=repo_root, truth_root=truth_root, day_utc=DAY)
    assert payload["current_state"] == STATE_BLOCKED
    assert payload["first_blocker"]["reason_code"] == RC_EXECUTION_ROOT_PATH_MISMATCH
    assert payload["ambiguity_state"]["status"] == STATE_CLEAR


def test_execution_profile_dossier_resolves_consistent_profile_sources(tmp_path: Path) -> None:
    repo_root = _seed_minimal_repo(tmp_path)
    truth_root = _seed_minimal_truth(tmp_path)
    payload = build_execution_profile_dossier_v1(repo_root=repo_root, truth_root=truth_root, day_utc=DAY)
    assert payload["current_state"] == STATE_RESOLVED
    assert payload["first_blocker"]["reason_code"] == ""
    assert payload["sleeve_id"] == "PRIMARY"
    assert payload["account_id"] == "DUO847203"
    assert payload["registry_profile"]["client_id_observer"] == "179"


def test_execution_identity_dossier_resolves_governed_submit_identity(tmp_path: Path) -> None:
    repo_root = _seed_minimal_repo(tmp_path)
    truth_root = _seed_minimal_truth(tmp_path)
    payload = build_execution_identity_dossier_v1(repo_root=repo_root, truth_root=truth_root, day_utc=DAY)
    assert payload["current_state"] == STATE_RESOLVED
    assert payload["canonical_owner_status"] == "execution_identity_binding_v1"
    assert payload["binding_status"] == STATE_RESOLVED
    assert payload["sleeve_id"] == "PRIMARY"
    assert payload["environment"] == "PAPER"
    assert payload["account_id"] == "DUO847203"
    assert payload["client_id_orders"] == "7"
    assert payload["client_id_observer"] == "179"


def test_execution_identity_dossier_blocks_on_profile_client_id_mismatch(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    repo_root = _seed_minimal_repo(tmp_path)
    truth_root = _seed_minimal_truth(tmp_path)
    original = build_execution_profile_dossier_v1

    def _mismatched_profile(**kwargs):
        payload = original(**kwargs)
        payload["registry_profile"]["client_id_orders"] = "99"
        return payload

    monkeypatch.setattr(
        "constellation_2.common.subsystem_authority_v1.build_execution_profile_dossier_v1",
        _mismatched_profile,
    )
    payload = build_execution_identity_dossier_v1(repo_root=repo_root, truth_root=truth_root, day_utc=DAY)
    assert payload["current_state"] == STATE_BLOCKED
    assert payload["first_blocker"]["reason_code"] == RC_EXECUTION_IDENTITY_CLIENT_ID_ORDERS_MISMATCH


def test_account_trading_policy_dossier_resolves_engine_symbol_authority(tmp_path: Path) -> None:
    repo_root = _seed_minimal_repo(tmp_path)
    truth_root = _seed_minimal_truth(tmp_path)
    payload = build_account_trading_policy_dossier_v1(repo_root=repo_root, truth_root=truth_root, day_utc=DAY)
    assert payload["current_state"] == STATE_RESOLVED
    assert payload["ambiguity_state"]["status"] != STATE_AMBIGUOUS_AUTHORITY
    assert payload["submission_policy_owner"] == "trading_symbol_policy_authority_v1"


def test_operator_summary_dossier_uses_session_and_submit_boundary_precedence(tmp_path: Path) -> None:
    repo_root = _seed_minimal_repo(tmp_path)
    truth_root = _seed_minimal_truth(tmp_path)
    payload = build_operator_summary_dossier_v1(repo_root=repo_root, truth_root=truth_root, day_utc=DAY)
    validated = validate_governed_artifact_payload_v1(
        repo_root=Path(__file__).resolve().parents[3],
        artifact_id="operator_summary_dossier_v1",
        payload=payload,
    )
    assert payload["current_state"] == STATE_READY
    assert payload["submission_authorization_status"] == "AUTHORIZED"
    assert payload["ambiguity_state"]["status"] == STATE_CLEAR
    assert payload["first_blocker"]["reason_code"] == ""
    assert payload["is_canonical"] is False
    assert payload["authority_level"] == "derived"
    assert "submit_boundary_status_v1" in payload["derived_from"]
    assert "active_session_v1" in payload["derived_from"]
    advisory = {row["source"]: row["classification"] for row in payload["advisory_only_signals"]}
    assert advisory["capability_state_v1"] == "ADVISORY_ONLY"
    assert advisory["paper_policy_verdict_v1"] == "ADVISORY_ONLY"
    assert advisory["operator_summary_v1"] == "LEGACY_DERIVED_ONLY"
    assert validated["constitutional_dependency_declaration"]["declared_dependency_artifacts"] == [
        "active_session_v1",
        "target_day_admission_v1",
        "target_day_build_v1",
        "submit_boundary_status_v1",
    ]


def test_build_subsystem_authority_payloads_includes_operator_summary_bundle(tmp_path: Path) -> None:
    repo_root = _seed_minimal_repo(tmp_path)
    truth_root = _seed_minimal_truth(tmp_path)
    payloads = build_subsystem_authority_payloads_v1(repo_root=repo_root, truth_root=truth_root, day_utc=DAY)
    assert SUBSYSTEM_ID_EXECUTION_AUTHORITY in payloads
    assert payloads["execution_identity_dossier_v1"]["current_state"] == STATE_RESOLVED
    assert payloads["operator_summary_dossier_v1"]["current_state"] == STATE_READY
    assert payloads["account_trading_policy_dossier_v1"]["first_blocker"]["reason_code"] != RC_ACCOUNT_TRADING_POLICY_AUTHORITY_AMBIGUOUS
