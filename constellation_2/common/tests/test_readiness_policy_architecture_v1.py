from __future__ import annotations

import json
import sys
import tempfile
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

REPO_ROOT = Path(__file__).resolve().parents[3]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from constellation_2.common.capability_state_v1 import (
    derive_capability_state_payload,
    resolve_capability_state_path,
    resolve_paper_policy_verdict_path,
    resolve_policy_diff_path,
    resolve_production_policy_verdict_path,
)
from constellation_2.common.paper_policy_verdict_v1 import derive_paper_policy_verdict_payload
from constellation_2.common.paper_session_fact_plane_v1 import SurfaceRefV1
from constellation_2.common.policy_diff_v1 import (
    derive_policy_diff_payload,
    summarize_override_impact_from_replay_manifests_v1,
)
from constellation_2.common.production_policy_verdict_v1 import derive_production_policy_verdict_payload
import ops.tools.run_trade_submit_readiness_c2_v1 as readiness_module
import ops.tools.run_production_policy_verdict_v1 as production_policy_module


DAY = "2026-04-09"


def _write_json(path: Path, obj: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(obj, sort_keys=True, separators=(",", ":")) + "\n", encoding="utf-8")


def _surface_ref(path: Path, payload: dict) -> SurfaceRefV1:
    return SurfaceRefV1(path=path, payload=payload, sha256=readiness_module._sha256_file(path))


def _write_minimal_registries(root: Path) -> None:
    _write_json(
        root / "governance" / "02_REGISTRIES" / "C2_IB_ACCOUNT_REGISTRY_V1.json",
        {
            "schema_id": "c2_ib_account_registry",
            "schema_version": "v1",
            "accounts": [
                {
                    "account_id": "DUO847203",
                    "environment": "PAPER",
                    "enabled_for_submission": True,
                    "allowed_sleeve_ids": ["PRIMARY"],
                }
            ],
        },
    )
    _write_json(
        root / "governance" / "02_REGISTRIES" / "C2_SLEEVE_REGISTRY_V1.json",
        {
            "schema_id": "c2_sleeve_registry",
            "schema_version": "v1",
            "sleeves": [
                {
                    "sleeve_id": "PRIMARY",
                    "enabled": True,
                    "mode": "PAPER",
                    "execution_mode": "AUTO",
                    "status": "PRODUCTION",
                    "ib_account": "DUO847203",
                    "truth_partition": "truth_sleeves/PRIMARY/PAPER",
                    "assigned_engine_ids": ["C2_TREND_EQ_PRIMARY_V1"],
                    "active_controllable_engine_ids": ["C2_TREND_EQ_PRIMARY_V1"],
                    "disabled_by_default_engine_ids": [],
                    "support_only_engine_ids": [],
                }
            ],
        },
    )
    _write_json(
        root / "governance" / "02_REGISTRIES" / "GATE_HIERARCHY_V1.json",
        {
            "schema_id": "GATE_HIERARCHY_V1",
            "schema_version": 1,
            "gates": [
                {"gate_id": "operator_daily_gate_v3", "artifact_relpath": "operator_daily_gate.v3.json", "class": "CLASS1_SYSTEM_HARD_STOP", "required": True, "blocking": True},
                {"gate_id": "feed_attestation_gate_v1", "artifact_relpath": "feed_attestation_gate.v1.json", "class": "CLASS1_SYSTEM_HARD_STOP", "required": True, "blocking": True},
                {"gate_id": "heartbeat_gate_v1", "artifact_relpath": "heartbeat_gate.v1.json", "class": "CLASS1_SYSTEM_HARD_STOP", "required": True, "blocking": True},
                {"gate_id": "capital_risk_envelope_v2", "artifact_relpath": "capital_risk_envelope.v2.json", "class": "CLASS1_SYSTEM_HARD_STOP", "required": True, "blocking": True},
                {"gate_id": "correlation_envelope_gate_v1", "artifact_relpath": "correlation_envelope_gate.v1.json", "class": "CLASS1_SYSTEM_HARD_STOP", "required": True, "blocking": True},
                {"gate_id": "liquidity_slippage_gate_v1", "artifact_relpath": "liquidity_slippage_gate.v1.json", "class": "CLASS1_SYSTEM_HARD_STOP", "required": True, "blocking": True},
                {"gate_id": "replay_certification_gate_v1", "artifact_relpath": "replay_certification_gate.v1.json", "class": "CLASS1_SYSTEM_HARD_STOP", "required": True, "blocking": True},
            ],
        },
    )
    _write_json(
        root / "governance" / "02_REGISTRIES" / "CAPABILITY_POLICY_REGISTRY_V1.json",
        {
            "schema_id": "capability_policy_registry",
            "schema_version": "v1",
            "status": "ACTIVE",
            "owner": "Constellation_2",
            "capabilities": [
                {"capability_id": "account_binding_valid", "paper_role": "BLOCKING", "production_role": "BLOCKING", "owner": "Constellation_2", "rationale": "binding"},
                {"capability_id": "startup_materialization_ready", "paper_role": "BLOCKING", "production_role": "BLOCKING", "owner": "Constellation_2", "rationale": "startup"},
                {"capability_id": "paper_trading_posture_ready", "paper_role": "BLOCKING", "production_role": "BLOCKING", "owner": "Constellation_2", "rationale": "posture"},
                {"capability_id": "broker_connectivity_available", "paper_role": "BLOCKING", "production_role": "BLOCKING", "owner": "Constellation_2", "rationale": "handshake"},
                {"capability_id": "core_sleeve_gate_set_ready", "paper_role": "BLOCKING", "production_role": "BLOCKING", "owner": "Constellation_2", "rationale": "core gates"},
                {"capability_id": "production_certification_gate_set_complete", "paper_role": "ADVISORY", "production_role": "BLOCKING", "owner": "Constellation_2", "rationale": "prod cert"},
            ],
        },
    )


def _write_minimal_evidence(root: Path, truth_root: Path) -> Path:
    sleeve_truth = root / "constellation_2" / "runtime" / "truth_sleeves" / "PRIMARY" / "PAPER"
    _write_json(
        truth_root / "reports" / "startup_materialization_v1" / DAY / "startup_materialization.v1.json",
        {
            "schema_id": "startup_materialization",
            "schema_version": "v1",
            "authority_scope": "NON_AUTHORITY_FACT",
            "day_utc": DAY,
            "session_id": f"paper_session:{DAY}:PAPER",
            "status": "SUCCESS",
            "blocking_codes": [],
            "freshness_verdict": "CURRENT",
            "linkage_verdict": "LINKED",
            "materialized_outputs": [],
            "path_resolution_evidence": {
                "latest_active_attempt_path": str(truth_root / "phaseC_preflight_v1" / DAY / "latest_active_attempt.v1.json"),
                "phasec_root": str(truth_root / "phaseC_preflight_v1" / DAY),
            },
            "phasec_materializer_result": {"returncode": 0, "stdout": "", "stderr": ""},
            "produced_at_utc": f"{DAY}T00:00:00Z",
            "producer": {"repo": "constellation", "module": "test", "git_sha": "a" * 40},
            "producer_run_id": f"startup_materialization:{DAY}",
            "required_inputs_checked": [],
        },
    )
    _write_json(
        truth_root / "reports" / "paper_trading_posture_v1" / DAY / "paper_trading_posture.v1.json",
        {
            "schema_id": "paper_trading_posture",
            "schema_version": "v1",
            "authority_scope": "NON_AUTHORITY_FACT",
            "day_utc": DAY,
            "session_id": f"paper_session:{DAY}:PAPER",
            "posture_status": "ENABLED",
            "posture_class": "PAPER_READY_ACTIVE",
            "system_ready": True,
            "freshness_verdict": "CURRENT",
            "linkage_verdict": "LINKED",
            "blocking_family": "NONE",
            "blocking_codes": [],
            "blocking_reason_codes": [],
            "policy_reasons": ["MARKET_CALENDAR_TRADING_SESSION"],
            "expected_no_op_today": False,
            "source_dependencies": [],
            "produced_at_utc": f"{DAY}T00:00:00Z",
            "producer": {"repo": "constellation", "module": "test", "git_sha": "a" * 40},
        },
    )
    handshake_path = truth_root / "ib_api_handshake" / DAY / "ib_api_handshake.v1.json"
    _write_json(
        handshake_path,
        {
            "schema_id": "C2_IB_API_HANDSHAKE_V1",
            "schema_version": 1,
            "day_utc": DAY,
            "status": "OK",
            "ok": True,
            "environment": "PAPER",
            "ib_account": "DUO847203",
            "reason_codes": ["HANDSHAKE_OK_NEXTVALIDID_SEEN_NO_504_AFTER"],
            "inputs": {},
            "observations": {},
        },
    )
    _write_json(
        truth_root / "ib_api_handshake" / "latest_pointer.v1.json",
        {
            "schema_id": "C2_IB_API_HANDSHAKE_LATEST_POINTER_V1",
            "schema_version": 1,
            "day_utc": DAY,
            "pointers": {
                "handshake_path": str(handshake_path),
                "handshake_sha256": readiness_module._sha256_file(handshake_path),
            },
        },
    )
    gate_specs = {
        "capital_risk_envelope_v2": ("capital_risk_envelope.v2.json", "PASS", []),
        "correlation_envelope_gate_v1": ("correlation_envelope_gate.v1.json", "PASS", []),
        "liquidity_slippage_gate_v1": ("liquidity_slippage_gate.v1.json", "PASS", ["LIQPOL_PASS"]),
        "operator_daily_gate_v3": ("operator_daily_gate.v3.json", "PASS", []),
        "feed_attestation_gate_v1": ("feed_attestation_gate.v1.json", "FAIL", ["FAL_STALE"]),
        "heartbeat_gate_v1": ("heartbeat_gate.v1.json", "FAIL", ["C2_HB_MISSING:C2_TREND_EQ_PRIMARY_V1"]),
        "replay_certification_gate_v1": ("replay_certification_gate.v1.json", "PASS", ["REPLAY_CERT_FIRST_RUN"]),
    }
    for gate_id, (filename, status, reasons) in gate_specs.items():
        _write_json(
            sleeve_truth / "reports" / gate_id / DAY / filename,
            {"schema_id": gate_id, "schema_version": "v1", "day_utc": DAY, "status": status, "reason_codes": reasons},
        )
    gate_stack_path = sleeve_truth / "reports" / "gate_stack_verdict_v1" / DAY / "gate_stack_verdict.v1.json"
    _write_json(
        gate_stack_path,
        {
            "schema_id": "gate_stack_verdict",
            "schema_version": "v1",
            "day_utc": DAY,
            "produced_utc": f"{DAY}T00:00:00Z",
            "status": "FAIL",
            "blocking_class": "CLASS1_SYSTEM_HARD_STOP",
            "reason_codes": ["GATE_REQUIRED_NOT_PASS:feed_attestation_gate_v1:FAIL"],
            "input_manifest": [],
            "gates": [
                {"gate_id": "capital_risk_envelope_v2", "gate_class": "CLASS1_SYSTEM_HARD_STOP", "required": True, "blocking": True, "status": "PASS", "artifact_path": str(sleeve_truth / "reports" / "capital_risk_envelope_v2" / DAY / "capital_risk_envelope.v2.json"), "artifact_sha256": "a" * 64, "reason_codes": []},
                {"gate_id": "feed_attestation_gate_v1", "gate_class": "CLASS1_SYSTEM_HARD_STOP", "required": True, "blocking": True, "status": "FAIL", "artifact_path": str(sleeve_truth / "reports" / "feed_attestation_gate_v1" / DAY / "feed_attestation_gate.v1.json"), "artifact_sha256": "b" * 64, "reason_codes": []},
            ],
        },
    )
    return gate_stack_path


def _write_canonical_production_cert_gates(canonical_sleeve_truth_root: Path) -> None:
    gate_specs = {
        "feed_attestation_gate_v1": ("feed_attestation_gate.v1.json", "PASS", []),
        "heartbeat_gate_v1": ("heartbeat_gate.v1.json", "PASS", []),
        "replay_certification_gate_v1": ("replay_certification_gate.v1.json", "PASS", ["REPLAY_CERT_FIRST_RUN"]),
    }
    for gate_id, (filename, status, reasons) in gate_specs.items():
        _write_json(
            canonical_sleeve_truth_root / "reports" / gate_id / DAY / filename,
            {"schema_id": gate_id, "schema_version": "v1", "day_utc": DAY, "status": status, "reason_codes": reasons},
        )
    _write_json(
        canonical_sleeve_truth_root / "reports" / "gate_stack_verdict_v1" / DAY / "gate_stack_verdict.v1.json",
        {
            "schema_id": "gate_stack_verdict",
            "schema_version": "v1",
            "day_utc": DAY,
            "produced_utc": f"{DAY}T00:00:00Z",
            "status": "FAIL",
            "blocking_class": "CLASS1_SYSTEM_HARD_STOP",
            "reason_codes": ["GATE_REQUIRED_NOT_PASS:correlation_envelope_gate_v1:FAIL"],
            "input_manifest": [],
            "gates": [],
        },
    )


def test_capability_state_derives_core_and_production_capabilities() -> None:
    with tempfile.TemporaryDirectory(dir=str(REPO_ROOT / "tmp")) as td:
        root = Path(td)
        truth_root = root / "runtime_truth"
        _write_minimal_registries(root)
        _write_minimal_evidence(root, truth_root)

        with patch(
            "constellation_2.common.trade_submit_readiness_authority_v1.resolve_truth_sleeves_root",
            side_effect=RuntimeError("no canonical sleeve root in unit test"),
        ), patch(
            "constellation_2.common.capability_state_v1.resolve_decision_truth_root_v1",
            return_value=truth_root.resolve(),
        ), patch(
            "constellation_2.common.capability_state_v1.resolve_governed_account_binding",
            return_value=SimpleNamespace(
                account_registry_path=root / "governance" / "02_REGISTRIES" / "C2_IB_ACCOUNT_REGISTRY_V1.json",
                sleeve_registry_path=root / "governance" / "02_REGISTRIES" / "C2_SLEEVE_REGISTRY_V1.json",
            ),
        ), patch(
            "constellation_2.common.capability_state_v1.resolve_governed_sleeve_truth_bindings",
            return_value=[SimpleNamespace(sleeve_id="PRIMARY", truth_root=root / "constellation_2" / "runtime" / "truth_sleeves" / "PRIMARY" / "PAPER")],
        ):
            payload = derive_capability_state_payload(
                repo_root=root,
                truth_root=truth_root,
                day_utc=DAY,
                ib_account="DUO847203",
                environment="PAPER",
            )

        by_id = {row["capability_id"]: row for row in payload["capabilities"]}
        assert by_id["account_binding_valid"]["status"] == "PASS"
        assert by_id["core_sleeve_gate_set_ready"]["status"] == "PASS"
        assert by_id["production_certification_gate_set_complete"]["status"] == "FAIL"


def test_capability_state_passes_missing_monday_core_gate_class_when_gate_artifacts_exist() -> None:
    with tempfile.TemporaryDirectory(dir=str(REPO_ROOT / "tmp")) as td:
        root = Path(td)
        truth_root = root / "runtime_truth"
        sleeve_truth = root / "constellation_2" / "runtime" / "truth_sleeves" / "PRIMARY" / "PAPER"
        _write_minimal_registries(root)
        _write_minimal_evidence(root, truth_root)
        _write_json(
            sleeve_truth / "reports" / "authorization_gate_verdict_v1" / DAY / "authorization_gate_verdict.v1.json",
            {
                "schema_id": "authorization_gate_verdict_v1",
                "schema_version": 1,
                "day_utc": DAY,
                "mode": "PAPER",
                "produced_utc": f"{DAY}T00:00:00Z",
                "included_gates": [],
                "excluded_gates": [],
                "blocking_gates": [],
                "status": "PASS",
                "blocking_class": "NONE",
                "reason_codes": [],
                "evidence_refs": [],
                "decision_ledger_ref": "ledger",
                "gate_classification_registry_id": "test",
                "gate_classification_registry_version": "v1",
                "lifecycle_state_authority_ref": "lifecycle",
            },
        )
        _write_json(
            sleeve_truth / "reports" / "economic_health_gate_verdict_v1" / DAY / "economic_health_gate_verdict.v1.json",
            {
                "schema_id": "economic_health_gate_verdict_v1",
                "schema_version": 1,
                "day_utc": DAY,
                "mode": "PAPER",
                "produced_utc": f"{DAY}T00:00:00Z",
                "included_gates": [],
                "excluded_gates": [],
                "blocking_gates": [],
                "status": "PASS",
                "blocking_class": "NONE",
                "reason_codes": [],
                "evidence_refs": [],
                "decision_ledger_ref": "ledger",
                "gate_classification_registry_id": "test",
                "gate_classification_registry_version": "v1",
                "lifecycle_state_authority_ref": "lifecycle",
            },
        )

        with patch(
            "constellation_2.common.trade_submit_readiness_authority_v1.resolve_truth_sleeves_root",
            side_effect=RuntimeError("no canonical sleeve root in unit test"),
        ), patch(
            "constellation_2.common.capability_state_v1.resolve_decision_truth_root_v1",
            return_value=truth_root.resolve(),
        ), patch(
            "constellation_2.common.capability_state_v1.resolve_governed_account_binding",
            return_value=SimpleNamespace(
                account_registry_path=root / "governance" / "02_REGISTRIES" / "C2_IB_ACCOUNT_REGISTRY_V1.json",
                sleeve_registry_path=root / "governance" / "02_REGISTRIES" / "C2_SLEEVE_REGISTRY_V1.json",
            ),
        ), patch(
            "constellation_2.common.capability_state_v1.resolve_governed_sleeve_truth_bindings",
            return_value=[SimpleNamespace(sleeve_id="PRIMARY", truth_root=root / "constellation_2" / "runtime" / "truth_sleeves" / "PRIMARY" / "PAPER")],
        ):
            payload = derive_capability_state_payload(
                repo_root=root,
                truth_root=truth_root,
                day_utc=DAY,
                ib_account="DUO847203",
                environment="PAPER",
            )

        by_id = {row["capability_id"]: row for row in payload["capabilities"]}
        assert by_id["core_sleeve_gate_set_ready"]["status"] == "PASS"
        assert by_id["economic_health_gate_set_complete"]["status"] == "PASS"
        assert not any(code.endswith(":MISSING") for code in by_id["core_sleeve_gate_set_ready"]["reason_codes"])


def test_capability_state_resolves_authoritative_sleeve_truth_from_release_repo_role() -> None:
    with tempfile.TemporaryDirectory(dir=str(REPO_ROOT / "tmp")) as td:
        tmp_root = Path(td)
        authoritative_root = tmp_root / "authoritative_repo"
        release_root = tmp_root / "release_repo"
        truth_root = tmp_root / "runtime_truth"

        _write_minimal_registries(authoritative_root)
        _write_minimal_evidence(authoritative_root, truth_root)
        _write_minimal_registries(release_root)
        _write_json(
            release_root / "repo_role.v1.json",
            {
                "schema_id": "repo_role",
                "schema_version": "v1",
                "authoritative_repo_root": str(authoritative_root),
            },
        )

        canonical_truth_sleeves_root = tmp_root / "runtime_data" / "truth_sleeves"
        _write_canonical_production_cert_gates(canonical_truth_sleeves_root / "PRIMARY" / "PAPER")

        with patch(
            "constellation_2.common.trade_submit_readiness_authority_v1.resolve_truth_sleeves_root",
            return_value=canonical_truth_sleeves_root,
        ):
            payload = derive_capability_state_payload(
                repo_root=release_root,
                truth_root=truth_root,
                day_utc=DAY,
                ib_account="DUO847203",
                environment="PAPER",
            )

        by_id = {row["capability_id"]: row for row in payload["capabilities"]}
        assert by_id["account_binding_valid"]["status"] == "PASS"
        assert by_id["core_sleeve_gate_set_ready"]["status"] == "PASS"
        assert by_id["production_certification_gate_set_complete"]["status"] == "PASS"
        assert authoritative_root.as_posix() in by_id["core_sleeve_gate_set_ready"]["details"]["sleeve_truth_root"]
        assert str(canonical_truth_sleeves_root / "PRIMARY" / "PAPER") == by_id["production_certification_gate_set_complete"]["details"]["sleeve_truth_root"]


def test_production_policy_runner_resolves_authoritative_sleeve_truth_from_release_repo_role() -> None:
    with tempfile.TemporaryDirectory(dir=str(REPO_ROOT / "tmp")) as td:
        tmp_root = Path(td)
        authoritative_root = tmp_root / "authoritative_repo"
        release_root = tmp_root / "release_repo"

        _write_minimal_registries(authoritative_root)
        _write_minimal_registries(release_root)
        _write_json(
            release_root / "repo_role.v1.json",
            {
                "schema_id": "repo_role",
                "schema_version": "v1",
                "authoritative_repo_root": str(authoritative_root),
            },
        )
        (
            authoritative_root
            / "constellation_2"
            / "runtime"
            / "truth_sleeves"
            / "PRIMARY"
            / "PAPER"
        ).mkdir(parents=True, exist_ok=True)

        expected = (tmp_root / "runtime_data" / "truth_sleeves" / "PRIMARY" / "PAPER").resolve()
        expected.mkdir(parents=True, exist_ok=True)

        with patch.object(production_policy_module, "REPO_ROOT", release_root), patch(
            "constellation_2.common.trade_submit_readiness_authority_v1.resolve_truth_sleeves_root",
            return_value=(tmp_root / "runtime_data" / "truth_sleeves").resolve(),
        ):
            resolved = production_policy_module._resolve_primary_sleeve_truth_root(
                ib_account="DUO847203",
                environment="PAPER",
            )

        assert resolved == expected


def test_policy_verdicts_split_paper_from_production_without_splitting_evidence() -> None:
    with tempfile.TemporaryDirectory(dir=str(REPO_ROOT / "tmp")) as td:
        root = Path(td)
        truth_root = root / "runtime_truth"
        _write_minimal_registries(root)
        gate_stack_path = _write_minimal_evidence(root, truth_root)

        with patch(
            "constellation_2.common.trade_submit_readiness_authority_v1.resolve_truth_sleeves_root",
            side_effect=RuntimeError("no canonical sleeve root in unit test"),
        ):
            capability_payload = derive_capability_state_payload(
                repo_root=root,
                truth_root=truth_root,
                day_utc=DAY,
                ib_account="DUO847203",
                environment="PAPER",
            )
        capability_path = resolve_capability_state_path(truth_root=truth_root, day_utc=DAY)
        _write_json(capability_path, capability_payload)
        capability_ref = _surface_ref(capability_path, capability_payload)

        paper_payload = derive_paper_policy_verdict_payload(repo_root=root, truth_root=truth_root, capability_ref=capability_ref)
        paper_path = resolve_paper_policy_verdict_path(truth_root=truth_root, day_utc=DAY)
        _write_json(paper_path, paper_payload)
        paper_ref = _surface_ref(paper_path, paper_payload)

        production_payload = derive_production_policy_verdict_payload(
            repo_root=root,
            truth_root=truth_root,
            capability_ref=capability_ref,
            gate_stack_path=gate_stack_path,
        )
        production_path = resolve_production_policy_verdict_path(truth_root=truth_root, day_utc=DAY)
        _write_json(production_path, production_payload)
        production_ref = _surface_ref(production_path, production_payload)

        diff_payload = derive_policy_diff_payload(
            capability_ref=capability_ref,
            paper_policy_ref=paper_ref,
            production_policy_ref=production_ref,
        )

        assert paper_payload["overall_status"] == "PASS"
        assert production_payload["overall_status"] == "FAIL"
        assert any(item["capability_id"] == "production_certification_gate_set_complete" for item in paper_payload["production_only_open_items"])
        assert any(item["item_id"] == "production_certification_gate_set_complete" for item in production_payload["blocking_items"])
        assert any((row.get("capability_id") or row.get("item_id")) == "production_certification_gate_set_complete" for row in diff_payload["production_only_open_items"])


def test_trade_submit_readiness_uses_paper_policy_when_production_only_items_remain_open() -> None:
    with tempfile.TemporaryDirectory(dir=str(REPO_ROOT / "tmp")) as td:
        root = Path(td)
        truth_root = root / "constellation_2" / "runtime" / "truth"
        _write_minimal_registries(root)
        gate_stack_path = _write_minimal_evidence(root, truth_root)

        with patch(
            "constellation_2.common.trade_submit_readiness_authority_v1.resolve_truth_sleeves_root",
            side_effect=RuntimeError("no canonical sleeve root in unit test"),
        ):
            capability_payload = derive_capability_state_payload(
                repo_root=root,
                truth_root=truth_root,
                day_utc=DAY,
                ib_account="DUO847203",
                environment="PAPER",
            )
        capability_path = resolve_capability_state_path(truth_root=truth_root, day_utc=DAY)
        _write_json(capability_path, capability_payload)
        capability_ref = _surface_ref(capability_path, capability_payload)

        paper_payload = derive_paper_policy_verdict_payload(repo_root=root, truth_root=truth_root, capability_ref=capability_ref)
        paper_path = resolve_paper_policy_verdict_path(truth_root=truth_root, day_utc=DAY)
        _write_json(paper_path, paper_payload)

        production_payload = derive_production_policy_verdict_payload(
            repo_root=root,
            truth_root=truth_root,
            capability_ref=capability_ref,
            gate_stack_path=gate_stack_path,
        )
        production_path = resolve_production_policy_verdict_path(truth_root=truth_root, day_utc=DAY)
        _write_json(production_path, production_payload)

        diff_payload = derive_policy_diff_payload(
            capability_ref=capability_ref,
            paper_policy_ref=_surface_ref(paper_path, paper_payload),
            production_policy_ref=_surface_ref(production_path, production_payload),
        )
        diff_path = resolve_policy_diff_path(truth_root=truth_root, day_utc=DAY)
        _write_json(diff_path, diff_payload)

        with patch.object(readiness_module, "REPO_ROOT", root), patch.object(
            readiness_module, "TRUTH_ROOT", truth_root
        ), patch.object(
            readiness_module, "OUT_ROOT", truth_root / "trade_submit_readiness_c2_v1"
        ), patch.object(
            readiness_module, "validate_against_repo_schema_v1", lambda *args, **kwargs: None
        ), patch.object(
            readiness_module, "_refresh_policy_stack_for_day", lambda **kwargs: None
        ), patch(
            "sys.argv",
            ["run_trade_submit_readiness_c2_v1.py", "--day_utc", DAY, "--ib_account", "DUO847203", "--environment", "PAPER"],
        ):
            rc = readiness_module.main()

        assert rc == 0
        status = json.loads(
            (truth_root / "trade_submit_readiness_c2_v1" / "_history" / "PAPER" / "DUO847203" / DAY / "status.json").read_text(
                encoding="utf-8"
            )
        )
        assert status["ok"] is True
        assert "PAPER_POLICY_VERDICT_OK" in status["reasons"]
        assert "INFO:PRODUCTION_POLICY_NOT_PASS" in status["reasons"]
        assert "INFO:PRODUCTION_ONLY_OPEN:production_certification_gate_set_complete" in status["reasons"]


def test_paper_policy_fails_closed_when_startup_materialization_capability_fails() -> None:
    with tempfile.TemporaryDirectory(dir=str(REPO_ROOT / "tmp")) as td:
        root = Path(td)
        truth_root = root / "constellation_2" / "runtime" / "truth"
        _write_minimal_registries(root)
        capability_payload = {
            "schema_id": "capability_state",
            "schema_version": "v1",
            "day_utc": DAY,
            "environment": "PAPER",
            "ib_account": "DUO847203",
            "sleeve_id": "PRIMARY",
            "overall_status": "FAIL",
            "capabilities": [
                {
                    "capability_id": "startup_materialization_ready",
                    "status": "FAIL",
                    "kind": "SYNTHESIZED_SUMMARY",
                    "reason_codes": [
                        "STARTUP_MATERIALIZATION_FAIL:PHASEC_RISK_INPUTS_PREP_FAIL:ACCOUNTING_NAV_COMPAT_BRIDGE_NONZERO"
                    ],
                    "source_artifacts": [],
                    "details": {},
                }
            ],
            "source_artifacts": [],
            "registry_ref": {"artifact_sha256": "a" * 64},
            "release_id": "TEST_RELEASE",
            "git_sha": "b" * 40,
            "generated_at_utc": f"{DAY}T00:00:00Z",
        }
        capability_path = resolve_capability_state_path(truth_root=truth_root, day_utc=DAY)
        _write_json(capability_path, capability_payload)
        capability_ref = _surface_ref(capability_path, capability_payload)

        paper_payload = derive_paper_policy_verdict_payload(repo_root=root, truth_root=truth_root, capability_ref=capability_ref)

        assert paper_payload["overall_status"] == "FAIL"
        assert paper_payload["paper_allowed"] is False
        assert paper_payload["blocking_items"][0]["capability_id"] == "startup_materialization_ready"


def test_policy_diff_reflects_override_impact_from_replay_manifests() -> None:
    with tempfile.TemporaryDirectory(dir=str(REPO_ROOT / "tmp")) as td:
        root = Path(td)
        truth_root = root / "constellation_2" / "runtime" / "truth"
        capability_payload = {
            "schema_id": "capability_state",
            "schema_version": "v1",
            "day_utc": DAY,
            "environment": "PAPER",
            "ib_account": "DUO847203",
            "sleeve_id": "PRIMARY",
            "overall_status": "PASS",
            "capabilities": [],
            "source_artifacts": [],
            "registry_ref": {"artifact_sha256": "a" * 64},
            "release_id": "TEST_RELEASE",
            "git_sha": "b" * 40,
            "generated_at_utc": f"{DAY}T00:00:00Z",
        }
        capability_path = resolve_capability_state_path(truth_root=truth_root, day_utc=DAY)
        _write_json(capability_path, capability_payload)
        capability_ref = _surface_ref(capability_path, capability_payload)

        paper_payload = {
            "schema_id": "paper_policy_verdict",
            "schema_version": "v1",
            "day_utc": DAY,
            "overall_status": "PASS",
            "blocking_items": [],
            "production_only_open_items": [],
            "source_artifacts": [],
            "generated_at_utc": f"{DAY}T00:00:00Z",
        }
        paper_path = resolve_paper_policy_verdict_path(truth_root=truth_root, day_utc=DAY)
        _write_json(paper_path, paper_payload)
        paper_ref = _surface_ref(paper_path, paper_payload)

        production_payload = {
            "schema_id": "production_policy_verdict",
            "schema_version": "v1",
            "day_utc": DAY,
            "overall_status": "FAIL",
            "blocking_items": [{"item_id": "production_certification_gate_set_complete"}],
            "source_artifacts": [],
            "generated_at_utc": f"{DAY}T00:00:00Z",
        }
        production_path = resolve_production_policy_verdict_path(truth_root=truth_root, day_utc=DAY)
        _write_json(production_path, production_payload)
        production_ref = _surface_ref(production_path, production_payload)

        replay_manifest_path = truth_root / "reports" / "replay_manifest_v1" / DAY / ("a" * 64 + ".replay_manifest.v1.json")
        _write_json(
            replay_manifest_path,
            {
                "schema_id": "C2_REPLAY_MANIFEST_V1",
                "schema_version": 1,
                "produced_utc": f"{DAY}T00:00:00Z",
                "day_utc": DAY,
                "source_truth_root": str(truth_root),
                "replay_truth_root": str((root / "replay").resolve()),
                "submission_id": "a" * 64,
                "status": "OK",
                "reason_codes": [],
                "tool_runs": [],
                "comparisons": [],
                "override_analysis": {
                    "comparison_status": "identical",
                    "source": {
                        "review_required_count": 1,
                        "operator_decision_count": 1,
                        "approved_override_count": 1,
                        "override_frequency_by_action_class": {"PROTECTIVE": 1},
                        "mismatch_count": 1,
                        "mismatch_cases": [],
                        "block_reasons_distribution": {"POLICY_REQUIRES_HUMAN_REVIEW": 1},
                        "human_system_divergence_count": 1,
                        "override_cases": [],
                    },
                    "replay": {
                        "review_required_count": 1,
                        "operator_decision_count": 1,
                        "approved_override_count": 1,
                        "override_frequency_by_action_class": {"PROTECTIVE": 1},
                        "mismatch_count": 1,
                        "mismatch_cases": [],
                        "block_reasons_distribution": {"POLICY_REQUIRES_HUMAN_REVIEW": 1},
                        "human_system_divergence_count": 1,
                        "override_cases": [],
                    },
                },
                "canonical_json_hash": "b" * 64,
            },
        )

        override_impact = summarize_override_impact_from_replay_manifests_v1(
            truth_root=truth_root,
            day_utc=DAY,
        )
        diff_payload = derive_policy_diff_payload(
            capability_ref=capability_ref,
            paper_policy_ref=paper_ref,
            production_policy_ref=production_ref,
            override_impact_analysis=override_impact,
        )

        assert diff_payload["override_impact_analysis"]["replay_manifest_count"] == 1
        assert diff_payload["override_impact_analysis"]["override_frequency_by_action_class"] == {"PROTECTIVE": 1}
        assert diff_payload["override_impact_analysis"]["mismatch_count"] == 1
