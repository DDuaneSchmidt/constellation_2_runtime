from __future__ import annotations

import hashlib
import json
import sys
from pathlib import Path
from types import SimpleNamespace

import pytest


def _repo_root() -> Path:
    return Path(__file__).resolve().parents[3]


REPO_ROOT = _repo_root()
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from constellation_2.common.budget_enforcement_v1 import check_budget_enforcement_day_v1  # noqa: E402
import constellation_2.common.tests.test_phase2_immutable_record_adapters_v1 as phase2  # noqa: E402
import ops.tools.run_portfolio_governance_snapshot_day_v1 as portfolio_snapshot_writer  # noqa: E402
import ops.tools.run_sleeve_allocation_epoch_day_v1 as allocation_epoch_writer  # noqa: E402
import ops.tools.run_intent_authorization_ledger_day_v1 as authorization_ledger_writer  # noqa: E402
import ops.tools.run_exposure_ledger_day_v1 as exposure_ledger_writer  # noqa: E402
import ops.tools.run_epoch_ledger_reconciliation_day_v1 as epoch_reconciliation_writer  # noqa: E402
import ops.tools.run_submit_boundary_status_v1 as submit_boundary_writer  # noqa: E402


DAY = "2026-04-14"
DAY_EXPOSURE = "2026-04-13"
INTENT_HASH = phase2.INTENT_HASH
INTENT_HASH_EXPOSURE = phase2.INTENT_HASH_EXPOSURE
BINDING_HASH = phase2.BINDING_HASH


def _load_json(path: Path) -> dict:
    return json.loads(path.read_text(encoding="utf-8"))


def _load_jsonl(path: Path) -> list[dict]:
    return [json.loads(line) for line in path.read_text(encoding="utf-8").splitlines() if line.strip()]


def _write_json(path: Path, obj: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(obj, sort_keys=True, indent=2) + "\n", encoding="utf-8")


def _run_main(module, *argv: str, monkeypatch: pytest.MonkeyPatch) -> int:
    monkeypatch.setattr(sys, "argv", [module.__name__.split(".")[-1], *argv])
    return module.main()


def _seed_base_budget_context(truth_root: Path, *, day_utc: str, intent_hash: str) -> None:
    phase2._seed_capital_risk_envelope(truth_root, day_utc=day_utc)
    phase2._seed_capital_allocation(truth_root, day_utc=day_utc, intent_hash=intent_hash)
    phase2._seed_authorization_artifact(truth_root, day_utc=day_utc, intent_hash=intent_hash, status="AUTHORIZED")
    phase2._seed_equity_order_plan(truth_root, day_utc=day_utc, intent_hash=intent_hash)


def _materialize_epoch(truth_root: Path, *, day_utc: str, monkeypatch: pytest.MonkeyPatch) -> None:
    assert _run_main(portfolio_snapshot_writer, "--day", day_utc, "--truth_root", str(truth_root), monkeypatch=monkeypatch) == 0
    assert _run_main(allocation_epoch_writer, "--day", day_utc, "--truth_root", str(truth_root), monkeypatch=monkeypatch) == 0


def test_shadow_mode_does_not_block(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    truth_root = tmp_path / "truth"
    validation = {
        "schema_id": "C2_INTENT_BUDGET_VALIDATION_V1_UNGOVERNED",
        "schema_version": 1,
        "day_utc": DAY,
        "intent_hash": INTENT_HASH,
        "intent_id": "intent-shadow",
        "engine_id": "C2_TREND_EQ_PRIMARY_V1",
        "canonical_sleeve_id": "C2_TREND_EQ_PRIMARY",
        "allocation_epoch_id": "epoch-1",
        "allocation_authority_source": "SLEEVE_ALLOCATION_EPOCH_V1",
        "enforcement_mode_configured": "SHADOW",
        "enforcement_mode_effective": "SHADOW",
        "risk_unit": "risk_cents",
        "risk_cents": 10000,
        "requested_quantity": 1,
        "exposure_risk_cents": 0,
        "total_risk_cents": 10000,
        "allocation_budget_cents": 0,
        "remaining_budget_cents": 0,
        "enforce_budget": False,
        "would_block_under_enforcement": True,
        "total_risk_exceeds_budget": True,
        "decision_shadow": "ALLOW",
        "enforcement_decision": "ALLOW",
        "reason_codes": ["TOTAL_RISK_EXCEEDS_BUDGET"],
        "source_refs": [],
    }
    _write_json(
        truth_root / "reports" / "intent_budget_validation_v1" / DAY / f"{INTENT_HASH}.intent_budget_validation.v1.json",
        validation,
    )
    monkeypatch.delenv("ENFORCE_BUDGET", raising=False)
    monkeypatch.setenv("ENFORCEMENT_MODE", "SHADOW")
    result = check_budget_enforcement_day_v1(day_utc=DAY, truth_root=truth_root)
    assert result["blocking"] is False
    assert result["effective_mode"] == "SHADOW"


def test_soft_mode_marks_reject_but_does_not_block(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    truth_root = tmp_path / "truth"
    _seed_base_budget_context(truth_root, day_utc=DAY, intent_hash=INTENT_HASH)
    allocation_path = truth_root / "allocation_v1" / "capital_authority_allocation_v1" / DAY / "capital_authority_allocation.v1.json"
    allocation_obj = _load_json(allocation_path)
    allocation_obj["per_sleeve"][0]["headroom_cents"] = 0
    phase2._write_json(allocation_path, allocation_obj)
    _materialize_epoch(truth_root, day_utc=DAY, monkeypatch=monkeypatch)
    monkeypatch.setenv("ENFORCE_BUDGET", "1")
    monkeypatch.setenv("ENFORCEMENT_MODE", "SOFT")
    assert _run_main(authorization_ledger_writer, "--day", DAY, "--truth_root", str(truth_root), monkeypatch=monkeypatch) == 0
    validation = _load_json(
        truth_root / "reports" / "intent_budget_validation_v1" / DAY / f"{INTENT_HASH}.intent_budget_validation.v1.json"
    )
    guard = check_budget_enforcement_day_v1(day_utc=DAY, truth_root=truth_root)
    assert validation["decision_shadow"] == "WOULD_REJECT"
    assert validation["enforcement_decision"] == "REJECT"
    assert guard["blocking"] is False
    assert guard["effective_mode"] == "SOFT"


def test_hard_mode_submit_boundary_would_block(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    truth_root = tmp_path / "truth"
    validation = {
        "schema_id": "C2_INTENT_BUDGET_VALIDATION_V1_UNGOVERNED",
        "schema_version": 1,
        "day_utc": DAY,
        "intent_hash": INTENT_HASH,
        "intent_id": "intent-hard",
        "engine_id": "C2_TREND_EQ_PRIMARY_V1",
        "canonical_sleeve_id": "C2_TREND_EQ_PRIMARY",
        "allocation_epoch_id": "epoch-1",
        "allocation_authority_source": "SLEEVE_ALLOCATION_EPOCH_V1",
        "enforcement_mode_configured": "HARD",
        "enforcement_mode_effective": "HARD",
        "risk_unit": "risk_cents",
        "risk_cents": 10000,
        "requested_quantity": 1,
        "exposure_risk_cents": 0,
        "total_risk_cents": 10000,
        "allocation_budget_cents": 0,
        "remaining_budget_cents": 0,
        "enforce_budget": True,
        "would_block_under_enforcement": True,
        "total_risk_exceeds_budget": True,
        "decision_shadow": "WOULD_REJECT",
        "enforcement_decision": "REJECT",
        "reason_codes": ["TOTAL_RISK_EXCEEDS_BUDGET", "ENFORCEMENT_DECISION_REJECT"],
        "source_refs": [],
    }
    validation_path = truth_root / "reports" / "intent_budget_validation_v1" / DAY / f"{INTENT_HASH}.intent_budget_validation.v1.json"
    _write_json(validation_path, validation)

    def _surface_ref(relpath: str, payload: dict) -> SimpleNamespace:
        path = truth_root / relpath
        _write_json(path, payload)
        return SimpleNamespace(path=path, payload=payload)

    monkeypatch.setenv("ENFORCE_BUDGET", "1")
    monkeypatch.setenv("ENFORCEMENT_MODE", "HARD")
    monkeypatch.setattr(submit_boundary_writer, "require_authoritative_repo_runtime_v1", lambda repo_root: None)
    monkeypatch.setattr(submit_boundary_writer, "resolve_decision_truth_root_v1", lambda raw, repo_root=None: Path(str(raw)).resolve())
    monkeypatch.setattr(submit_boundary_writer, "resolve_single_paper_ib_account_from_sleeve_registry", lambda repo_root: "DU1234567")
    monkeypatch.setattr(
        submit_boundary_writer,
        "resolve_sleeve_execution_root_v1",
        lambda **kwargs: SimpleNamespace(execution_root_path=(truth_root / "execution")),
    )
    monkeypatch.setattr(
        submit_boundary_writer,
        "read_target_day_build_ref_v1",
        lambda **kwargs: _surface_ref(
            "reports/target_day_build_v1/2026-04-14/target_day_build.v1.json",
            {
                "build_status": "COMPLETE",
                "completeness_result": "COMPLETE",
                "closure_status": "CLOSED",
                "hidden_dependency_check_result": {"status": "PASS"},
                "blocker_chain": [],
            },
        ),
    )
    monkeypatch.setattr(
        submit_boundary_writer,
        "read_target_day_admission_ref_v1",
        lambda **kwargs: _surface_ref(
            "reports/target_day_admission_v1/2026-04-14/target_day_admission.v1.json",
            {"admission_status": "ADMIT", "binding": True, "blocking_reason_codes": []},
        ),
    )
    monkeypatch.setattr(
        submit_boundary_writer,
        "read_startup_materialization_ref_v1",
        lambda **kwargs: _surface_ref(
            "reports/startup_materialization_v1/2026-04-14/startup_materialization.v1.json",
            {"status": "SUCCESS", "blocking_codes": []},
        ),
    )
    monkeypatch.setattr(
        submit_boundary_writer,
        "read_paper_trading_posture_ref_v1",
        lambda **kwargs: _surface_ref(
            "reports/paper_trading_posture_v1/2026-04-14/paper_trading_posture.v1.json",
            {"system_ready": True, "posture_status": "ENABLED", "blocking_codes": []},
        ),
    )
    monkeypatch.setattr(submit_boundary_writer, "_refresh_trade_submit_readiness_artifact_v1", lambda **kwargs: 0)
    control_path = truth_root / "runtime_control_kernel_v1" / "records" / DAY / "PAPER" / "DU1234567" / "runtime_control_record.v1.json"
    _write_json(control_path, {"control_state": "ALLOW"})
    monkeypatch.setattr(
        submit_boundary_writer,
        "run_runtime_control_kernel_v1",
        lambda **kwargs: {
            "runtime_control_decision": SimpleNamespace(reason_codes=[]),
            "runtime_control_record": SimpleNamespace(control_state="ALLOW"),
            "runtime_control_record_path": control_path,
            "runtime_control_decision_path": control_path,
        },
    )

    def _fake_atomic_write(*, path: Path, payload: dict, schema_relpath: str, volatile_field_names: tuple[str, ...]) -> SimpleNamespace:
        raw = json.dumps(payload, sort_keys=True).encode("utf-8")
        _write_json(path, payload)
        return SimpleNamespace(path=path, sha256=hashlib.sha256(raw).hexdigest())

    monkeypatch.setattr(submit_boundary_writer, "atomic_write_idempotent_validated_json_v1", _fake_atomic_write)
    rc = submit_boundary_writer.main(["--day_utc", DAY, "--truth_root", str(truth_root)])
    payload = _load_json(truth_root / "reports" / "submit_boundary_status_v1" / DAY / "submit_boundary_status.v1.json")
    assert rc == 2
    assert payload["submission_authorized"] is False
    assert payload["boundary_status"] == "BLOCKED"
    assert "ENFORCEMENT_DECISION_REJECT" in payload["blocking_codes"]
    assert "budget_enforcement_guard_v1" in {row["logical_name"] for row in payload["required_boundary_checks"]}


def test_exposure_plus_new_risk_exceeds_budget_is_detected(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    truth_root = tmp_path / "truth"
    _seed_base_budget_context(truth_root, day_utc=DAY_EXPOSURE, intent_hash=INTENT_HASH_EXPOSURE)
    phase2._seed_authorization_artifact(truth_root, day_utc=DAY_EXPOSURE, intent_hash=INTENT_HASH, status="AUTHORIZED")
    phase2._seed_equity_order_plan(truth_root, day_utc=DAY_EXPOSURE, intent_hash=INTENT_HASH)
    allocation_path = truth_root / "allocation_v1" / "capital_authority_allocation_v1" / DAY_EXPOSURE / "capital_authority_allocation.v1.json"
    allocation_obj = _load_json(allocation_path)
    allocation_obj["per_sleeve"][0]["allowed_capital_at_risk_cents"] = 15000
    allocation_obj["per_sleeve"][0]["used_capital_at_risk_cents"] = 0
    allocation_obj["per_sleeve"][0]["headroom_cents"] = 15000
    allocation_obj["portfolio"]["allowed_capital_at_risk_cents"] = 15000
    allocation_obj["portfolio"]["used_capital_at_risk_cents"] = 0
    allocation_obj["portfolio"]["headroom_cents"] = 15000
    phase2._write_json(allocation_path, allocation_obj)
    _materialize_epoch(truth_root, day_utc=DAY_EXPOSURE, monkeypatch=monkeypatch)
    assert _run_main(authorization_ledger_writer, "--day", DAY_EXPOSURE, "--truth_root", str(truth_root), monkeypatch=monkeypatch) == 0
    positions_path = phase2._seed_positions_snapshot(truth_root, day_utc=DAY_EXPOSURE, binding_hash=BINDING_HASH)
    positions_obj = _load_json(positions_path)
    positions_obj["positions"]["items"][0]["qty"] = 1
    phase2._write_json(positions_path, positions_obj)
    phase2._seed_fill_ledger(truth_root, day_utc=DAY_EXPOSURE, binding_hash=BINDING_HASH, intent_hash=INTENT_HASH_EXPOSURE)
    assert _run_main(exposure_ledger_writer, "--day", DAY_EXPOSURE, "--truth_root", str(truth_root), monkeypatch=monkeypatch) == 0
    monkeypatch.setenv("ENFORCE_BUDGET", "1")
    monkeypatch.setenv("ENFORCEMENT_MODE", "SOFT")
    assert _run_main(authorization_ledger_writer, "--day", DAY_EXPOSURE, "--truth_root", str(truth_root), monkeypatch=monkeypatch) == 0
    validation = _load_json(
        truth_root / "reports" / "intent_budget_validation_v1" / DAY_EXPOSURE / f"{INTENT_HASH}.intent_budget_validation.v1.json"
    )
    assert validation["exposure_risk_cents"] == 10000
    assert validation["total_risk_cents"] == 20000
    assert validation["allocation_budget_cents"] == 15000
    assert validation["total_risk_exceeds_budget"] is True
    assert "TOTAL_RISK_EXCEEDS_BUDGET" in validation["reason_codes"]


def test_reconciliation_emits_enforcement_recommendation(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    truth_root = tmp_path / "truth"
    _seed_base_budget_context(truth_root, day_utc=DAY_EXPOSURE, intent_hash=INTENT_HASH_EXPOSURE)
    phase2._seed_authorization_artifact(truth_root, day_utc=DAY_EXPOSURE, intent_hash=INTENT_HASH, status="AUTHORIZED")
    phase2._seed_equity_order_plan(truth_root, day_utc=DAY_EXPOSURE, intent_hash=INTENT_HASH)
    allocation_path = truth_root / "allocation_v1" / "capital_authority_allocation_v1" / DAY_EXPOSURE / "capital_authority_allocation.v1.json"
    allocation_obj = _load_json(allocation_path)
    allocation_obj["per_sleeve"][0]["allowed_capital_at_risk_cents"] = 15000
    allocation_obj["per_sleeve"][0]["used_capital_at_risk_cents"] = 0
    allocation_obj["per_sleeve"][0]["headroom_cents"] = 15000
    allocation_obj["portfolio"]["allowed_capital_at_risk_cents"] = 15000
    allocation_obj["portfolio"]["used_capital_at_risk_cents"] = 0
    allocation_obj["portfolio"]["headroom_cents"] = 15000
    phase2._write_json(allocation_path, allocation_obj)
    _materialize_epoch(truth_root, day_utc=DAY_EXPOSURE, monkeypatch=monkeypatch)
    assert _run_main(authorization_ledger_writer, "--day", DAY_EXPOSURE, "--truth_root", str(truth_root), monkeypatch=monkeypatch) == 0
    positions_path = phase2._seed_positions_snapshot(truth_root, day_utc=DAY_EXPOSURE, binding_hash=BINDING_HASH)
    positions_obj = _load_json(positions_path)
    positions_obj["positions"]["items"][0]["qty"] = 1
    phase2._write_json(positions_path, positions_obj)
    phase2._seed_fill_ledger(truth_root, day_utc=DAY_EXPOSURE, binding_hash=BINDING_HASH, intent_hash=INTENT_HASH_EXPOSURE)
    assert _run_main(exposure_ledger_writer, "--day", DAY_EXPOSURE, "--truth_root", str(truth_root), monkeypatch=monkeypatch) == 0
    monkeypatch.setenv("ENFORCE_BUDGET", "1")
    monkeypatch.setenv("ENFORCEMENT_MODE", "SOFT")
    assert _run_main(authorization_ledger_writer, "--day", DAY_EXPOSURE, "--truth_root", str(truth_root), monkeypatch=monkeypatch) == 0
    assert _run_main(epoch_reconciliation_writer, "--day", DAY_EXPOSURE, "--truth_root", str(truth_root), monkeypatch=monkeypatch) == 0
    report = _load_json(
        truth_root / "reports" / "epoch_ledger_reconciliation_v1" / DAY_EXPOSURE / "epoch_ledger_reconciliation.v1.json"
    )
    assert report["status"] == "HARD_VIOLATION"
    assert report["enforcement_recommendation"] == "BLOCK"
    assert "TOTAL_RISK_EXCEEDS_BUDGET" in report["reason_codes"]
