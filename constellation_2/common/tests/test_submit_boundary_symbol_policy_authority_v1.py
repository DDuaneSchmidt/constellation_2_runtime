from __future__ import annotations

import json
from pathlib import Path

import pytest

from constellation_2.common.paper_execution_authority_v1 import (
    RC_EXECUTION_ROOT_GLOBAL_REFERENCE_FORBIDDEN,
    RC_EXECUTION_ROOT_SLEEVE_ID_MISSING,
    require_governed_execution_family_path,
    resolve_governed_paper_execution_roots,
)
from constellation_2.common.execution_identity_binding_v1 import (
    RC_EXECUTION_IDENTITY_ACCOUNT_MISMATCH,
    RC_EXECUTION_IDENTITY_CLIENT_ID_ORDERS_MISMATCH,
)
from constellation_2.phaseD.lib.submit_boundary_paper_v4 import (
    RC_ENGINE_SYMBOL_NOT_ALLOWED,
    SubmitBoundaryV4Error,
    _enforce_engine_symbol_policy,
    _enforce_ib_account_registry,
    _resolve_submit_scope_from_phasec_out_dir,
    run_submit_boundary_paper_v4,
)


def _write_json(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def _seed_repo(
    repo_root: Path,
    *,
    account_allowed_symbols,
    engine_allowed_symbols,
) -> None:
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
                    "allowed_symbols": account_allowed_symbols,
                    "allowed_sleeve_ids": ["PRIMARY"],
                    "notes": [],
                }
            ],
        },
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
                    "execution_mode": "AUTO",
                    "status": "PRODUCTION",
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
        repo_root / "governance/02_REGISTRIES/ENGINE_MODEL_REGISTRY_V1.json",
        {
            "schema_id": "engine_model_registry",
            "schema_version": "v1",
            "engines": [
                {
                    "engine_id": "C2_MEAN_REVERSION_EQ_V1",
                    "activation_status": "ACTIVE",
                    "allowed_symbols": engine_allowed_symbols,
                }
            ],
        },
    )


def _install_minimal_execution_package_mocks(
    monkeypatch: pytest.MonkeyPatch,
    *,
    phasec_out_dir: Path,
    day_utc: str,
) -> Path:
    fake_package_path = (phasec_out_dir / "execution_package.v1.json").resolve()
    fake_build_path = (phasec_out_dir / "execution_build.v1.json").resolve()

    def _fake_read_execution_package(repo_root: Path, execution_package_path: Path):
        assert execution_package_path.resolve() == fake_package_path
        return (
            {
                "schema_id": "execution_package",
                "schema_version": "v1",
                "day_utc": day_utc,
                "candidate_ref": {"phasec_out_dir": str(phasec_out_dir.resolve())},
            },
            fake_build_path,
        )

    monkeypatch.setattr(
        "constellation_2.phaseD.lib.submit_boundary_paper_v4._read_execution_package",
        _fake_read_execution_package,
    )
    monkeypatch.setattr(
        "constellation_2.phaseD.lib.submit_boundary_paper_v4._load_and_enforce_submission_record",
        lambda **_: None,
    )
    return fake_package_path


def test_execution_root_resolution_rejects_missing_sleeve_id(tmp_path: Path) -> None:
    _seed_repo(
        tmp_path,
        account_allowed_symbols=None,
        engine_allowed_symbols=["SPY"],
    )
    with pytest.raises(ValueError, match=RC_EXECUTION_ROOT_SLEEVE_ID_MISSING):
        resolve_governed_paper_execution_roots(
            repo_root=tmp_path,
            environment="PAPER",
            ib_account="DUO847203",
            sleeve_id="",
        )


def test_execution_root_family_rejects_global_root(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    _seed_repo(
        tmp_path,
        account_allowed_symbols=None,
        engine_allowed_symbols=["SPY"],
    )
    truth_sleeves_root = tmp_path / "runtime" / "truth_sleeves"
    canonical_truth_root = tmp_path / "runtime" / "truth"
    (truth_sleeves_root / "PRIMARY" / "PAPER").mkdir(parents=True)
    canonical_truth_root.mkdir(parents=True)

    import constellation_2.common.sleeve_execution_root_v1 as sleeve_root_module
    import constellation_2.common.trade_submit_readiness_authority_v1 as readiness_authority_module

    monkeypatch.setattr(sleeve_root_module, "resolve_truth_sleeves_root", lambda: truth_sleeves_root)
    monkeypatch.setattr(sleeve_root_module, "resolve_canonical_truth_root", lambda: canonical_truth_root)
    monkeypatch.setattr(readiness_authority_module, "resolve_truth_sleeves_root", lambda: truth_sleeves_root)

    with pytest.raises(ValueError, match=RC_EXECUTION_ROOT_GLOBAL_REFERENCE_FORBIDDEN):
        require_governed_execution_family_path(
            repo_root=tmp_path,
            environment="PAPER",
            ib_account="DUO847203",
            sleeve_id="PRIMARY",
            family="phaseC_preflight_v1",
            actual_path=canonical_truth_root / "phaseC_preflight_v1" / "2026-04-14",
        )


def test_submit_boundary_never_reaches_broker_connect_when_execution_root_validation_fails(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _seed_repo(
        tmp_path,
        account_allowed_symbols=None,
        engine_allowed_symbols=["SPY"],
    )
    called = {"connect": False}
    phasec_root = tmp_path / "runtime" / "truth_sleeves"
    phasec_out_dir = phasec_root / "PRIMARY" / "PAPER" / "phaseC_preflight_v1" / "2026-04-14" / "attempt_A0001"
    phasec_out_dir.mkdir(parents=True, exist_ok=True)

    def _fail_connect(self) -> None:
        called["connect"] = True

    monkeypatch.setattr(
        "constellation_2.phaseD.lib.submit_boundary_paper_v4.resolve_truth_sleeves_root",
        lambda: phasec_root,
    )
    monkeypatch.setattr(
        "constellation_2.phaseD.lib.submit_boundary_paper_v4._resolve_execution_roots_for_phasec_out_dir",
        lambda **_: (_ for _ in ()).throw(SubmitBoundaryV4Error("EXECUTION_ROOT_PATH_MISMATCH:test")),
    )
    monkeypatch.setattr(
        "constellation_2.phaseD.adapters.ib_paper_adapter_v2.IBPaperAdapterV2.connect",
        _fail_connect,
    )
    fake_package_path = _install_minimal_execution_package_mocks(
        monkeypatch,
        phasec_out_dir=phasec_out_dir,
        day_utc="2026-04-14",
    )

    with pytest.raises(SubmitBoundaryV4Error, match="EXECUTION_ROOT_PATH_MISMATCH"):
        run_submit_boundary_paper_v4(
            repo_root=tmp_path,
            eval_time_utc="2026-04-14T12:00:00Z",
            phasec_out_dir=phasec_out_dir,
            execution_package_path=fake_package_path,
            allow_legacy_raw_candidate=True,
            risk_budget_path=tmp_path / "risk_budget.json",
            ib_host="127.0.0.1",
            ib_port=4002,
            ib_client_id=7,
            ib_account="DUO847203",
            dry_run=True,
        )

    assert called["connect"] is False


def test_submit_scope_resolution_uses_truth_sleeves_root_helper(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    phasec_root = tmp_path / "runtime" / "truth_sleeves"
    phasec_out_dir = phasec_root / "PRIMARY" / "PAPER" / "phaseC_preflight_v1" / "2026-04-14" / "attempt_A0001"
    phasec_out_dir.mkdir(parents=True, exist_ok=True)

    monkeypatch.setattr(
        "constellation_2.phaseD.lib.submit_boundary_paper_v4.resolve_truth_sleeves_root",
        lambda: phasec_root,
    )

    sleeve_id, mode = _resolve_submit_scope_from_phasec_out_dir(phasec_out_dir)

    assert sleeve_id == "PRIMARY"
    assert mode == "PAPER"


def test_submit_boundary_never_reaches_broker_connect_when_execution_identity_account_mismatches(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _seed_repo(
        tmp_path,
        account_allowed_symbols=None,
        engine_allowed_symbols=["SPY"],
    )
    called = {"connect": False}
    phasec_root = tmp_path / "runtime" / "truth_sleeves"
    phasec_out_dir = phasec_root / "PRIMARY" / "PAPER" / "phaseC_preflight_v1" / "2026-04-14" / "attempt_A0001"
    phasec_out_dir.mkdir(parents=True, exist_ok=True)

    def _fail_connect(self) -> None:
        called["connect"] = True

    monkeypatch.setattr(
        "constellation_2.phaseD.lib.submit_boundary_paper_v4.resolve_truth_sleeves_root",
        lambda: phasec_root,
    )
    monkeypatch.setattr(
        "constellation_2.phaseD.adapters.ib_paper_adapter_v2.IBPaperAdapterV2.connect",
        _fail_connect,
    )
    fake_package_path = _install_minimal_execution_package_mocks(
        monkeypatch,
        phasec_out_dir=phasec_out_dir,
        day_utc="2026-04-14",
    )

    with pytest.raises(SubmitBoundaryV4Error, match=RC_EXECUTION_IDENTITY_ACCOUNT_MISMATCH):
        run_submit_boundary_paper_v4(
            repo_root=tmp_path,
            eval_time_utc="2026-04-14T12:00:00Z",
            phasec_out_dir=phasec_out_dir,
            execution_package_path=fake_package_path,
            allow_legacy_raw_candidate=True,
            risk_budget_path=tmp_path / "risk_budget.json",
            ib_host="127.0.0.1",
            ib_port=4002,
            ib_client_id=7,
            ib_account="DUO999999",
            dry_run=True,
        )

    assert called["connect"] is False


def test_submit_boundary_never_reaches_broker_connect_when_execution_identity_client_id_mismatches(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _seed_repo(
        tmp_path,
        account_allowed_symbols=None,
        engine_allowed_symbols=["SPY"],
    )
    called = {"connect": False}
    phasec_root = tmp_path / "runtime" / "truth_sleeves"
    phasec_out_dir = phasec_root / "PRIMARY" / "PAPER" / "phaseC_preflight_v1" / "2026-04-14" / "attempt_A0001"
    phasec_out_dir.mkdir(parents=True, exist_ok=True)

    def _fail_connect(self) -> None:
        called["connect"] = True

    monkeypatch.setattr(
        "constellation_2.phaseD.lib.submit_boundary_paper_v4.resolve_truth_sleeves_root",
        lambda: phasec_root,
    )
    monkeypatch.setattr(
        "constellation_2.phaseD.adapters.ib_paper_adapter_v2.IBPaperAdapterV2.connect",
        _fail_connect,
    )
    fake_package_path = _install_minimal_execution_package_mocks(
        monkeypatch,
        phasec_out_dir=phasec_out_dir,
        day_utc="2026-04-14",
    )

    with pytest.raises(SubmitBoundaryV4Error, match=RC_EXECUTION_IDENTITY_CLIENT_ID_ORDERS_MISMATCH):
        run_submit_boundary_paper_v4(
            repo_root=tmp_path,
            eval_time_utc="2026-04-14T12:00:00Z",
            phasec_out_dir=phasec_out_dir,
            execution_package_path=fake_package_path,
            allow_legacy_raw_candidate=True,
            risk_budget_path=tmp_path / "risk_budget.json",
            ib_host="127.0.0.1",
            ib_port=4002,
            ib_client_id=8,
            ib_account="DUO847203",
            dry_run=True,
        )

    assert called["connect"] is False


def test_account_registry_allowed_symbols_no_longer_blocks_symbol_policy(tmp_path: Path) -> None:
    _seed_repo(
        tmp_path,
        account_allowed_symbols=["SPY"],
        engine_allowed_symbols=["QQQ"],
    )

    pointers: list[str] = []
    _enforce_ib_account_registry(
        repo_root=tmp_path,
        ib_account="DUO847203",
        engine_id="C2_MEAN_REVERSION_EQ_V1",
        pointers=pointers,
    )
    _enforce_engine_symbol_policy(
        repo_root=tmp_path,
        engine_id="C2_MEAN_REVERSION_EQ_V1",
        plan_symbol="QQQ",
        pointers=pointers,
    )

    assert any(path.endswith("C2_IB_ACCOUNT_REGISTRY_V1.json") for path in pointers)
    assert any(path.endswith("ENGINE_MODEL_REGISTRY_V1.json") for path in pointers)


def test_engine_symbol_policy_blocks_symbol_outside_engine_universe(tmp_path: Path) -> None:
    _seed_repo(
        tmp_path,
        account_allowed_symbols=None,
        engine_allowed_symbols=["QQQ"],
    )

    with pytest.raises(SubmitBoundaryV4Error, match=RC_ENGINE_SYMBOL_NOT_ALLOWED):
        _enforce_engine_symbol_policy(
            repo_root=tmp_path,
            engine_id="C2_MEAN_REVERSION_EQ_V1",
            plan_symbol="SPY",
            pointers=[],
        )


def test_engine_symbol_policy_allows_any_symbol_when_engine_universe_is_null(tmp_path: Path) -> None:
    _seed_repo(
        tmp_path,
        account_allowed_symbols=None,
        engine_allowed_symbols=None,
    )

    _enforce_engine_symbol_policy(
        repo_root=tmp_path,
        engine_id="C2_MEAN_REVERSION_EQ_V1",
        plan_symbol="GLD",
        pointers=[],
    )


def test_engine_symbol_policy_uses_current_universe_and_ignores_legacy_registry_list(tmp_path: Path) -> None:
    _seed_repo(
        tmp_path,
        account_allowed_symbols=None,
        engine_allowed_symbols=["IWM"],
    )
    execution_truth_root = tmp_path / "runtime" / "truth_sleeves" / "PRIMARY" / "PAPER"
    ranked_path = (
        execution_truth_root
        / "reports"
        / "ranked_symbol_universe_v1"
        / "2026-04-24"
        / "ranked_symbol_universe.v1.json"
    )
    _write_json(
        ranked_path,
        {
            "schema_id": "ranked_symbol_universe",
            "schema_version": "v1",
            "day_utc": "2026-04-24",
            "status": "PASS",
            "symbols": ["SPY"],
        },
    )

    pointers: list[str] = []
    _enforce_engine_symbol_policy(
        repo_root=tmp_path,
        engine_id="C2_MEAN_REVERSION_EQ_V1",
        plan_symbol="SPY",
        pointers=pointers,
        execution_truth_root=execution_truth_root,
        day_utc="2026-04-24",
    )

    assert str(ranked_path.resolve()) in pointers
    assert not any(path.endswith("ENGINE_MODEL_REGISTRY_V1.json") for path in pointers)
