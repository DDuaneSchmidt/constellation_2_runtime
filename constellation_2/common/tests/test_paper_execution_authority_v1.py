from __future__ import annotations

import json
import sys
from pathlib import Path

SOURCE_ROOT = Path("/home/node/constellation")
if str(SOURCE_ROOT) not in sys.path:
    sys.path.insert(0, str(SOURCE_ROOT))

from constellation_2.common.paper_execution_authority_v1 import (  # noqa: E402
    EXECUTION_ROOT_AUTHORITY_OWNER,
    RC_EXECUTION_ROOT_GLOBAL_REFERENCE_FORBIDDEN,
    GovernedPaperExecutionProfile,
    require_governed_execution_family_path,
    resolve_governed_paper_execution_profile,
    resolve_governed_paper_execution_roots,
)
import ops.tools.run_c2_paper_day_orchestrator_v2 as orchestrator_module  # noqa: E402
import pytest  # noqa: E402


def _write_json(path: Path, payload: object) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    import json

    path.write_text(json.dumps(payload, sort_keys=True, indent=2) + "\n", encoding="utf-8")


def _seed_registries(repo_root: Path) -> None:
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
                    "allowed_engine_ids": ["C2_TREND_EQ_PRIMARY_V1"],
                    "allowed_sleeve_ids": ["PRIMARY"],
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
                        "client_id_market_data": 1101,
                        "client_id_orders": 7,
                        "client_id_observer": 179,
                    },
                }
            ],
        },
    )


def test_resolve_governed_paper_execution_roots_returns_single_canonical_sleeve_root(
    tmp_path: Path,
    monkeypatch,
) -> None:
    repo_root = tmp_path / "repo"
    canonical_truth_root = tmp_path / "runtime" / "truth"
    truth_sleeves_root = tmp_path / "runtime" / "truth_sleeves"
    (truth_sleeves_root / "PRIMARY" / "PAPER").mkdir(parents=True)
    canonical_truth_root.mkdir(parents=True)
    _seed_registries(repo_root)

    import constellation_2.common.paper_execution_authority_v1 as authority_module
    import constellation_2.common.sleeve_execution_root_v1 as sleeve_root_module
    import constellation_2.common.trade_submit_readiness_authority_v1 as readiness_authority_module

    monkeypatch.setattr(sleeve_root_module, "resolve_canonical_truth_root", lambda: canonical_truth_root)
    monkeypatch.setattr(sleeve_root_module, "resolve_truth_sleeves_root", lambda: truth_sleeves_root)
    monkeypatch.setattr(readiness_authority_module, "resolve_truth_sleeves_root", lambda: truth_sleeves_root)

    roots = resolve_governed_paper_execution_roots(
        repo_root=repo_root,
        environment="PAPER",
        ib_account="DUO847203",
        sleeve_id="PRIMARY",
    )

    assert roots.authority_owner == EXECUTION_ROOT_AUTHORITY_OWNER
    assert roots.execution_root_path == (truth_sleeves_root / "PRIMARY" / "PAPER").resolve()
    assert roots.sleeve_id == "PRIMARY"
    assert roots.mode == "PAPER"


def test_require_governed_execution_family_path_rejects_global_truth_reference(tmp_path: Path, monkeypatch) -> None:
    repo_root = tmp_path / "repo"
    canonical_truth_root = tmp_path / "runtime" / "truth"
    truth_sleeves_root = tmp_path / "runtime" / "truth_sleeves"
    (truth_sleeves_root / "PRIMARY" / "PAPER").mkdir(parents=True)
    canonical_truth_root.mkdir(parents=True)
    _seed_registries(repo_root)

    import constellation_2.common.sleeve_execution_root_v1 as sleeve_root_module
    import constellation_2.common.trade_submit_readiness_authority_v1 as readiness_authority_module

    monkeypatch.setattr(sleeve_root_module, "resolve_canonical_truth_root", lambda: canonical_truth_root)
    monkeypatch.setattr(sleeve_root_module, "resolve_truth_sleeves_root", lambda: truth_sleeves_root)
    monkeypatch.setattr(readiness_authority_module, "resolve_truth_sleeves_root", lambda: truth_sleeves_root)

    with pytest.raises(ValueError, match=RC_EXECUTION_ROOT_GLOBAL_REFERENCE_FORBIDDEN):
        require_governed_execution_family_path(
            repo_root=repo_root,
            environment="PAPER",
            ib_account="DUO847203",
            sleeve_id="PRIMARY",
            family="phaseC_preflight_v1",
            actual_path=canonical_truth_root / "phaseC_preflight_v1" / "2026-04-14",
        )


def test_resolve_governed_paper_execution_profile_reads_sleeve_registry(tmp_path: Path, monkeypatch) -> None:
    repo_root = tmp_path / "repo"
    truth_sleeves_root = tmp_path / "runtime" / "truth_sleeves"
    (truth_sleeves_root / "PRIMARY" / "PAPER").mkdir(parents=True)
    _seed_registries(repo_root)

    import constellation_2.common.trade_submit_readiness_authority_v1 as readiness_authority_module

    monkeypatch.setattr(readiness_authority_module, "resolve_truth_sleeves_root", lambda: truth_sleeves_root)

    profile = resolve_governed_paper_execution_profile(
        repo_root=repo_root,
        environment="PAPER",
        ib_account="DUO847203",
        sleeve_id="PRIMARY",
    )

    assert profile.host == "127.0.0.1"
    assert profile.port == 4002
    assert profile.client_id_orders == 7
    assert profile.client_id_observer == 179


def test_orchestrator_module_exposes_git_sha_contract() -> None:
    value = orchestrator_module._git_sha()
    assert isinstance(value, str)
    assert value
    assert all(ch in "0123456789abcdef" for ch in value.lower())
    assert len(value) in {7, 40}


def test_sleeve_edge_core2_summary_ready_guard(tmp_path: Path) -> None:
    truth_root = tmp_path / "truth"
    day = "2026-04-23"

    ready, reason = orchestrator_module._sleeve_edge_core2_summary_ready(truth_root, day)
    assert ready is False
    assert reason == "SKIP_SLEEVE_EDGE_CORE2_SUMMARY_MISSING"

    summary_path = (
        truth_root
        / "reports"
        / "reconciled_trade_state_summary_v1"
        / day
        / "set_a"
        / "reconciled_trade_state_summary.v1.json"
    )
    _write_json(summary_path, {"trade_refs": []})
    ready, reason = orchestrator_module._sleeve_edge_core2_summary_ready(truth_root, day)
    assert ready is False
    assert reason == "SKIP_SLEEVE_EDGE_CORE2_SUMMARY_EMPTY"

    _write_json(summary_path, {"trade_refs": [{"trade_id": "t1"}]})
    ready, reason = orchestrator_module._sleeve_edge_core2_summary_ready(truth_root, day)
    assert ready is True
    assert reason == ""


def test_orchestrator_governed_submit_cmd_uses_execution_profile(monkeypatch, tmp_path: Path) -> None:
    monkeypatch.setattr(
        orchestrator_module,
        "resolve_governed_paper_execution_roots",
        lambda **_: type("Roots", (), {"execution_root_path": tmp_path / "truth_sleeves" / "PRIMARY" / "PAPER"})(),
    )
    monkeypatch.setattr(
        orchestrator_module,
        "resolve_governed_paper_execution_profile",
        lambda **_: GovernedPaperExecutionProfile(
            environment="PAPER",
            ib_account="DUO847203",
            sleeve_id="PRIMARY",
            host="127.0.0.1",
            port=4002,
            client_id_orders=7,
            client_id_observer=179,
            sleeve_registry_path=tmp_path / "governance/02_REGISTRIES/C2_SLEEVE_REGISTRY_V1.json",
        ),
    )

    materializer_cmd, _ = orchestrator_module._build_phasec_materializer_cmd(
        truth_root=tmp_path / "truth",
        day="2026-04-14",
        produced_utc="2026-04-14T00:00:00Z",
        ib_account="DUO847203",
    )

    assert "--execution_truth_root" in materializer_cmd
    assert materializer_cmd[materializer_cmd.index("--execution_truth_root") + 1] == str(
        tmp_path / "truth_sleeves" / "PRIMARY" / "PAPER"
    )

    cmd = orchestrator_module._build_governed_submit_cmd(
        execution_package_path=tmp_path / "truth_sleeves" / "PRIMARY" / "PAPER" / "execution_package_v1" / "2026-04-14" / ("a" * 64) / "execution_package.v1.json",
        submission_record_path=tmp_path / "truth_sleeves" / "PRIMARY" / "PAPER" / "execution_kernel_v1" / "submission_records" / "2026-04-14" / ("a" * 64) / "submission_record.v1.json",
        day="2026-04-14",
        produced_utc="2026-04-14T00:00:00Z",
        ib_account="DUO847203",
        env={},
    )

    assert "--ib_host" in cmd
    assert cmd[cmd.index("--ib_host") + 1] == "127.0.0.1"
    assert cmd[cmd.index("--ib_port") + 1] == "4002"
    assert cmd[cmd.index("--ib_client_id") + 1] == "7"
    assert "--execution_package_path" in cmd
    assert "--submission_record_path" in cmd
    assert "--phasec_out_dir" not in cmd


def test_orchestrator_discovers_same_day_identity_dirs_under_execution_root(monkeypatch, tmp_path: Path) -> None:
    canonical_truth_root = tmp_path / "truth"
    execution_truth_root = tmp_path / "truth_sleeves" / "PRIMARY" / "PAPER"
    identity_dir = execution_truth_root / "phaseC_preflight_v1" / "2026-04-14" / "attempt_A0001" / "intent_hash"
    identity_dir.mkdir(parents=True)
    for filename in ("equity_order_plan.v2.json", "mapping_ledger_record.v2.json", "binding_record.v2.json"):
        (identity_dir / filename).write_text("{}\n", encoding="utf-8")

    monkeypatch.setattr(
        orchestrator_module,
        "resolve_governed_paper_execution_roots",
        lambda **_: type("Roots", (), {"execution_root_path": execution_truth_root})(),
    )

    identity_dirs = orchestrator_module._discover_same_day_identity_dirs(
        canonical_truth_root,
        "2026-04-14",
        "DUO847203",
    )

    assert identity_dirs == [identity_dir.resolve()]


def test_emit_governed_submit_skip_veto_writes_submission_veto_evidence(tmp_path: Path) -> None:
    truth_root = (tmp_path / "truth").resolve()
    day = "2026-04-14"
    identity_dir = (
        tmp_path
        / "truth_sleeves"
        / "PRIMARY"
        / "PAPER"
        / "phaseC_preflight_v1"
        / day
        / "attempt_A0001"
        / ("b" * 64)
    ).resolve()
    identity_dir.mkdir(parents=True, exist_ok=True)
    submission_id = "f" * 64
    _write_json(
        identity_dir / "equity_order_plan.v2.json",
        {
            "schema_id": "equity_order_plan",
            "schema_version": "v2",
            "intent_hash": "2" * 64,
            "plan_hash": "3" * 64,
            "source_intent_id": "c2_trend_eq_spy_2026-04-14_v1",
        },
    )
    _write_json(identity_dir / "mapping_ledger_record.v2.json", {"schema_id": "mapping_ledger_record", "schema_version": "v2"})
    _write_json(identity_dir / "binding_record.v2.json", {"schema_id": "binding_record", "schema_version": "v2", "submission_id": submission_id})

    emitted, reason_code = orchestrator_module._emit_governed_submit_skip_veto(
        truth_root=truth_root,
        day=day,
        eval_time_utc=f"{day}T00:00:00Z",
        identity_dir=identity_dir,
        reason_code="GOV_SUBMIT_AUTHZ_NOT_AUTHORIZED",
        reason_detail="status=REJECTED",
        pointer_paths=[str(identity_dir)],
    )

    veto_path = (
        truth_root
        / "execution_evidence_v1"
        / "submissions"
        / day
        / submission_id
        / "veto_record.v1.json"
    ).resolve()
    assert emitted is True
    assert "GOV_SUBMIT_SKIP_EVIDENCE_" in reason_code
    assert veto_path.exists()
    veto_obj = json.loads(veto_path.read_text(encoding="utf-8"))
    assert veto_obj["reason_code"] == "GOV_SUBMIT_AUTHZ_NOT_AUTHORIZED"


def test_ensure_governed_submit_inputs_materializes_execution_build_when_missing(monkeypatch, tmp_path: Path) -> None:
    truth_root = (tmp_path / "truth_sleeves" / "PRIMARY" / "PAPER").resolve()
    day = "2026-04-14"
    identity_dir = (
        truth_root
        / "phaseC_preflight_v1"
        / day
        / "attempt_A0001"
        / ("c" * 64)
    ).resolve()
    identity_dir.mkdir(parents=True, exist_ok=True)
    submission_id = "e" * 64
    _write_json(
        identity_dir / "equity_order_plan.v2.json",
        {
            "schema_id": "equity_order_plan",
            "schema_version": "v2",
            "intent_hash": "2" * 64,
            "plan_hash": "3" * 64,
            "source_intent_id": "c2_trend_eq_spy_2026-04-14_v1",
        },
    )
    _write_json(identity_dir / "mapping_ledger_record.v2.json", {"schema_id": "mapping_ledger_record", "schema_version": "v2"})
    _write_json(identity_dir / "binding_record.v2.json", {"schema_id": "binding_record", "schema_version": "v2", "submission_id": submission_id})

    package_path = (
        truth_root / "execution_package_v1" / day / submission_id / "execution_package.v1.json"
    ).resolve()
    submission_record_path = (
        truth_root / "execution_kernel_v1" / "submission_records" / day / submission_id / "submission_record.v1.json"
    ).resolve()

    def _fake_materialize(*, identity_dir: Path, env: dict[str, str]) -> int:
        _write_json(package_path, {"schema_id": "execution_package", "schema_version": "v1"})
        return 0

    writer_calls: list[dict[str, object]] = []

    def _fake_write_submission_record_from_package(**kwargs):
        writer_calls.append(dict(kwargs))
        _write_json(submission_record_path, {"schema_id": "execution_submission_record", "schema_version": "v1"})
        return object(), str(submission_record_path), "WROTE"

    monkeypatch.setattr(orchestrator_module, "_run_execution_build_authority_for_identity", _fake_materialize)
    monkeypatch.setattr(
        orchestrator_module,
        "write_execution_submission_record_from_execution_package_v1",
        _fake_write_submission_record_from_package,
    )

    result = orchestrator_module._ensure_governed_submit_inputs(
        truth_root=truth_root,
        day=day,
        identity_dir=identity_dir,
        env={},
    )

    assert result["ok"] is True
    assert Path(str(result["execution_package_path"])).resolve() == package_path
    assert Path(str(result["submission_record_path"])).resolve() == submission_record_path
    assert len(writer_calls) == 1
    assert Path(str(writer_calls[0]["execution_package_path"])).resolve() == package_path
    assert writer_calls[0]["day_utc"] == day
    assert Path(str(writer_calls[0]["truth_root"])).resolve() == truth_root


def test_ensure_governed_submit_inputs_fails_when_submission_record_materialization_fails(monkeypatch, tmp_path: Path) -> None:
    truth_root = (tmp_path / "truth_sleeves" / "PRIMARY" / "PAPER").resolve()
    day = "2026-04-14"
    identity_dir = (
        truth_root
        / "phaseC_preflight_v1"
        / day
        / "attempt_A0001"
        / ("d" * 64)
    ).resolve()
    identity_dir.mkdir(parents=True, exist_ok=True)
    submission_id = "a" * 64
    _write_json(
        identity_dir / "equity_order_plan.v2.json",
        {
            "schema_id": "equity_order_plan",
            "schema_version": "v2",
            "intent_hash": "2" * 64,
            "plan_hash": "3" * 64,
            "source_intent_id": "c2_trend_eq_spy_2026-04-14_v1",
        },
    )
    _write_json(identity_dir / "mapping_ledger_record.v2.json", {"schema_id": "mapping_ledger_record", "schema_version": "v2"})
    _write_json(identity_dir / "binding_record.v2.json", {"schema_id": "binding_record", "schema_version": "v2", "submission_id": submission_id})

    package_path = (
        truth_root / "execution_package_v1" / day / submission_id / "execution_package.v1.json"
    ).resolve()

    def _fake_materialize(*, identity_dir: Path, env: dict[str, str]) -> int:
        _write_json(package_path, {"schema_id": "execution_package", "schema_version": "v1"})
        return 0

    def _raising_writer(**kwargs):
        raise ValueError("invalid_package")

    monkeypatch.setattr(orchestrator_module, "_run_execution_build_authority_for_identity", _fake_materialize)
    monkeypatch.setattr(
        orchestrator_module,
        "write_execution_submission_record_from_execution_package_v1",
        _raising_writer,
    )

    result = orchestrator_module._ensure_governed_submit_inputs(
        truth_root=truth_root,
        day=day,
        identity_dir=identity_dir,
        env={},
    )

    assert result["ok"] is False
    assert str(result["reason_code"]).startswith("GOV_SUBMIT_SUBMISSION_RECORD_MATERIALIZE_FAILED:")
