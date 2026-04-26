from __future__ import annotations

import json
import sys
from pathlib import Path
from types import SimpleNamespace

import pytest

SOURCE_ROOT = Path("/home/node/constellation")
if str(SOURCE_ROOT) not in sys.path:
    sys.path.insert(0, str(SOURCE_ROOT))

import ops.tools.run_paper_session_bootstrap_v1 as bootstrap_module
from constellation_2.common.paper_session_path_alignment_v1 import (
    resolve_operator_statement_path,
    resolve_paper_capital_seed_path,
    resolve_paper_session_authority_path,
    resolve_paper_session_bootstrap_path,
    resolve_runtime_ledger_path,
)
from constellation_2.common.pre_open_materializer_v1 import resolve_pre_open_bundle_path_v1
from constellation_2.common.session_promotion_gate_v1 import resolve_session_promotion_decision_path_v1
from constellation_2.common.runtime_path_authority_v1 import RuntimePathAuthorityV1
from constellation_2.common.trade_submit_readiness_authority_v1 import GovernedSleeveTruthBinding


DAY = "2026-04-14"
ACCOUNT = "DUO847203"


def _write_json(path: Path, payload: object) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, sort_keys=True, separators=(",", ":"), ensure_ascii=False) + "\n", encoding="utf-8")


def _fake_authority(*, canonical_truth_root: Path, sleeve_truth_root: Path) -> RuntimePathAuthorityV1:
    repo_truth_root = canonical_truth_root.parent.parent / "repo_truth"
    repo_truth_sleeves_root = sleeve_truth_root.parent.parent.parent / "repo_truth_sleeves"
    repo_truth_root.mkdir(parents=True, exist_ok=True)
    repo_truth_sleeves_root.mkdir(parents=True, exist_ok=True)
    return RuntimePathAuthorityV1(
        authoritative_repo_root=Path("/home/node/constellation"),
        canonical_runtime_truth_root=canonical_truth_root,
        canonical_runtime_truth_sleeves_root=sleeve_truth_root.parent.parent,
        authoritative_repo_truth_root=repo_truth_root,
        authoritative_repo_truth_sleeves_root=repo_truth_sleeves_root,
        active_release_root=Path("/home/node/constellation"),
    )


def _seed_payload(*, day_utc: str = DAY, seed_usd: str = "5000000.00") -> dict[str, object]:
    return {
        "schema_id": "C2_PAPER_CAPITAL_SEED",
        "schema_version": 1,
        "day_utc": day_utc,
        "produced_utc": f"{day_utc}T00:00:00Z",
        "environment": "PAPER",
        "ib_account": ACCOUNT,
        "currency": "USD",
        "seed_mode": "EXPLICIT_USD",
        "cash_total": seed_usd,
        "nlv_total": seed_usd,
        "policy_ref": {"path": "/policy.json", "sha256": "a" * 64, "policy_id": "test"},
        "notes": ["governed seed"],
    }


def _statement_payload() -> dict[str, object]:
    return {
        "observed_at_utc": f"{DAY}T00:00:00Z",
        "currency": "USD",
        "cash_total": "5000000.00",
        "nlv_total": "5000000.00",
        "available_funds": None,
        "excess_liquidity": None,
        "account_id": ACCOUNT,
        "notes": ["governed seed"],
    }


def _kill_switch_payload(*, state: str = "INACTIVE", reason_codes: list[str] | None = None) -> dict[str, object]:
    return {
        "day_utc": DAY,
        "state": state,
        "state_sha256": "b" * 64,
        "input_manifest": [{"sha256": "c" * 64}],
        "reason_codes": list(reason_codes or []),
    }


def _promotion_payload(
    *,
    promotion_state: str = "PROMOTED",
    blocked_reason_codes: list[str] | None = None,
) -> dict[str, object]:
    return {
        "schema_id": "session_promotion_decision",
        "schema_version": "v1",
        "target_day": DAY,
        "pre_open_bundle_ref": {
            "artifact_path": f"/tmp/{DAY}/pre_open_bundle.v1.json",
            "artifact_sha256": "f" * 64,
        },
        "target_day_admission_ref": {
            "artifact_path": f"/tmp/{DAY}/target_day_admission.v1.json",
            "artifact_sha256": "e" * 64,
        },
        "promotion_state": promotion_state,
        "blocked_reason_codes": list(blocked_reason_codes or []),
        "candidate_artifacts": ["active_session_v1/current.json"],
        "promoted_artifacts": ["active_session_v1/current.json"] if promotion_state == "PROMOTED" else [],
        "prior_current_state": {"active_day": _day_before()},
        "owner_tool": "ops/tools/run_session_authority_v1.py",
        "decided_at_utc": f"{DAY}T00:00:00Z",
        "rules_version": "v1",
    }


def _day_before() -> str:
    year, month, day = [int(part) for part in DAY.split("-")]
    from datetime import date, timedelta

    return (date(year, month, day) - timedelta(days=1)).isoformat()


def _pre_open_row(artifact_id: str, *, role_class: str = "REQUIRED_BINDING_INPUT") -> dict[str, object]:
    return {
        "artifact_id": artifact_id,
        "artifact_name": artifact_id,
        "required": True,
        "role_class": role_class,
        "classification": "PRE_OPEN_PREREQUISITE",
        "canonical_path": f"/tmp/{artifact_id}.json",
        "authority_path": f"/tmp/{artifact_id}.json",
        "path_family": "CANONICAL_RUNTIME_TRUTH_SUBPATH",
        "observed_status": "OK",
        "result_status": "PASS",
        "blocker_codes": [],
        "blocking_reason_code": "",
        "schema_status": "VALID",
        "schema_ref": "governance/test.schema.json",
        "freshness_rule": "TARGET_DAY_MATCH_AND_TIMESTAMP_PRESENT",
        "freshness_status": "CURRENT",
        "target_day_expected": DAY,
        "target_day_observed": DAY,
        "date_binding_status": "MATCH",
        "date_binding_value": DAY,
        "provenance_required": True,
        "provenance_summary": {
            "required": True,
            "present": True,
            "fields_present": ["producer.module"],
            "source": "producer",
        },
        "closure_status": "CLOSED",
        "producer": {"module": "test", "git_sha": "abc123"},
        "source_refs": [],
        "observed_dependency_artifacts": [],
    }


def _pre_open_bundle_payload(
    *,
    materialization_state: str = "COMPLETE",
    completion_state: str = "COMPLETE",
    blocking_reason_codes: list[str] | None = None,
) -> dict[str, object]:
    blocking_codes = list(blocking_reason_codes or [])
    result_status = "PASS" if materialization_state == "COMPLETE" else "FAIL"
    return {
        "schema_id": "pre_open_bundle",
        "schema_version": "v1",
        "target_day": DAY,
        "active_day_observed": DAY,
        "active_day_alignment_status": "MATCH",
        "owner_tool": "ops/tools/run_pre_open_materializer_v1.py",
        "materialization_state": materialization_state,
        "completion_state": completion_state,
        "blocking_reason_codes": blocking_codes,
        "prerequisite_checks": [
            {**_pre_open_row("ib_api_handshake_latest_pointer_v1"), "result_status": result_status, "blocker_codes": blocking_codes},
            {**_pre_open_row("ib_api_handshake_v1"), "result_status": result_status, "blocker_codes": blocking_codes},
            {
                **_pre_open_row("global_kill_switch_state_v1", role_class="REQUIRED_EXECUTION_BOUNDARY"),
                "result_status": result_status,
                "blocker_codes": blocking_codes,
            },
            {
                **_pre_open_row("primary_scoped_canonical_authority_head_v1"),
                "result_status": result_status,
                "blocker_codes": blocking_codes,
            },
        ],
        "producer_results": [],
        "market_calendar_status": {
            "available": False,
            "artifact_path": "",
            "artifact_sha256": "",
            "severity": "",
            "required_target_day": "",
            "reason_codes": [],
        },
        "built_at_utc": f"{DAY}T00:00:00Z",
        "producer": {
            "repo": str(SOURCE_ROOT),
            "module": "constellation_2/common/pre_open_materializer_v1.py",
            "git_sha": "abc123",
        },
    }


def _bootstrap_runtime_artifacts(
    *,
    canonical_truth_root: Path,
    sleeve_truth_root: Path,
    complete: bool = True,
    include_admission: bool = True,
    production_ready: bool = True,
    authorization_status: str = "PASS",
    authorization_reason_codes: list[str] | None = None,
    startup_convergence_status: str = "SUCCESS",
    promotion_state: str = "PROMOTED",
    promotion_blocked_reason_codes: list[str] | None = None,
) -> None:
    _write_json(
        sleeve_truth_root / "cash_ledger_v1" / "snapshots" / DAY / "cash_ledger_snapshot.v1.json",
        {"day_utc": DAY, "status": "PASS", "schema_id": "cash_ledger_snapshot_v1"},
    )
    _write_json(
        sleeve_truth_root / "accounting_v2" / "nav" / DAY / "nav.v2.json",
        {"day_utc": DAY, "status": "PASS", "nav_total": "5000000.00"},
    )
    _write_json(
        sleeve_truth_root / "allocation_v1" / "summary" / DAY / "summary.json",
        {"day_utc": DAY, "status": "PASS"},
    )
    _write_json(
        sleeve_truth_root / "reports" / "capital_risk_envelope_v2" / DAY / "capital_risk_envelope.v2.json",
        {
            "day_utc": DAY,
            "status": "PASS",
            "reason_codes": [],
            "envelope": {
                "allowed_capital_at_risk_cents": 10000000,
                "headroom_cents": 10000000,
                "nav_total_cents": 500000000,
            },
        },
    )
    _write_json(
        sleeve_truth_root / "allocation_v1" / "capital_authority_allocation_v1" / DAY / "capital_authority_allocation.v1.json",
        {
            "day_utc": DAY,
            "status": "PASS",
            "portfolio": {
                "allowed_capital_at_risk_cents": 10000000,
                "headroom_cents": 10000000,
            },
        },
    )
    if complete:
        auth_reason_codes = list(authorization_reason_codes or [])
        hidden_dependency_check_result = {
            "status": "PASS" if production_ready else "FAIL",
            "blocking_reason_code": "" if production_ready else "PARTIAL_BUILD",
            "summary": "production prereqs pass" if production_ready else "failing_producers=ops/tools/run_session_readiness_refresh_v1.py",
        }
        _write_json(
            sleeve_truth_root / "reports" / "authorization_gate_verdict_v1" / DAY / "authorization_gate_verdict.v1.json",
            {"day_utc": DAY, "status": authorization_status, "reason_codes": auth_reason_codes},
        )
        _write_json(
            canonical_truth_root / "reports" / "paper_startup_authorization_convergence_v1" / DAY / "paper_startup_authorization_convergence.v1.json",
            {
                "day_utc": DAY,
                "convergence_status": startup_convergence_status,
                "authorization_verdict_ready": True,
                "blocker_chain": [] if startup_convergence_status == "SUCCESS" else auth_reason_codes,
            },
        )
        if include_admission:
            _write_json(
                canonical_truth_root / "target_day_admission_v1" / f"{DAY}.json",
                {
                    "day_utc": DAY,
                    "admission_status": "ADMIT",
                    "mode": "PAPER_BOOTSTRAP",
                    "reason": "PAPER_BOOTSTRAP_SESSION_ADMISSION",
                    "binding": True,
                    "build_ref": {
                        "artifact_path": str(canonical_truth_root / "target_day_build_v1" / f"{DAY}.json"),
                        "artifact_sha256": "e" * 64,
                    },
                    "closure_status": "CLOSED" if production_ready else "OPEN",
                    "hidden_dependency_check_result": hidden_dependency_check_result,
                    "reason_codes": [],
                },
            )
            _write_json(
                canonical_truth_root / "run_pointer_v2" / "canonical_authority_head.v1.json",
                {"day_utc": DAY, "points_to": "/tmp/authorization_gate_verdict.v1.json", "status": "PASS", "reason_codes": []},
            )
            _write_json(
                resolve_session_promotion_decision_path_v1(truth_root=canonical_truth_root, day_utc=DAY),
                _promotion_payload(
                    promotion_state=promotion_state,
                    blocked_reason_codes=promotion_blocked_reason_codes,
                ),
            )


def _fake_subprocess_run_factory(
    *,
    canonical_truth_root: Path,
    sleeve_truth_root: Path,
    operator_input_root: Path,
    activation_complete: bool = True,
    production_ready: bool = True,
    authorization_status: str = "PASS",
    authorization_reason_codes: list[str] | None = None,
    startup_convergence_status: str = "SUCCESS",
    pre_open_materialization_state: str = "COMPLETE",
    pre_open_blocking_reason_codes: list[str] | None = None,
    promotion_state: str = "PROMOTED",
    promotion_blocked_reason_codes: list[str] | None = None,
    called_tools: list[str] | None = None,
    called_cmds: list[list[str]] | None = None,
) -> callable:
    def _fake_run(cmd: list[str], cwd: str | None = None, capture_output: bool = True, text: bool = True, env: dict[str, str] | None = None):  # type: ignore[override]
        tool = cmd[1] if len(cmd) > 1 else ""
        if called_tools is not None:
            called_tools.append(tool)
        if called_cmds is not None:
            called_cmds.append(list(cmd))
        stdout = ""
        if tool == str(bootstrap_module.ENSURE_PAPER_CAPITAL_SEED_TOOL):
            _write_json(resolve_paper_capital_seed_path(operator_input_root=operator_input_root, day_utc=DAY), _seed_payload())
            stdout = f"OK: PAPER_CAPITAL_SEED_WRITTEN day_utc={DAY}"
        elif tool == str(bootstrap_module.ENSURE_OPERATOR_STATEMENT_TOOL):
            _write_json(resolve_operator_statement_path(operator_input_root=operator_input_root, day_utc=DAY), _statement_payload())
            stdout = f"OK: OPERATOR_STATEMENT_WRITTEN day_utc={DAY}"
        elif tool == str(bootstrap_module.RUN_GLOBAL_KILL_SWITCH_TOOL):
            auth_path = sleeve_truth_root / "reports" / "authorization_gate_verdict_v1" / DAY / "authorization_gate_verdict.v1.json"
            auth_payload = json.loads(auth_path.read_text(encoding="utf-8")) if auth_path.exists() else {}
            auth_state = str(auth_payload.get("status") or "").strip().upper()
            payload = _kill_switch_payload(
                state="INACTIVE" if auth_state in {"PASS", "BOOTSTRAP_PASS"} else "ACTIVE",
                reason_codes=[] if auth_state in {"PASS", "BOOTSTRAP_PASS"} else ["C2_KILL_SWITCH_ACTIVE"],
            )
            _write_json(canonical_truth_root / "risk_v1" / "kill_switch_v1" / DAY / "global_kill_switch_state.v1.json", payload)
            _write_json(sleeve_truth_root / "risk_v1" / "kill_switch_v1" / DAY / "global_kill_switch_state.v1.json", payload)
            stdout = f"OK: GLOBAL_KILL_SWITCH_WRITTEN day_utc={DAY}"
        elif tool == str(bootstrap_module.RUN_STARTUP_AUTHORIZATION_CONVERGENCE_TOOL):
            _bootstrap_runtime_artifacts(
                canonical_truth_root=canonical_truth_root,
                sleeve_truth_root=sleeve_truth_root,
                complete=True,
                include_admission=False,
                production_ready=production_ready,
                authorization_status=authorization_status,
                authorization_reason_codes=authorization_reason_codes,
                startup_convergence_status=startup_convergence_status,
            )
            stdout = json.dumps({"target_day": DAY, "convergence_status": startup_convergence_status}, sort_keys=True)
        elif tool == str(bootstrap_module.RUN_PRE_OPEN_MATERIALIZER_TOOL):
            blocking_codes = list(pre_open_blocking_reason_codes or [])
            _write_json(
                resolve_pre_open_bundle_path_v1(truth_root=canonical_truth_root, day_utc=DAY),
                _pre_open_bundle_payload(
                    materialization_state=pre_open_materialization_state,
                    completion_state="COMPLETE" if pre_open_materialization_state == "COMPLETE" else "INCOMPLETE",
                    blocking_reason_codes=blocking_codes,
                ),
            )
            stdout = json.dumps(
                {
                    "target_day": DAY,
                    "materialization_state": pre_open_materialization_state,
                    "blocking_reason_codes": blocking_codes,
                },
                sort_keys=True,
            )
            return SimpleNamespace(
                returncode=0 if pre_open_materialization_state == "COMPLETE" else 2,
                stdout=stdout,
                stderr="",
            )
        elif tool == str(bootstrap_module.RUN_EXPOSURE_NET_TOOL):
            _write_json(
                sleeve_truth_root / "risk_v1" / "exposure_net_v1" / DAY / "exposure_net.v1.json",
                {
                    "day_utc": DAY,
                    "status": "OK",
                    "reason_codes": [],
                    "schema_id": "C2_EXPOSURE_NET_V1",
                    "schema_version": 1,
                },
            )
            stdout = f"OK: EXPOSURE_NET_V1_WRITTEN day_utc={DAY}"
        elif tool == str(bootstrap_module.RUN_SESSION_AUTHORITY_TOOL):
            _bootstrap_runtime_artifacts(
                canonical_truth_root=canonical_truth_root,
                sleeve_truth_root=sleeve_truth_root,
                complete=True,
                include_admission=True,
                production_ready=production_ready,
                authorization_status=authorization_status,
                authorization_reason_codes=authorization_reason_codes,
                startup_convergence_status=startup_convergence_status,
                promotion_state=promotion_state,
                promotion_blocked_reason_codes=promotion_blocked_reason_codes,
            )
            stdout = json.dumps(
                {
                    "truth_root": str(canonical_truth_root),
                    "target_day": DAY,
                    "target_day_admission": {
                        "path": str(canonical_truth_root / "target_day_admission_v1" / f"{DAY}.json"),
                        "admission_status": "ADMIT",
                    },
                },
                sort_keys=True,
            )
        elif tool == str(bootstrap_module.RUN_DAY_ACTIVATION_TOOL):
            package_path = sleeve_truth_root / "day_activation_package_v1" / DAY / "test" / "day_activation_package.v1.json"
            build_path = canonical_truth_root / "reports" / "day_activation_build_v1" / DAY / "test" / "day_activation_build.v1.json"
            _write_json(package_path, {"status": "PASS"})
            _write_json(build_path, {"closure_status": "COMPLETE"})
            stdout = json.dumps(
                {
                    "build_path": str(build_path),
                    "package_path": str(package_path),
                    "closure_status": "COMPLETE" if activation_complete else "BLOCKED",
                    "first_real_blocker": None if activation_complete else {"dependency_id": "target_day_admission_v1"},
                },
                sort_keys=True,
            )
        else:
            _bootstrap_runtime_artifacts(
                canonical_truth_root=canonical_truth_root,
                sleeve_truth_root=sleeve_truth_root,
                complete=False,
                include_admission=False,
                production_ready=production_ready,
            )
            stdout = "OK"
        return SimpleNamespace(returncode=0, stdout=stdout, stderr="")

    return _fake_run


def _bootstrap_monkeypatch(
    monkeypatch: pytest.MonkeyPatch,
    *,
    canonical_truth_root: Path,
    sleeve_truth_root: Path,
    operator_input_root: Path,
    activation_complete: bool = True,
    production_ready: bool = True,
    authorization_status: str = "PASS",
    authorization_reason_codes: list[str] | None = None,
    startup_convergence_status: str = "SUCCESS",
    pre_open_materialization_state: str = "COMPLETE",
    pre_open_blocking_reason_codes: list[str] | None = None,
    promotion_state: str = "PROMOTED",
    promotion_blocked_reason_codes: list[str] | None = None,
    called_tools: list[str] | None = None,
    called_cmds: list[list[str]] | None = None,
) -> None:
    monkeypatch.setattr(
        bootstrap_module,
        "resolve_decision_truth_root_bridge_v1",
        lambda explicit_truth_root, repo_root=None, caller="": canonical_truth_root,
    )
    monkeypatch.setattr(
        bootstrap_module,
        "load_runtime_path_authority_bridge_v1",
        lambda repo_root=None, caller="": _fake_authority(
            canonical_truth_root=canonical_truth_root,
            sleeve_truth_root=sleeve_truth_root,
        ),
    )
    monkeypatch.setattr(
        bootstrap_module,
        "resolve_governed_sleeve_truth_bindings",
        lambda repo_root, environment, requested_ib_account, sleeve_id: (
            GovernedSleeveTruthBinding(
                environment="PAPER",
                ib_account=ACCOUNT,
                sleeve_id="PRIMARY",
                truth_partition="truth_sleeves/PRIMARY/PAPER",
                truth_root=sleeve_truth_root,
                sleeve_registry_path=operator_input_root / "sleeve_registry.json",
                sleeve_registry_sha256="d" * 64,
            ),
        ),
    )
    monkeypatch.setattr(bootstrap_module, "resolve_single_paper_ib_account_from_sleeve_registry", lambda repo_root: ACCOUNT)
    monkeypatch.setattr(bootstrap_module, "repo_git_sha_v1", lambda: "a" * 40)
    monkeypatch.setattr(
        bootstrap_module,
        "_session_day_blocker_from_market_calendar",
        lambda canonical_truth_root, day_utc: ("", ""),
    )
    monkeypatch.setattr(
        bootstrap_module.subprocess,
        "run",
        _fake_subprocess_run_factory(
            canonical_truth_root=canonical_truth_root,
            sleeve_truth_root=sleeve_truth_root,
            operator_input_root=operator_input_root,
            activation_complete=activation_complete,
            production_ready=production_ready,
            authorization_status=authorization_status,
            authorization_reason_codes=authorization_reason_codes,
            startup_convergence_status=startup_convergence_status,
            pre_open_materialization_state=pre_open_materialization_state,
            pre_open_blocking_reason_codes=pre_open_blocking_reason_codes,
            promotion_state=promotion_state,
            promotion_blocked_reason_codes=promotion_blocked_reason_codes,
            called_tools=called_tools,
            called_cmds=called_cmds,
        ),
    )
    monkeypatch.setattr(bootstrap_module, "resolve_session_authority_target_day_v1", lambda raw: DAY if not raw else raw)


def test_bootstrap_authority_boundary_uses_bridge_helpers(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    operator_input_root = (tmp_path / "operator_root").resolve()
    operator_input_root.mkdir(parents=True, exist_ok=True)
    canonical_truth_root = (tmp_path / "truth").resolve()
    canonical_truth_root.mkdir(parents=True, exist_ok=True)

    calls: dict[str, object] = {}

    def _decision_bridge(explicit_truth_root: str, *, repo_root=None, caller: str = "") -> Path:
        calls["decision"] = {
            "explicit_truth_root": explicit_truth_root,
            "repo_root": Path(repo_root).resolve() if repo_root is not None else None,
            "caller": caller,
        }
        return canonical_truth_root

    def _runtime_path_bridge(*, repo_root=None, caller: str = "") -> SimpleNamespace:
        calls["snapshot"] = {
            "repo_root": Path(repo_root).resolve() if repo_root is not None else None,
            "caller": caller,
        }
        return SimpleNamespace(canonical_runtime_truth_root=canonical_truth_root)

    monkeypatch.setattr(bootstrap_module, "resolve_session_authority_target_day_v1", lambda raw: DAY)
    monkeypatch.setattr(bootstrap_module, "resolve_decision_truth_root_bridge_v1", _decision_bridge)
    monkeypatch.setattr(bootstrap_module, "load_runtime_path_authority_bridge_v1", _runtime_path_bridge)
    monkeypatch.setattr(
        bootstrap_module,
        "resolve_governed_sleeve_truth_bindings",
        lambda **_: (_ for _ in ()).throw(RuntimeError("STOP_AFTER_AUTHORITY_BOUNDARY")),
    )

    with pytest.raises(RuntimeError, match="STOP_AFTER_AUTHORITY_BOUNDARY"):
        bootstrap_module.main(
            [
                "--day_utc",
                DAY,
                "--truth_root",
                str(canonical_truth_root),
                "--operator_input_root",
                str(operator_input_root),
            ]
        )

    assert calls["decision"] == {
        "explicit_truth_root": str(canonical_truth_root),
        "repo_root": bootstrap_module.REPO_ROOT.resolve(),
        "caller": "ops/tools/run_paper_session_bootstrap_v1.py",
    }
    assert calls["snapshot"] == {
        "repo_root": bootstrap_module.REPO_ROOT.resolve(),
        "caller": "ops/tools/run_paper_session_bootstrap_v1.py",
    }


def test_bootstrap_authority_boundary_fails_closed_when_bridge_invalid(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    operator_input_root = (tmp_path / "operator_root").resolve()
    operator_input_root.mkdir(parents=True, exist_ok=True)

    monkeypatch.setattr(bootstrap_module, "resolve_session_authority_target_day_v1", lambda raw: DAY)
    monkeypatch.setattr(
        bootstrap_module,
        "resolve_decision_truth_root_bridge_v1",
        lambda explicit_truth_root, repo_root=None, caller="": (_ for _ in ()).throw(
            SystemExit("FAIL: runtime_authority_bridge_release_current_invalid path=/tmp/current.json err=ValueError:bad")
        ),
    )

    with pytest.raises(SystemExit, match="runtime_authority_bridge_release_current_invalid"):
        bootstrap_module.main(
            [
                "--day_utc",
                DAY,
                "--truth_root",
                str((tmp_path / "truth").resolve()),
                "--operator_input_root",
                str(operator_input_root),
            ]
        )


def test_bootstrap_authority_guard_semantics_unchanged(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    operator_input_root = (tmp_path / "operator_root").resolve()
    operator_input_root.mkdir(parents=True, exist_ok=True)
    requested_truth_root = (tmp_path / "requested_truth").resolve()
    canonical_truth_root = (tmp_path / "canonical_truth").resolve()
    requested_truth_root.mkdir(parents=True, exist_ok=True)
    canonical_truth_root.mkdir(parents=True, exist_ok=True)

    monkeypatch.setattr(bootstrap_module, "resolve_session_authority_target_day_v1", lambda raw: DAY)
    monkeypatch.setattr(
        bootstrap_module,
        "resolve_decision_truth_root_bridge_v1",
        lambda explicit_truth_root, repo_root=None, caller="": requested_truth_root,
    )
    monkeypatch.setattr(
        bootstrap_module,
        "load_runtime_path_authority_bridge_v1",
        lambda repo_root=None, caller="": SimpleNamespace(canonical_runtime_truth_root=canonical_truth_root),
    )

    with pytest.raises(SystemExit, match="PAPER_BOOTSTRAP_CANONICAL_TRUTH_REQUIRED"):
        bootstrap_module.main(
            [
                "--day_utc",
                DAY,
                "--truth_root",
                str(requested_truth_root),
                "--operator_input_root",
                str(operator_input_root),
            ]
        )


def test_bootstrap_fails_closed_outside_paper(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    canonical_truth_root = tmp_path / "truth"
    sleeve_truth_root = tmp_path / "truth_sleeves" / "PRIMARY" / "PAPER"
    operator_input_root = tmp_path / "operator_root"
    canonical_truth_root.mkdir(parents=True, exist_ok=True)
    sleeve_truth_root.mkdir(parents=True, exist_ok=True)
    operator_input_root.mkdir(parents=True, exist_ok=True)
    _bootstrap_monkeypatch(
        monkeypatch,
        canonical_truth_root=canonical_truth_root,
        sleeve_truth_root=sleeve_truth_root,
        operator_input_root=operator_input_root,
    )
    with pytest.raises(SystemExit, match="PAPER_ONLY_BOOTSTRAP_TOOL"):
        bootstrap_module.main(
            [
                "--day_utc",
                DAY,
                "--truth_root",
                str(canonical_truth_root),
                "--operator_input_root",
                str(operator_input_root),
                "--environment",
                "LIVE",
            ]
        )


def test_bootstrap_success_materializes_report_and_allows_smoke(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    canonical_truth_root = tmp_path / "truth"
    sleeve_truth_root = tmp_path / "truth_sleeves" / "PRIMARY" / "PAPER"
    operator_input_root = tmp_path / "operator_root"
    canonical_truth_root.mkdir(parents=True, exist_ok=True)
    sleeve_truth_root.mkdir(parents=True, exist_ok=True)
    operator_input_root.mkdir(parents=True, exist_ok=True)
    _bootstrap_monkeypatch(
        monkeypatch,
        canonical_truth_root=canonical_truth_root,
        sleeve_truth_root=sleeve_truth_root,
        operator_input_root=operator_input_root,
    )
    rc = bootstrap_module.main(
        [
            "--day_utc",
            DAY,
            "--truth_root",
            str(canonical_truth_root),
            "--operator_input_root",
            str(operator_input_root),
            "--environment",
            "PAPER",
            "--seed_usd",
            "5000000.00",
        ]
    )
    assert rc == 0
    report_path = resolve_paper_session_bootstrap_path(truth_root=canonical_truth_root, day_utc=DAY)
    payload = json.loads(report_path.read_text(encoding="utf-8"))
    startup_materialization_path = canonical_truth_root / "reports" / "startup_materialization_v1" / DAY / "startup_materialization.v1.json"
    startup_materialization_payload = json.loads(startup_materialization_path.read_text(encoding="utf-8"))
    paper_authority_path = resolve_paper_session_authority_path(truth_root=canonical_truth_root, day_utc=DAY)
    paper_authority_payload = json.loads(paper_authority_path.read_text(encoding="utf-8"))
    assert payload["bootstrap_status"] == "READY"
    assert payload["bootstrap_semantic_status"] == "READY"
    assert payload["day_activation_ready"] is True
    activation_step = next(row for row in payload["step_results"] if row["step_id"] == "day_activation_readiness")
    assert activation_step["action"] == "MATERIALIZED"
    assert payload["smoke_submit_allowed"] is True
    assert payload["startup_materialization_phase"]["status"] == "COMPLETE"
    assert payload["evaluation_phase"]["status"] == "READY"
    assert payload["activation_phase"]["status"] == "READY"
    assert payload["root_blocker_class"] == "NONE"
    assert payload["required_prerequisites_status"]["status"] == "PASS"
    assert payload["runtime_prerequisite_verification"]["status"] == "READY"
    assert payload["runtime_prerequisite_verification"]["stage"] == "NONE"
    assert payload["runtime_prerequisite_verification"]["earliest_failing_prerequisite"] is None
    assert payload["production_only_prerequisites_status"]["status"] == "PASS"
    assert payload["owner_run_id"] == payload["bootstrap_run_id"]
    assert len(payload["frozen_input_manifest_sha256"]) == 64
    assert payload["shared_control_state"]["sleeve_kill_switch_projection"]["synced_from_canonical"] is True
    assert payload["session_bootstrap"]["startup_authorization_convergence"]["status"] == "SUCCESS"
    assert payload["session_bootstrap"]["admission"]["admission_status"] == "ADMIT"
    assert payload["session_bootstrap"]["promotion_gate"]["promotion_state"] == "PROMOTED"
    assert payload["session_bootstrap"]["pointer_refresh"]["refreshed_for_day"] is True
    assert payload["session_bootstrap"]["capital_risk_envelope"]["status"] == "PASS"
    assert payload["session_bootstrap"]["allocation_readiness"]["portfolio_allowed_capital_at_risk_cents"] == 10000000
    assert startup_materialization_payload["materialization_scope"] == "BOOTSTRAP_SESSION_PREREQUISITES"
    assert startup_materialization_payload["phase_summary"]["phase_status"] == "COMPLETE"
    assert startup_materialization_payload["phase_summary"]["gate_artifacts_status"] == "READY"
    assert report_path.parent.parent.name == "paper_session_bootstrap_v1"
    step_ids = [row["step_id"] for row in payload["step_results"]]
    assert step_ids.index("startup_authorization_convergence") < step_ids.index("global_kill_switch")
    assert step_ids.index("pre_open_materialization") < step_ids.index("session_authority")
    assert step_ids.index("session_authority") < step_ids.index("session_promotion_gate")
    assert step_ids.index("session_promotion_gate") < step_ids.index("day_activation_readiness")
    assert payload["runtime_ledger_projection"]["derived_from_runtime_ledger"] is True
    assert paper_authority_payload["authority_status"] == "GRANTED"
    assert paper_authority_payload["paper_open_allowed"] is True
    assert paper_authority_payload["degraded_mode"] is False
    assert paper_authority_payload["blocking_reason_codes"] == []
    assert paper_authority_payload["mode"] == "PAPER"
    assert paper_authority_payload["upstream_refs"]["paper_session_bootstrap_v1"] == str(report_path.resolve())
    assert payload["operator_guidance"]["first_blocker_prerequisite_id"] == ""
    assert payload["operator_guidance"]["fix_then_rerun_rule"] == "FIX_EARLIEST_FAILING_PREREQUISITE_THEN_RERUN_CANONICAL_ENTRY"
    assert step_ids.index("exposure_net") < step_ids.index("capital_authority_allocation")
    assert step_ids.index("startup_authorization_convergence") < step_ids.index("capital_authority_allocation")
    assert payload["runtime_ledger_projection"]["event_types"] == [
        "ACTIVATION_READY",
        "ADMISSION_GRANTED",
        "BOOTSTRAP_READY",
        "BOOTSTRAP_STARTED",
        "KILL_SWITCH_EVALUATED_INACTIVE",
        "STARTUP_AUTH_CONVERGENCE_COMPLETE",
        "STARTUP_GATE_ARTIFACTS_READY",
        "STARTUP_MATERIALIZATION_COMPLETE",
        "STARTUP_MATERIALIZATION_STARTED",
    ]
    ledger_path = resolve_runtime_ledger_path(truth_root=canonical_truth_root, day_utc=DAY)
    ledger_rows = [json.loads(line) for line in ledger_path.read_text(encoding="utf-8").splitlines()]
    assert [row["event_type"] for row in ledger_rows] == [
        "BOOTSTRAP_STARTED",
        "STARTUP_MATERIALIZATION_STARTED",
        "STARTUP_AUTH_CONVERGENCE_COMPLETE",
        "STARTUP_GATE_ARTIFACTS_READY",
        "STARTUP_MATERIALIZATION_COMPLETE",
        "KILL_SWITCH_EVALUATED_INACTIVE",
        "ADMISSION_GRANTED",
        "ACTIVATION_READY",
        "BOOTSTRAP_READY",
    ]
    assert ledger_rows[0]["owner_plane"] == "CANONICAL_CONTROL_PLANE"
    assert ledger_rows[1]["owner_plane"] == "STARTUP_MATERIALIZATION_PLANE"
    assert ledger_rows[5]["owner_plane"] == "EVALUATION_SAFETY_PLANE"
    assert ledger_rows[-1]["owner_plane"] == "CANONICAL_CONTROL_PLANE"


def test_bootstrap_materializes_seed_from_continuity_when_explicit_seed_omitted(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    canonical_truth_root = tmp_path / "truth"
    sleeve_truth_root = tmp_path / "truth_sleeves" / "PRIMARY" / "PAPER"
    operator_input_root = tmp_path / "operator_root"
    canonical_truth_root.mkdir(parents=True, exist_ok=True)
    sleeve_truth_root.mkdir(parents=True, exist_ok=True)
    operator_input_root.mkdir(parents=True, exist_ok=True)
    prior_day = _day_before()
    _write_json(
        resolve_paper_capital_seed_path(operator_input_root=operator_input_root, day_utc=prior_day),
        _seed_payload(day_utc=prior_day, seed_usd="6100000.00"),
    )
    called_cmds: list[list[str]] = []
    _bootstrap_monkeypatch(
        monkeypatch,
        canonical_truth_root=canonical_truth_root,
        sleeve_truth_root=sleeve_truth_root,
        operator_input_root=operator_input_root,
        called_cmds=called_cmds,
    )
    rc = bootstrap_module.main(
        [
            "--day_utc",
            DAY,
            "--truth_root",
            str(canonical_truth_root),
            "--operator_input_root",
            str(operator_input_root),
            "--environment",
            "PAPER",
        ]
    )
    assert rc == 0
    payload = json.loads(resolve_paper_session_bootstrap_path(truth_root=canonical_truth_root, day_utc=DAY).read_text(encoding="utf-8"))
    seed_step = next(row for row in payload["step_results"] if row["step_id"] == "paper_capital_seed")
    assert seed_step["action"] == "MATERIALIZED"
    assert payload["bootstrap_status"] == "READY"
    seed_cmd = next(
        cmd
        for cmd in called_cmds
        if len(cmd) > 1 and cmd[1] == str(bootstrap_module.ENSURE_PAPER_CAPITAL_SEED_TOOL)
    )
    assert "--seed_usd" in seed_cmd
    assert seed_cmd[seed_cmd.index("--seed_usd") + 1] == "6100000.00"


def test_bootstrap_fails_closed_when_continuity_seed_cannot_be_resolved(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    canonical_truth_root = tmp_path / "truth"
    sleeve_truth_root = tmp_path / "truth_sleeves" / "PRIMARY" / "PAPER"
    operator_input_root = tmp_path / "operator_root"
    canonical_truth_root.mkdir(parents=True, exist_ok=True)
    sleeve_truth_root.mkdir(parents=True, exist_ok=True)
    operator_input_root.mkdir(parents=True, exist_ok=True)
    _bootstrap_monkeypatch(
        monkeypatch,
        canonical_truth_root=canonical_truth_root,
        sleeve_truth_root=sleeve_truth_root,
        operator_input_root=operator_input_root,
    )
    rc = bootstrap_module.main(
        [
            "--day_utc",
            DAY,
            "--truth_root",
            str(canonical_truth_root),
            "--operator_input_root",
            str(operator_input_root),
            "--environment",
            "PAPER",
        ]
    )
    assert rc == 2
    payload = json.loads(resolve_paper_session_bootstrap_path(truth_root=canonical_truth_root, day_utc=DAY).read_text(encoding="utf-8"))
    seed_step = next(row for row in payload["step_results"] if row["step_id"] == "paper_capital_seed")
    assert seed_step["action"] == "FAILED"
    assert "CONTINUITY_PAPER_CAPITAL_SEED_RESOLUTION_FAILED" in seed_step["stderr"]
    assert "PAPER_CAPITAL_SEED_MISSING" in payload["blocker_chain"]


def test_bootstrap_missing_seed_fails_closed_with_clear_blocker_chain(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    canonical_truth_root = tmp_path / "truth"
    sleeve_truth_root = tmp_path / "truth_sleeves" / "PRIMARY" / "PAPER"
    operator_input_root = tmp_path / "operator_root"
    canonical_truth_root.mkdir(parents=True, exist_ok=True)
    sleeve_truth_root.mkdir(parents=True, exist_ok=True)
    operator_input_root.mkdir(parents=True, exist_ok=True)
    _bootstrap_monkeypatch(
        monkeypatch,
        canonical_truth_root=canonical_truth_root,
        sleeve_truth_root=sleeve_truth_root,
        operator_input_root=operator_input_root,
    )
    rc = bootstrap_module.main(
        [
            "--day_utc",
            DAY,
            "--truth_root",
            str(canonical_truth_root),
            "--operator_input_root",
            str(operator_input_root),
            "--environment",
            "PAPER",
            "--materialize",
            "NO",
        ]
    )
    assert rc == 2
    report_path = resolve_paper_session_bootstrap_path(truth_root=canonical_truth_root, day_utc=DAY)
    payload = json.loads(report_path.read_text(encoding="utf-8"))
    startup_materialization_payload = json.loads(
        (canonical_truth_root / "reports" / "startup_materialization_v1" / DAY / "startup_materialization.v1.json").read_text(
            encoding="utf-8"
        )
    )
    paper_authority_payload = json.loads(
        resolve_paper_session_authority_path(truth_root=canonical_truth_root, day_utc=DAY).read_text(encoding="utf-8")
    )
    assert payload["bootstrap_status"] == "BLOCKED"
    assert payload["bootstrap_semantic_status"] == "BLOCKED"
    assert payload["startup_materialization_phase"]["status"] == "BLOCKED"
    assert payload["evaluation_phase"]["status"] == "BLOCKED"
    assert payload["activation_phase"]["status"] == "BLOCKED"
    assert payload["root_blocker_class"] == "STARTUP_MATERIALIZATION"
    assert payload["required_prerequisites_status"]["status"] == "FAIL"
    assert payload["runtime_prerequisite_verification"]["status"] == "BLOCKED"
    assert payload["runtime_prerequisite_verification"]["stage"] == "STARTUP_MATERIALIZATION"
    earliest = payload["runtime_prerequisite_verification"]["earliest_failing_prerequisite"]
    assert earliest["prerequisite_id"] == "paper_capital_seed_v1"
    assert earliest["owner_tool"].endswith("ops/tools/ensure_paper_capital_seed_v1.py")
    assert earliest["blocker_class"] == "MISSING_ARTIFACT"
    assert earliest["reason_codes"] == ["PAPER_CAPITAL_SEED_MISSING"]
    assert payload["operator_guidance"]["recommended_action"] == "RETRY"
    assert payload["operator_guidance"]["first_blocker_prerequisite_id"] == "paper_capital_seed_v1"
    assert payload["operator_guidance"]["first_blocker_stage"] == "STARTUP_MATERIALIZATION"
    assert "ops/tools/run_day_open_attempt_v1.py" in payload["operator_guidance"]["do_not_run_manually"]
    assert "PAPER_CAPITAL_SEED_MISSING" in payload["blocker_chain"]
    assert "AUTHORIZATION_GATE_ARTIFACT_MISSING" in payload["startup_materialization_phase"]["blocker_chain"]
    session_step = next(row for row in payload["step_results"] if row["step_id"] == "session_authority")
    activation_step = next(row for row in payload["step_results"] if row["step_id"] == "day_activation_readiness")
    assert session_step["action"] == "SKIPPED"
    assert activation_step["action"] == "SKIPPED"
    assert payload["session_bootstrap"]["pointer_refresh"]["exists"] is False
    assert payload["day_activation_ready"] is False
    assert payload["smoke_submit_allowed"] is False
    assert paper_authority_payload["authority_status"] == "DENIED"
    assert paper_authority_payload["paper_open_allowed"] is False
    assert "PAPER_CAPITAL_SEED_MISSING" in paper_authority_payload["blocking_reason_codes"]
    assert paper_authority_payload["degraded_mode"] is False
    assert startup_materialization_payload["status"] == "MISSING_DEPENDENCY"
    assert startup_materialization_payload["phase_summary"]["phase_status"] == "BLOCKED"
    ledger_rows = [
        json.loads(line)
        for line in resolve_runtime_ledger_path(truth_root=canonical_truth_root, day_utc=DAY).read_text(encoding="utf-8").splitlines()
    ]
    assert [row["event_type"] for row in ledger_rows] == [
        "BOOTSTRAP_STARTED",
        "STARTUP_MATERIALIZATION_STARTED",
        "STARTUP_AUTH_CONVERGENCE_BLOCKED",
        "STARTUP_GATE_ARTIFACTS_BLOCKED",
        "STARTUP_MATERIALIZATION_BLOCKED",
        "KILL_SWITCH_EVALUATED_ACTIVE",
        "ADMISSION_BLOCKED",
        "ACTIVATION_BLOCKED",
        "BOOTSTRAP_BLOCKED",
    ]


def test_bootstrap_stops_before_session_authority_and_activation_when_pre_open_blocks(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    canonical_truth_root = tmp_path / "truth"
    sleeve_truth_root = tmp_path / "truth_sleeves" / "PRIMARY" / "PAPER"
    operator_input_root = tmp_path / "operator_root"
    canonical_truth_root.mkdir(parents=True, exist_ok=True)
    sleeve_truth_root.mkdir(parents=True, exist_ok=True)
    operator_input_root.mkdir(parents=True, exist_ok=True)
    called_tools: list[str] = []
    _bootstrap_monkeypatch(
        monkeypatch,
        canonical_truth_root=canonical_truth_root,
        sleeve_truth_root=sleeve_truth_root,
        operator_input_root=operator_input_root,
        pre_open_materialization_state="BLOCKED",
        pre_open_blocking_reason_codes=["IB_API_HANDSHAKE_NOT_OK"],
        called_tools=called_tools,
    )
    rc = bootstrap_module.main(
        [
            "--truth_root",
            str(canonical_truth_root),
            "--operator_input_root",
            str(operator_input_root),
            "--environment",
            "PAPER",
            "--seed_usd",
            "5000000.00",
        ]
    )
    assert rc == 2
    payload = json.loads(resolve_paper_session_bootstrap_path(truth_root=canonical_truth_root, day_utc=DAY).read_text(encoding="utf-8"))
    pre_open_step = next(row for row in payload["step_results"] if row["step_id"] == "pre_open_materialization")
    session_step = next(row for row in payload["step_results"] if row["step_id"] == "session_authority")
    activation_step = next(row for row in payload["step_results"] if row["step_id"] == "day_activation_readiness")
    assert pre_open_step["action"] == "FAILED"
    assert pre_open_step["status"] == "BLOCKED"
    assert session_step["action"] == "SKIPPED"
    assert activation_step["action"] == "SKIPPED"
    assert str(bootstrap_module.RUN_SESSION_AUTHORITY_TOOL) not in called_tools
    assert str(bootstrap_module.RUN_DAY_ACTIVATION_TOOL) not in called_tools
    assert payload["runtime_prerequisite_verification"]["stage"] == "PRE_OPEN"
    earliest = payload["runtime_prerequisite_verification"]["earliest_failing_prerequisite"]
    assert earliest["prerequisite_id"] == "ib_api_handshake_latest_pointer_v1"
    assert earliest["owner_tool"].endswith("ops/tools/run_ib_api_handshake_spine_v1.py")


def test_bootstrap_blocks_explicitly_when_rollover_fails_stale_authority_head(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    canonical_truth_root = tmp_path / "truth"
    sleeve_truth_root = tmp_path / "truth_sleeves" / "PRIMARY" / "PAPER"
    operator_input_root = tmp_path / "operator_root"
    canonical_truth_root.mkdir(parents=True, exist_ok=True)
    sleeve_truth_root.mkdir(parents=True, exist_ok=True)
    operator_input_root.mkdir(parents=True, exist_ok=True)
    _bootstrap_monkeypatch(
        monkeypatch,
        canonical_truth_root=canonical_truth_root,
        sleeve_truth_root=sleeve_truth_root,
        operator_input_root=operator_input_root,
        pre_open_materialization_state="BLOCKED",
        pre_open_blocking_reason_codes=["ROLLOVER_FAILED_STALE_AUTHORITY_HEAD"],
    )
    rc = bootstrap_module.main(
        [
            "--truth_root",
            str(canonical_truth_root),
            "--operator_input_root",
            str(operator_input_root),
            "--environment",
            "PAPER",
            "--seed_usd",
            "5000000.00",
        ]
    )
    assert rc == 2
    payload = json.loads(resolve_paper_session_bootstrap_path(truth_root=canonical_truth_root, day_utc=DAY).read_text(encoding="utf-8"))
    paper_authority_payload = json.loads(
        resolve_paper_session_authority_path(truth_root=canonical_truth_root, day_utc=DAY).read_text(encoding="utf-8")
    )
    assert payload["bootstrap_status"] == "BLOCKED"
    assert payload["required_prerequisites_status"]["status"] == "FAIL"
    assert "ROLLOVER_FAILED_STALE_AUTHORITY_HEAD" in payload["blocker_chain"]
    assert payload["canonical_stop_surface"] == "pre_open_bundle_v1"
    assert payload["canonical_stop_reason_codes"] == ["ROLLOVER_FAILED_STALE_AUTHORITY_HEAD"]
    assert paper_authority_payload["authority_status"] == "DENIED"
    assert paper_authority_payload["blocking_reason_codes"] == ["ROLLOVER_FAILED_STALE_AUTHORITY_HEAD"]


def test_bootstrap_stops_before_activation_when_promotion_blocks(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    canonical_truth_root = tmp_path / "truth"
    sleeve_truth_root = tmp_path / "truth_sleeves" / "PRIMARY" / "PAPER"
    operator_input_root = tmp_path / "operator_root"
    canonical_truth_root.mkdir(parents=True, exist_ok=True)
    sleeve_truth_root.mkdir(parents=True, exist_ok=True)
    operator_input_root.mkdir(parents=True, exist_ok=True)
    called_tools: list[str] = []
    _bootstrap_monkeypatch(
        monkeypatch,
        canonical_truth_root=canonical_truth_root,
        sleeve_truth_root=sleeve_truth_root,
        operator_input_root=operator_input_root,
        promotion_state="BLOCKED",
        promotion_blocked_reason_codes=["REQUIRED_GATE_FAIL"],
        called_tools=called_tools,
    )
    rc = bootstrap_module.main(
        [
            "--truth_root",
            str(canonical_truth_root),
            "--operator_input_root",
            str(operator_input_root),
            "--environment",
            "PAPER",
            "--seed_usd",
            "5000000.00",
        ]
    )
    assert rc == 0
    payload = json.loads(resolve_paper_session_bootstrap_path(truth_root=canonical_truth_root, day_utc=DAY).read_text(encoding="utf-8"))
    promotion_step = next(row for row in payload["step_results"] if row["step_id"] == "session_promotion_gate")
    activation_step = next(row for row in payload["step_results"] if row["step_id"] == "day_activation_readiness")
    assert promotion_step["action"] in {"MATERIALIZED", "REUSED"}
    assert promotion_step["status"] == "BLOCKED"
    assert activation_step["action"] == "SKIPPED"
    assert payload["session_bootstrap"]["promotion_gate"]["promotion_state"] == "BLOCKED"
    assert payload["canonical_stop_surface"] == ""
    assert payload["canonical_stop_artifact_path"] == ""
    assert payload["canonical_stop_reason_codes"] == []
    assert payload["day_activation_ready"] is False
    assert payload["bootstrap_status"] == "READY"
    assert payload["required_prerequisites_status"]["status"] == "PASS"
    assert payload["runtime_prerequisite_verification"]["stage"] in {"PROMOTION", "ACTIVATION"}
    earliest = payload["runtime_prerequisite_verification"]["earliest_failing_prerequisite"]
    assert earliest["prerequisite_id"] in {"session_promotion_decision_v1", "day_activation_readiness_v1"}
    assert str(bootstrap_module.RUN_SESSION_AUTHORITY_TOOL) in called_tools
    assert str(bootstrap_module.RUN_DAY_ACTIVATION_TOOL) not in called_tools


def test_bootstrap_reuses_existing_artifacts_and_stays_canonical_first(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    canonical_truth_root = tmp_path / "truth"
    sleeve_truth_root = tmp_path / "truth_sleeves" / "PRIMARY" / "PAPER"
    operator_input_root = tmp_path / "operator_root"
    canonical_truth_root.mkdir(parents=True, exist_ok=True)
    sleeve_truth_root.mkdir(parents=True, exist_ok=True)
    operator_input_root.mkdir(parents=True, exist_ok=True)
    _write_json(resolve_paper_capital_seed_path(operator_input_root=operator_input_root, day_utc=DAY), _seed_payload())
    _write_json(resolve_operator_statement_path(operator_input_root=operator_input_root, day_utc=DAY), _statement_payload())
    payload = _kill_switch_payload()
    _write_json(canonical_truth_root / "risk_v1" / "kill_switch_v1" / DAY / "global_kill_switch_state.v1.json", payload)
    _write_json(sleeve_truth_root / "risk_v1" / "kill_switch_v1" / DAY / "global_kill_switch_state.v1.json", payload)
    _bootstrap_runtime_artifacts(
        canonical_truth_root=canonical_truth_root,
        sleeve_truth_root=sleeve_truth_root,
        complete=True,
        include_admission=True,
    )
    _bootstrap_monkeypatch(
        monkeypatch,
        canonical_truth_root=canonical_truth_root,
        sleeve_truth_root=sleeve_truth_root,
        operator_input_root=operator_input_root,
    )
    rc = bootstrap_module.main(
        [
            "--day_utc",
            DAY,
            "--truth_root",
            str(canonical_truth_root),
            "--operator_input_root",
            str(operator_input_root),
            "--environment",
            "PAPER",
        ]
    )
    assert rc == 0
    payload = json.loads(resolve_paper_session_bootstrap_path(truth_root=canonical_truth_root, day_utc=DAY).read_text(encoding="utf-8"))
    startup_materialization_payload = json.loads(
        (canonical_truth_root / "reports" / "startup_materialization_v1" / DAY / "startup_materialization.v1.json").read_text(
            encoding="utf-8"
        )
    )
    assert "global_kill_switch" in payload["reused_steps"]
    assert payload["truth_roots"]["canonical_truth_root"] == str(canonical_truth_root)
    assert payload["truth_roots"]["sleeve_truth_root"] == str(sleeve_truth_root)
    assert payload["shared_control_state"]["canonical_kill_switch"]["sha256"] == payload["shared_control_state"]["sleeve_kill_switch_projection"]["sha256"]
    assert startup_materialization_payload["materialization_scope"] == "BOOTSTRAP_SESSION_PREREQUISITES"


def test_bootstrap_separates_production_only_gaps_from_required_ready(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    canonical_truth_root = tmp_path / "truth"
    sleeve_truth_root = tmp_path / "truth_sleeves" / "PRIMARY" / "PAPER"
    operator_input_root = tmp_path / "operator_root"
    canonical_truth_root.mkdir(parents=True, exist_ok=True)
    sleeve_truth_root.mkdir(parents=True, exist_ok=True)
    operator_input_root.mkdir(parents=True, exist_ok=True)
    _bootstrap_monkeypatch(
        monkeypatch,
        canonical_truth_root=canonical_truth_root,
        sleeve_truth_root=sleeve_truth_root,
        operator_input_root=operator_input_root,
        production_ready=False,
    )
    rc = bootstrap_module.main(
        [
            "--day_utc",
            DAY,
            "--truth_root",
            str(canonical_truth_root),
            "--operator_input_root",
            str(operator_input_root),
            "--environment",
            "PAPER",
            "--seed_usd",
            "5000000.00",
        ]
    )
    assert rc == 0
    payload = json.loads(resolve_paper_session_bootstrap_path(truth_root=canonical_truth_root, day_utc=DAY).read_text(encoding="utf-8"))
    paper_authority_payload = json.loads(
        resolve_paper_session_authority_path(truth_root=canonical_truth_root, day_utc=DAY).read_text(encoding="utf-8")
    )
    assert payload["bootstrap_status"] == "READY"
    assert payload["required_prerequisites_status"]["status"] == "PASS"
    assert payload["production_only_prerequisites_status"]["status"] == "UNMET"
    assert "PARTIAL_BUILD" in payload["production_only_prerequisites_status"]["unmet"]
    assert payload["bootstrap_semantic_status"] == "READY_PAPER_ONLY"
    assert payload["smoke_submit_allowed"] is True
    assert paper_authority_payload["authority_status"] == "GRANTED"
    assert paper_authority_payload["paper_open_allowed"] is True
    assert paper_authority_payload["degraded_mode"] is True
    assert "PARTIAL_BUILD" in [row["reason_code"] for row in paper_authority_payload["advisory_checks"]]


def test_bootstrap_evaluation_phase_blocks_when_materialized_authorization_verdict_fails(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    canonical_truth_root = tmp_path / "truth"
    sleeve_truth_root = tmp_path / "truth_sleeves" / "PRIMARY" / "PAPER"
    operator_input_root = tmp_path / "operator_root"
    canonical_truth_root.mkdir(parents=True, exist_ok=True)
    sleeve_truth_root.mkdir(parents=True, exist_ok=True)
    operator_input_root.mkdir(parents=True, exist_ok=True)
    _bootstrap_monkeypatch(
        monkeypatch,
        canonical_truth_root=canonical_truth_root,
        sleeve_truth_root=sleeve_truth_root,
        operator_input_root=operator_input_root,
        authorization_status="FAIL",
        authorization_reason_codes=["AUTHORIZATION_GATE_DENY_TEST"],
        startup_convergence_status="SUCCESS",
    )
    rc = bootstrap_module.main(
        [
            "--day_utc",
            DAY,
            "--truth_root",
            str(canonical_truth_root),
            "--operator_input_root",
            str(operator_input_root),
            "--environment",
            "PAPER",
            "--seed_usd",
            "5000000.00",
        ]
    )
    assert rc == 2
    payload = json.loads(resolve_paper_session_bootstrap_path(truth_root=canonical_truth_root, day_utc=DAY).read_text(encoding="utf-8"))
    startup_materialization_payload = json.loads(
        (canonical_truth_root / "reports" / "startup_materialization_v1" / DAY / "startup_materialization.v1.json").read_text(
            encoding="utf-8"
        )
    )
    assert payload["startup_materialization_phase"]["status"] == "COMPLETE"
    assert payload["evaluation_phase"]["status"] == "BLOCKED"
    assert payload["activation_phase"]["status"] == "BLOCKED"
    assert payload["root_blocker_class"] == "EVALUATION_SAFETY"
    assert payload["shared_control_state"]["canonical_kill_switch"]["state"] == "ACTIVE"
    assert payload["bootstrap_status"] == "BLOCKED"
    assert payload["smoke_submit_allowed"] is False
    assert "CANONICAL_KILL_SWITCH_ACTIVE" in payload["blocker_chain"]
    assert "AUTHORIZATION_GATE_DENY_TEST" not in payload["blocker_chain"]
    assert startup_materialization_payload["status"] == "SUCCESS"
    assert startup_materialization_payload["phase_summary"]["phase_status"] == "COMPLETE"
    ledger_rows = [
        json.loads(line)
        for line in resolve_runtime_ledger_path(truth_root=canonical_truth_root, day_utc=DAY).read_text(encoding="utf-8").splitlines()
    ]
    assert "KILL_SWITCH_EVALUATED_ACTIVE" in [row["event_type"] for row in ledger_rows]


def test_bootstrap_activation_phase_blocks_after_materialization_and_evaluation_pass(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    canonical_truth_root = tmp_path / "truth"
    sleeve_truth_root = tmp_path / "truth_sleeves" / "PRIMARY" / "PAPER"
    operator_input_root = tmp_path / "operator_root"
    canonical_truth_root.mkdir(parents=True, exist_ok=True)
    sleeve_truth_root.mkdir(parents=True, exist_ok=True)
    operator_input_root.mkdir(parents=True, exist_ok=True)
    _bootstrap_monkeypatch(
        monkeypatch,
        canonical_truth_root=canonical_truth_root,
        sleeve_truth_root=sleeve_truth_root,
        operator_input_root=operator_input_root,
        activation_complete=False,
    )
    rc = bootstrap_module.main(
        [
            "--day_utc",
            DAY,
            "--truth_root",
            str(canonical_truth_root),
            "--operator_input_root",
            str(operator_input_root),
            "--environment",
            "PAPER",
            "--seed_usd",
            "5000000.00",
        ]
    )
    assert rc == 0
    payload = json.loads(resolve_paper_session_bootstrap_path(truth_root=canonical_truth_root, day_utc=DAY).read_text(encoding="utf-8"))
    paper_authority_payload = json.loads(
        resolve_paper_session_authority_path(truth_root=canonical_truth_root, day_utc=DAY).read_text(encoding="utf-8")
    )
    assert payload["startup_materialization_phase"]["status"] == "COMPLETE"
    assert payload["evaluation_phase"]["status"] == "READY"
    assert payload["activation_phase"]["status"] == "BLOCKED"
    assert payload["root_blocker_class"] == "ACTIVATION"
    assert payload["bootstrap_status"] == "READY"
    assert payload["required_prerequisites_status"]["status"] == "PASS"
    assert payload["day_activation_ready"] is False
    assert payload["smoke_submit_allowed"] is False
    assert paper_authority_payload["authority_status"] == "GRANTED"
    assert paper_authority_payload["submission_authorized"] is False


def test_bootstrap_non_trading_day_blocks_with_session_authority_not_nav_budget(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    canonical_truth_root = tmp_path / "truth"
    sleeve_truth_root = tmp_path / "truth_sleeves" / "PRIMARY" / "PAPER"
    operator_input_root = tmp_path / "operator_root"
    canonical_truth_root.mkdir(parents=True, exist_ok=True)
    sleeve_truth_root.mkdir(parents=True, exist_ok=True)
    operator_input_root.mkdir(parents=True, exist_ok=True)

    called_tools: list[str] = []
    _bootstrap_monkeypatch(
        monkeypatch,
        canonical_truth_root=canonical_truth_root,
        sleeve_truth_root=sleeve_truth_root,
        operator_input_root=operator_input_root,
        called_tools=called_tools,
    )
    monkeypatch.setattr(
        bootstrap_module,
        "_session_day_blocker_from_market_calendar",
        lambda canonical_truth_root, day_utc: (
            "NON_TRADING_DAY",
            str(Path(canonical_truth_root) / "market_calendar_v1" / "NYSE" / "2026.jsonl"),
        ),
    )

    rc = bootstrap_module.main(
        [
            "--day_utc",
            DAY,
            "--truth_root",
            str(canonical_truth_root),
            "--operator_input_root",
            str(operator_input_root),
            "--environment",
            "PAPER",
            "--seed_usd",
            "5000000.00",
        ]
    )
    assert rc == 2

    payload = json.loads(
        resolve_paper_session_bootstrap_path(truth_root=canonical_truth_root, day_utc=DAY).read_text(encoding="utf-8")
    )
    assert payload["bootstrap_status"] == "BLOCKED"
    assert payload["required_prerequisites_status"]["status"] == "FAIL"
    assert payload["required_prerequisites_status"]["unmet"] == ["NON_TRADING_DAY"]
    assert payload["runtime_prerequisite_verification"]["status"] == "BLOCKED"
    assert payload["runtime_prerequisite_verification"]["earliest_failing_prerequisite"]["reason_codes"] == [
        "NON_TRADING_DAY"
    ]
    assert payload["canonical_stop_surface"] == "market_calendar_day"
    assert payload["canonical_stop_reason_codes"] == ["NON_TRADING_DAY"]
    assert str(payload["canonical_stop_artifact_path"]).endswith("market_calendar_v1/NYSE/2026.jsonl")
    assert "B2_NAV_TOTAL_MISSING_OR_INVALID" not in payload["blocker_chain"]
    assert "AUTHZ_MISSING_EXPOSURE_BUDGET_NAV_TOTAL_CENTS" not in payload["blocker_chain"]
    assert str(bootstrap_module.RUN_CAPITAL_RISK_ENVELOPE_TOOL) not in called_tools
    assert str(bootstrap_module.RUN_PRE_OPEN_MATERIALIZER_TOOL) not in called_tools
