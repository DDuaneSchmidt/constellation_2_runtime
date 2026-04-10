from __future__ import annotations

from pathlib import Path
import sys

SOURCE_ROOT = Path("/home/node/constellation")
if str(SOURCE_ROOT) not in sys.path:
    sys.path.insert(0, str(SOURCE_ROOT))

import constellation_2.common.fresh_day_admission_v1 as fresh_day_module
from constellation_2.common.runtime_path_authority_v1 import RuntimePathAuthorityV1


class _Ref:
    def __init__(self, path: Path, payload: dict[str, object]) -> None:
        self.path = path
        self.payload = payload
        self.sha256 = "f" * 64
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.path.write_text("{}", encoding="utf-8")


def test_fresh_day_admission_admits_when_required_target_day_artifacts_are_green(monkeypatch, tmp_path: Path) -> None:
    truth_root = (tmp_path / "truth").resolve()
    target_day = "2026-04-09"
    env = "PAPER"
    acct = "DU1234567"

    monkeypatch.setattr(fresh_day_module, "resolve_decision_truth_root_v1", lambda truth_root, repo_root=None: Path(truth_root).resolve())
    monkeypatch.setattr(
        fresh_day_module,
        "resolve_runtime_path_authority_snapshot_v1",
        lambda repo_root=None: {
            "authoritative_repo_root": str(tmp_path / "repo"),
            "canonical_runtime_truth_root": str(truth_root),
            "canonical_runtime_truth_sleeves_root": str(tmp_path / "truth_sleeves"),
            "authoritative_repo_truth_root": str(tmp_path / "repo" / "constellation_2" / "runtime" / "truth"),
            "authoritative_repo_truth_sleeves_root": str(tmp_path / "repo" / "constellation_2" / "runtime" / "truth_sleeves"),
            "active_release_root": str(tmp_path / "release"),
        },
    )
    monkeypatch.setattr(
        fresh_day_module,
        "load_runtime_path_authority_v1",
        lambda repo_root=None: RuntimePathAuthorityV1(
            authoritative_repo_root=(tmp_path / "repo").resolve(),
            canonical_runtime_truth_root=truth_root,
            canonical_runtime_truth_sleeves_root=(tmp_path / "truth_sleeves").resolve(),
            authoritative_repo_truth_root=(tmp_path / "repo" / "constellation_2" / "runtime" / "truth").resolve(),
            authoritative_repo_truth_sleeves_root=(tmp_path / "repo" / "constellation_2" / "runtime" / "truth_sleeves").resolve(),
            active_release_root=(tmp_path / "release").resolve(),
        ),
    )
    monkeypatch.setattr(
        fresh_day_module,
        "resolve_release_provenance",
        lambda: {"release_id": "r1", "git_sha": "a" * 40},
    )

    paper_path = fresh_day_module.resolve_paper_policy_verdict_path(truth_root=truth_root, day_utc=target_day)
    trade_path = truth_root / "trade_submit_readiness_c2_v1" / "_history" / env / acct / target_day / "status.json"
    trading_path = fresh_day_module.resolve_trading_day_state_machine_path(truth_root=truth_root, day_utc=target_day)
    probe_path = fresh_day_module.resolve_next_day_readiness_probe_path(truth_root=truth_root, target_day_utc=target_day)

    refs = {
        str(probe_path): _Ref(probe_path, {"probe_status": "BLOCKED"}),
        str(paper_path): _Ref(paper_path, {"overall_status": "PASS"}),
        str(trade_path): _Ref(trade_path, {"state": "OK", "ok": True}),
        str(trading_path): _Ref(trading_path, {"final_start_decision": "READY_NOW"}),
    }
    monkeypatch.setattr(
        fresh_day_module,
        "_read_optional_surface",
        lambda path, schema_relpath: refs.get(str(Path(path).resolve())),
    )

    payload = fresh_day_module.derive_fresh_day_admission_payload(
        repo_root=Path("/home/node/constellation"),
        truth_root=truth_root,
        target_day_utc=target_day,
        environment=env,
        ib_account=acct,
    )

    assert payload["admission_status"] == "ADMIT"
    assert payload["probe_status"] == "BLOCKED"
    assert payload["blocking_items"] == []


def test_fresh_day_admission_blocks_when_target_day_artifacts_are_missing_or_blocked(monkeypatch, tmp_path: Path) -> None:
    truth_root = (tmp_path / "truth").resolve()
    target_day = "2026-04-10"
    env = "PAPER"
    acct = "DU1234567"

    monkeypatch.setattr(fresh_day_module, "resolve_decision_truth_root_v1", lambda truth_root, repo_root=None: Path(truth_root).resolve())
    monkeypatch.setattr(
        fresh_day_module,
        "resolve_runtime_path_authority_snapshot_v1",
        lambda repo_root=None: {
            "authoritative_repo_root": str(tmp_path / "repo"),
            "canonical_runtime_truth_root": str(truth_root),
            "canonical_runtime_truth_sleeves_root": str(tmp_path / "truth_sleeves"),
            "authoritative_repo_truth_root": str(tmp_path / "repo" / "constellation_2" / "runtime" / "truth"),
            "authoritative_repo_truth_sleeves_root": str(tmp_path / "repo" / "constellation_2" / "runtime" / "truth_sleeves"),
            "active_release_root": str(tmp_path / "release"),
        },
    )
    monkeypatch.setattr(
        fresh_day_module,
        "load_runtime_path_authority_v1",
        lambda repo_root=None: RuntimePathAuthorityV1(
            authoritative_repo_root=(tmp_path / "repo").resolve(),
            canonical_runtime_truth_root=truth_root,
            canonical_runtime_truth_sleeves_root=(tmp_path / "truth_sleeves").resolve(),
            authoritative_repo_truth_root=(tmp_path / "repo" / "constellation_2" / "runtime" / "truth").resolve(),
            authoritative_repo_truth_sleeves_root=(tmp_path / "repo" / "constellation_2" / "runtime" / "truth_sleeves").resolve(),
            active_release_root=(tmp_path / "release").resolve(),
        ),
    )
    monkeypatch.setattr(
        fresh_day_module,
        "resolve_release_provenance",
        lambda: {"release_id": "r1", "git_sha": "a" * 40},
    )

    probe_path = fresh_day_module.resolve_next_day_readiness_probe_path(truth_root=truth_root, target_day_utc=target_day)
    refs = {
        str(probe_path): _Ref(probe_path, {"probe_status": "BLOCKED"}),
    }
    monkeypatch.setattr(
        fresh_day_module,
        "_read_optional_surface",
        lambda path, schema_relpath: refs.get(str(Path(path).resolve())),
    )

    payload = fresh_day_module.derive_fresh_day_admission_payload(
        repo_root=Path("/home/node/constellation"),
        truth_root=truth_root,
        target_day_utc=target_day,
        environment=env,
        ib_account=acct,
    )

    assert payload["admission_status"] == "BLOCKED"
    assert payload["probe_status"] == "BLOCKED"
    assert {row["artifact_id"] for row in payload["missing_required_artifacts"]} == {
        "paper_policy_verdict_v1",
        "trade_submit_readiness_c2_v1",
        "trading_day_state_machine_v1",
    }
