from __future__ import annotations

import importlib.util
import sys
from pathlib import Path


REPO_ROOT = Path("/home/node/constellation").resolve()


def _load_module_without_repo_on_syspath(module_name: str, script_path: Path):
    spec = importlib.util.spec_from_file_location(module_name, script_path)
    if spec is None or spec.loader is None:
        raise AssertionError(f"unable to load spec for {script_path}")
    module = importlib.util.module_from_spec(spec)
    original = list(sys.path)
    try:
        sys.path = [entry for entry in sys.path if Path(entry or ".").resolve() != REPO_ROOT]
        spec.loader.exec_module(module)
    finally:
        sys.path = original
    return module


def test_feed_attestation_accepts_authoritative_sleeve_truth_root(tmp_path, monkeypatch) -> None:
    from ops.tools import run_feed_attestation_gate_v1 as module

    sleeve_truth_root = tmp_path / "constellation_2" / "runtime" / "truth_sleeves" / "PRIMARY" / "PAPER"
    sleeve_truth_root.mkdir(parents=True)

    def _reject_contract(_path):
        raise ValueError("outside active runtime contract")

    monkeypatch.setattr(module, "require_truth_root_under_contract", _reject_contract)
    monkeypatch.setattr(module, "resolve_authoritative_repo_root_v1", lambda _repo_root=None: tmp_path)

    assert module._resolve_truth_root(str(sleeve_truth_root)) == sleeve_truth_root.resolve()


def test_liquidity_gate_accepts_authoritative_sleeve_truth_root(tmp_path, monkeypatch) -> None:
    from ops.tools import run_liquidity_slippage_gate_v1 as module

    sleeve_truth_root = tmp_path / "constellation_2" / "runtime" / "truth_sleeves" / "PRIMARY" / "PAPER"
    sleeve_truth_root.mkdir(parents=True)

    def _reject_contract(_path):
        raise ValueError("outside active runtime contract")

    monkeypatch.setattr(module, "require_truth_root_under_contract", _reject_contract)
    monkeypatch.setattr(module, "resolve_authoritative_repo_root_v1", lambda _repo_root=None: tmp_path)

    assert module._resolve_gate_truth_root(str(sleeve_truth_root)) == sleeve_truth_root.resolve()


def test_heartbeat_gate_bootstraps_repo_root_before_constellation_import() -> None:
    script_path = REPO_ROOT / "ops" / "tools" / "run_heartbeat_gate_v1.py"
    module = _load_module_without_repo_on_syspath(
        "heartbeat_gate_bootstrap_probe_v1",
        script_path,
    )
    assert str(REPO_ROOT) in sys.path
    assert hasattr(module, "_resolve_truth_root")


def test_heartbeat_gate_accepts_authoritative_sleeve_truth_root(tmp_path, monkeypatch) -> None:
    from ops.tools import run_heartbeat_gate_v1 as module

    sleeve_truth_root = tmp_path / "constellation_2" / "runtime" / "truth_sleeves" / "PRIMARY" / "PAPER"
    sleeve_truth_root.mkdir(parents=True)

    def _reject_contract(_path):
        raise ValueError("outside active runtime contract")

    monkeypatch.setattr(module, "require_truth_root_under_contract", _reject_contract)
    monkeypatch.setattr(module, "resolve_authoritative_repo_root_v1", lambda _repo_root=None: tmp_path)

    assert module._resolve_truth_root(str(sleeve_truth_root)) == sleeve_truth_root.resolve()
