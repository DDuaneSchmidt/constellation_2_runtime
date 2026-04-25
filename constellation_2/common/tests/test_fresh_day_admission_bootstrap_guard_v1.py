from __future__ import annotations

from pathlib import Path
import sys
from types import SimpleNamespace

SOURCE_ROOT = Path("/home/node/constellation")
if str(SOURCE_ROOT) not in sys.path:
    sys.path.insert(0, str(SOURCE_ROOT))

import constellation_2.common.fresh_day_admission_v1 as fresh_day_module
from constellation_2.common.runtime_path_authority_v1 import RuntimePathAuthorityV1


def _write_json(path: Path, payload: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(payload + "\n", encoding="utf-8")


def test_bootstrap_admission_allows_first_day_without_capital_risk_pass(monkeypatch, tmp_path: Path) -> None:
    repo_root = (tmp_path / "repo").resolve()
    truth_root = (tmp_path / "truth").resolve()
    truth_sleeves_root = (tmp_path / "truth_sleeves").resolve()
    target_day = "2026-05-02"

    seed_path = repo_root / "constellation_2" / "operator_inputs" / "paper_capital_seed_v1" / target_day / "paper_capital_seed.v1.json"
    _write_json(
        seed_path,
        '{"schema_id":"C2_PAPER_CAPITAL_SEED","schema_version":1,"day_utc":"2026-05-02","environment":"PAPER","cash_total":"5000000.00","nlv_total":"5000000.00"}',
    )
    statement_path = (
        repo_root / "constellation_2" / "operator_inputs" / "cash_ledger_operator_statements" / target_day / "operator_statement.v1.json"
    )
    _write_json(statement_path, '{"account_id":"DU1234567","cash_total":"5000000.00","nlv_total":"5000000.00"}')

    monkeypatch.setattr(
        fresh_day_module,
        "load_runtime_path_authority_bridge_v1",
        lambda **kwargs: RuntimePathAuthorityV1(
            authoritative_repo_root=repo_root,
            canonical_runtime_truth_root=truth_root,
            canonical_runtime_truth_sleeves_root=truth_sleeves_root,
            authoritative_repo_truth_root=truth_root,
            authoritative_repo_truth_sleeves_root=truth_sleeves_root,
            active_release_root=(tmp_path / "release").resolve(),
        ),
    )
    monkeypatch.setattr(
        fresh_day_module,
        "read_control_plane_surface_v1",
        lambda **kwargs: SimpleNamespace(path=Path("/tmp/unused"), sha256="f" * 64, payload={"status": "FAIL"}),
    )

    result = fresh_day_module.evaluate_paper_bootstrap_admission_v1(
        repo_root=repo_root,
        target_day_utc=target_day,
        environment="PAPER",
    )

    assert result["eligible"] is True
    assert result["blocking_reason_codes"] == []
    artifact_ids = {str(row.get("artifact_id") or "") for row in result["artifacts"]}
    assert artifact_ids == {"paper_capital_seed_v1", "operator_statement_v1"}


def test_bootstrap_admission_fails_closed_when_prior_day_continuity_exists(monkeypatch, tmp_path: Path) -> None:
    repo_root = (tmp_path / "repo").resolve()
    truth_root = (tmp_path / "truth").resolve()
    truth_sleeves_root = (tmp_path / "truth_sleeves").resolve()
    target_day = "2026-05-02"

    seed_path = repo_root / "constellation_2" / "operator_inputs" / "paper_capital_seed_v1" / target_day / "paper_capital_seed.v1.json"
    _write_json(
        seed_path,
        '{"schema_id":"C2_PAPER_CAPITAL_SEED","schema_version":1,"day_utc":"2026-05-02","environment":"PAPER","cash_total":"5000000.00","nlv_total":"5000000.00"}',
    )
    statement_path = (
        repo_root / "constellation_2" / "operator_inputs" / "cash_ledger_operator_statements" / target_day / "operator_statement.v1.json"
    )
    _write_json(statement_path, '{"account_id":"DU1234567","cash_total":"5000000.00","nlv_total":"5000000.00"}')

    prior_build_path = (
        truth_root / "reports" / "economic_state_build_v1" / "2026-05-01" / "abc123" / "economic_state_build.v1.json"
    )
    _write_json(prior_build_path, '{"closure_status":"COMPLETE"}')

    monkeypatch.setattr(
        fresh_day_module,
        "load_runtime_path_authority_bridge_v1",
        lambda **kwargs: RuntimePathAuthorityV1(
            authoritative_repo_root=repo_root,
            canonical_runtime_truth_root=truth_root,
            canonical_runtime_truth_sleeves_root=truth_sleeves_root,
            authoritative_repo_truth_root=truth_root,
            authoritative_repo_truth_sleeves_root=truth_sleeves_root,
            active_release_root=(tmp_path / "release").resolve(),
        ),
    )

    result = fresh_day_module.evaluate_paper_bootstrap_admission_v1(
        repo_root=repo_root,
        target_day_utc=target_day,
        environment="PAPER",
    )

    assert result["eligible"] is False
    assert result["blocking_reason_codes"] == ["PAPER_BOOTSTRAP_PRIOR_DAY_CONTINUITY_PRESENT"]

