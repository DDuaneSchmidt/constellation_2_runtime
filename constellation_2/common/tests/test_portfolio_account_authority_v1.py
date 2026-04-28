from __future__ import annotations

import json
import sys
from pathlib import Path

SOURCE_ROOT = Path("/home/node/constellation")
if str(SOURCE_ROOT) not in sys.path:
    sys.path.insert(0, str(SOURCE_ROOT))

from constellation_2.common.portfolio_account_authority_v1 import evaluate_portfolio_account_authority_v1


DAY = "2026-04-27"


def _write_json(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, sort_keys=True) + "\n", encoding="utf-8")


def _broker(root: Path, *, observed: str = "2026-04-27T14:00:00Z", cash: int = 10000000) -> None:
    _write_json(
        root / "broker_account_snapshot_v1" / DAY / "broker_account_snapshot.v1.json",
        {"account_id": "DU123", "cash_total_cents": cash, "nlv_total_cents": cash, "observed_at_utc": observed, "currency": "USD"},
    )


def _operator(repo: Path, *, cash: int = 10000000) -> None:
    _write_json(
        repo / "constellation_2" / "operator_inputs" / "cash_ledger_operator_statements" / DAY / "operator_statement.v1.json",
        {"account_id": "DU123", "cash_total_cents": cash, "nlv_total_cents": cash, "observed_at_utc": "2026-04-27T14:00:00Z", "currency": "USD"},
    )


def test_fresh_broker_account_snapshot_ready(tmp_path: Path) -> None:
    _broker(tmp_path)

    payload = evaluate_portfolio_account_authority_v1(day_utc=DAY, truth_root=tmp_path, execution_root=tmp_path, repo_root=tmp_path)

    assert payload["account_state"] == "READY"
    assert payload["account_values"]["cash_total_cents"] == 10000000


def test_stale_snapshot_is_stale(tmp_path: Path) -> None:
    _broker(tmp_path, observed="2026-04-26T14:00:00Z")

    payload = evaluate_portfolio_account_authority_v1(day_utc=DAY, truth_root=tmp_path, execution_root=tmp_path, repo_root=tmp_path)

    assert payload["account_state"] == "STALE"


def test_operator_statement_only_allowed_for_paper(tmp_path: Path) -> None:
    _operator(tmp_path)

    payload = evaluate_portfolio_account_authority_v1(day_utc=DAY, truth_root=tmp_path, execution_root=tmp_path, repo_root=tmp_path)

    assert payload["account_state"] == "OPERATOR_STATEMENT_ONLY"
    assert payload["operator_statement_acceptable"] is True


def test_conflicting_broker_operator_values(tmp_path: Path) -> None:
    _broker(tmp_path, cash=10000000)
    _operator(tmp_path, cash=9000000)

    payload = evaluate_portfolio_account_authority_v1(day_utc=DAY, truth_root=tmp_path, execution_root=tmp_path, repo_root=tmp_path)

    assert payload["account_state"] == "CONFLICTING_ACCOUNT_STATE"
    assert payload["conflicts"][0]["field"] == "cash_total_cents"


def test_fills_require_reconciliation(tmp_path: Path) -> None:
    _broker(tmp_path)
    _write_json(tmp_path / "fill_ledger_v1" / DAY / "abc.fill_ledger.v1.json", {"submission_id": "abc"})

    payload = evaluate_portfolio_account_authority_v1(day_utc=DAY, truth_root=tmp_path, execution_root=tmp_path, repo_root=tmp_path)

    assert payload["account_state"] == "RECONCILIATION_REQUIRED"


def test_values_exposed_for_risk_sizing_with_path(tmp_path: Path) -> None:
    _operator(tmp_path, cash=123456)

    payload = evaluate_portfolio_account_authority_v1(day_utc=DAY, truth_root=tmp_path, execution_root=tmp_path, repo_root=tmp_path)

    assert payload["account_values"]["net_liquidation_cents"] == 123456
    assert any(row["artifact_type"] == "operator_statement" and row["exists"] for row in payload["input_evidence"])
