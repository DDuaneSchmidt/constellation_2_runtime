from __future__ import annotations

import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[3]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

import constellation_2.common.aegis_authority_graph_v1 as graph_module


DAY = "2026-04-27"


def test_graph_includes_required_authority_nodes(tmp_path: Path) -> None:
    graph = graph_module.build_aegis_authority_graph_v1(day_utc=DAY, truth_root=tmp_path, execution_root=tmp_path, repo_root=REPO_ROOT)
    names = {node["authority_name"] for node in graph["authority_nodes"]}
    assert {
        "portfolio_account_authority_v1",
        "strategy_decision_authority_v1",
        "market_data_authority_v1",
        "risk_sizing_authority_v1",
        "paper_trading_day_authority_v1",
        "runtime_service_authority_v1",
        "execution_mode_authority_v1",
        "execution_lifecycle_authority_v1",
        "trade_lineage_graph_v1",
        "trading_day_closure_authority_v1",
    }.issubset(names)


def test_dependency_order_is_valid(tmp_path: Path) -> None:
    graph = graph_module.build_aegis_authority_graph_v1(day_utc=DAY, truth_root=tmp_path, execution_root=tmp_path, repo_root=REPO_ROOT)
    order = graph["dependency_order"]
    positions = {name: idx for idx, name in enumerate(order)}
    for node in graph["authority_nodes"]:
        for dep in node["dependencies"]:
            assert positions[dep] < positions[node["authority_name"]]


def test_missing_optional_authority_is_diagnostic(tmp_path: Path) -> None:
    graph = graph_module.build_aegis_authority_graph_v1(day_utc=DAY, truth_root=tmp_path, execution_root=tmp_path, repo_root=REPO_ROOT)
    portfolio = next(node for node in graph["authority_nodes"] if node["authority_name"] == "portfolio_account_authority_v1")
    assert portfolio["required_or_diagnostic"] == "diagnostic"
    assert portfolio["status"] == "MISSING"
    assert not portfolio["blocks_daily_outcome"]


def test_missing_required_authority_blocks_with_owner_and_producer(tmp_path: Path) -> None:
    graph = graph_module.build_aegis_authority_graph_v1(day_utc=DAY, truth_root=tmp_path, execution_root=tmp_path, repo_root=REPO_ROOT)
    market = next(node for node in graph["authority_nodes"] if node["authority_name"] == "market_data_authority_v1")
    assert market["required_or_diagnostic"] == "required"
    assert market["blocks_daily_outcome"] is True
    blocker = next(row for row in graph["blocking_nodes"] if row["authority_name"] == "market_data_authority_v1")
    assert blocker["owner"] == "market_data_authority"
    assert "run_market_data_authority_v1.py" in blocker["producer_command"]
