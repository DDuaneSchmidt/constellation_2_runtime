from __future__ import annotations

import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[3]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from ops.tools.aegis_producer_contract_v1 import producer_contract_v1  # noqa: E402


def test_producer_contract_fingerprint_ignores_generated_time(tmp_path: Path) -> None:
    source = tmp_path / "input.json"
    source.write_text('{"ok":true}\n', encoding="utf-8")
    first = producer_contract_v1(
        producer_name="ops/tools/run_aegis_day_v1.py",
        producer_command="python3 ops/tools/run_aegis_day_v1.py --day_utc 2026-04-29 --environment PAPER",
        input_artifacts=[source],
        output_artifacts=[tmp_path / "out.json"],
        schema_versions={"x": "v1"},
    )
    second = producer_contract_v1(
        producer_name="ops/tools/run_aegis_day_v1.py",
        producer_command="python3 ops/tools/run_aegis_day_v1.py --day_utc 2026-04-29 --environment PAPER",
        input_artifacts=[source],
        output_artifacts=[tmp_path / "out.json"],
        schema_versions={"x": "v1"},
    )

    assert first["deterministic_fingerprint"] == second["deterministic_fingerprint"]


def test_authoritative_producers_attach_contracts() -> None:
    producers = [
        "ops/tools/run_aegis_day_v1.py",
        "ops/tools/run_market_open_data_gate_v1.py",
        "ops/tools/run_market_data_supply_v1.py",
        "ops/tools/run_broker_supply_v1.py",
        "ops/tools/run_capital_supply_v1.py",
        "ops/tools/run_risk_budget_supply_v1.py",
        "ops/tools/run_authorization_supply_v1.py",
        "ops/tools/run_submit_boundary_status_v1.py",
        "ops/tools/run_trading_day_control_plane_v1.py",
        "ops/tools/run_aegis_requirement_graph_v1.py",
    ]
    for relpath in producers:
        text = (REPO_ROOT / relpath).read_text(encoding="utf-8")
        assert "attach_producer_contract_v1" in text
