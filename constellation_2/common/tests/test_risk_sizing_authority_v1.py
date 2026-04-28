from __future__ import annotations

import json
import sys
from pathlib import Path

SOURCE_ROOT = Path("/home/node/constellation")
if str(SOURCE_ROOT) not in sys.path:
    sys.path.insert(0, str(SOURCE_ROOT))

from constellation_2.common.risk_sizing_authority_v1 import evaluate_risk_sizing_authority_v1


DAY = "2026-04-27"
IH = "1" * 64
SID = "2" * 64


def _write_json(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, sort_keys=True) + "\n", encoding="utf-8")


def _base(root: Path, *, target_pct: str = "0.01", allowed: int = 20000, final: int = 10000, qty: int = 1) -> None:
    _write_json(root / "intents_v1" / "snapshots" / DAY / f"{IH}.exposure_intent.v1.json", {"intent_id": "intent", "target_notional_pct": target_pct})
    _write_json(root / "reports" / "portfolio_account_authority_v1" / DAY / "portfolio_account_authority.v1.json", {"account_state": "READY", "account_values": {"net_liquidation_cents": 1000000}})
    _write_json(root / "reports" / "capital_risk_envelope_v2" / DAY / "capital_risk_envelope.v2.json", {"status": "PASS", "envelope": {"allowed_capital_at_risk_cents": allowed, "nav_total_cents": 1000000}})
    _write_json(root / "execution_package_v1" / DAY / SID / "execution_package.v1.json", {"intent_hash": IH, "submission_id": SID, "quantity": qty, "required_risk_cents": final, "risk_per_unit_cents": final})


def test_normal_intent_sized(tmp_path: Path) -> None:
    _base(tmp_path)

    payload = evaluate_risk_sizing_authority_v1(day_utc=DAY, truth_root=tmp_path, execution_root=tmp_path)

    assert payload["risk_sizing_state"] == "SIZED"


def test_size_clipped_by_risk_envelope(tmp_path: Path) -> None:
    _base(tmp_path, target_pct="0.02", allowed=10000, final=10000)

    payload = evaluate_risk_sizing_authority_v1(day_utc=DAY, truth_root=tmp_path, execution_root=tmp_path)

    assert payload["risk_sizing_state"] == "CLIPPED"
    assert payload["sizing_decisions"][0]["requested_risk_cents"] == 20000
    assert payload["sizing_decisions"][0]["final_risk_cents"] == 10000


def test_size_rounded_to_executable_quantity(tmp_path: Path) -> None:
    _base(tmp_path, target_pct="0.015", allowed=50000, final=10000)

    payload = evaluate_risk_sizing_authority_v1(day_utc=DAY, truth_root=tmp_path, execution_root=tmp_path)

    assert payload["risk_sizing_state"] == "ROUNDED"


def test_zero_quantity_explicit(tmp_path: Path) -> None:
    _base(tmp_path, final=0, qty=0)

    payload = evaluate_risk_sizing_authority_v1(day_utc=DAY, truth_root=tmp_path, execution_root=tmp_path)

    assert payload["risk_sizing_state"] == "ZERO_SIZED"
    assert payload["first_blocker"] == "FINAL_QUANTITY_ZERO"


def test_missing_account_data(tmp_path: Path) -> None:
    _write_json(tmp_path / "intents_v1" / "snapshots" / DAY / f"{IH}.exposure_intent.v1.json", {"intent_id": "intent", "target_notional_pct": "0.01"})

    payload = evaluate_risk_sizing_authority_v1(day_utc=DAY, truth_root=tmp_path, execution_root=tmp_path)

    assert payload["risk_sizing_state"] == "ACCOUNT_DATA_MISSING"


def test_final_size_agrees_with_execution_package(tmp_path: Path) -> None:
    _base(tmp_path, final=10000, qty=2)

    payload = evaluate_risk_sizing_authority_v1(day_utc=DAY, truth_root=tmp_path, execution_root=tmp_path)

    assert payload["sizing_decisions"][0]["final_size_agrees_with_execution_package"] is True
    assert payload["sizing_decisions"][0]["final_quantity"] == 2
