from __future__ import annotations

import hashlib
import json
import sys
import tempfile
from pathlib import Path
from unittest.mock import patch

SOURCE_ROOT = Path("/home/node/constellation")
if str(SOURCE_ROOT) not in sys.path:
    sys.path.insert(0, str(SOURCE_ROOT))

import ops.tools.run_liquidity_slippage_gate_v1 as gate_module


def _write_json(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, sort_keys=True, separators=(",", ":")) + "\n", encoding="utf-8")


def _write_market_data(truth_root: Path, *, symbol: str, day_rows: list[tuple[str, str]]) -> None:
    md_root = truth_root / "market_data_snapshot_v1"
    year_path = md_root / symbol / "2026.jsonl"
    year_path.parent.mkdir(parents=True, exist_ok=True)
    payload = "\n".join(
        json.dumps(
            {
                "dataset_version": "v1",
                "symbol": symbol,
                "timestamp_utc": f"{day}T00:00:00Z",
                "open": close,
                "high": close,
                "low": close,
                "close": close,
                "volume": 1000000,
                "source_name": "TEST",
                "source_hash": "a" * 64,
                "ingested_utc": "2026-04-10T00:00:00Z",
            },
            sort_keys=True,
            separators=(",", ":"),
        )
        for day, close in day_rows
    ) + "\n"
    year_path.write_text(payload, encoding="utf-8")
    _write_json(
        md_root / "dataset_manifest.json",
        {
            "created_utc": "2026-04-10T00:00:00Z",
            "source_snapshot_utc": "2026-04-10T00:00:00Z",
            "dataset_version": "v1",
            "date_range": {"start": day_rows[0][0], "end": day_rows[-1][0]},
            "files": [
                {
                    "symbol": symbol,
                    "year": 2026,
                    "file": f"{symbol}/2026.jsonl",
                    "sha256": hashlib.sha256(payload.encode("utf-8")).hexdigest(),
                }
            ],
            "symbols": [symbol],
            "global_hash": "b" * 64,
        },
    )


def _write_intent(truth_root: Path, day_utc: str) -> None:
    _write_json(
        truth_root / "intents_v1" / "snapshots" / day_utc / "spy.exposure_intent.v1.json",
        {
            "schema_id": "exposure_intent",
            "schema_version": "v1",
            "intent_id": f"intent:SPY:{day_utc}",
            "underlying": {"symbol": "SPY"},
            "target_notional_pct": "0.01",
        },
    )


def _write_nav(truth_root: Path, day_utc: str, *, nav_total: int) -> None:
    _write_json(
        truth_root / "accounting_v2" / "nav" / day_utc / "nav.v2.json",
        {
            "schema_id": "C2_ACCOUNTING_NAV_V2",
            "schema_version": 2,
            "day_utc": day_utc,
            "status": "ACTIVE",
            "nav": {
                "currency": "USD",
                "nav_total": nav_total,
                "cash_total": nav_total,
                "gross_positions_value": 0,
                "realized_pnl_to_date": 0,
                "unrealized_pnl": 0,
                "components": [],
                "notes": [],
            },
            "history": {},
        },
    )


def _write_stale_gate(truth_root: Path, day_utc: str) -> Path:
    path = truth_root / "reports" / "liquidity_slippage_gate_v1" / day_utc / "liquidity_slippage_gate.v1.json"
    _write_json(
        path,
        {
            "schema_id": "liquidity_slippage_gate",
            "schema_version": "v1",
            "day_utc": day_utc,
            "produced_utc": f"{day_utc}T00:00:00Z",
            "producer": {"repo": "constellation", "module": "test", "git_sha": "a" * 40},
            "status": "PASS",
            "reason_codes": ["LIQPOL_PASS"],
            "input_manifest": [
                {"type": "accounting_nav", "path": "/tmp/stale_nav.v2.json", "sha256": "0" * 64},
                {"type": "intent", "path": "/tmp/intent.json", "sha256": "1" * 64},
            ],
            "policy": {
                "path": "/tmp/policy.json",
                "sha256": "2" * 64,
                "schema_path": "/tmp/schema.json",
                "schema_sha256": "3" * 64,
            },
            "results": {
                "per_intent": [
                    {
                        "intent_hash": "4" * 64,
                        "engine_id": "C2_TREND_EQ_PRIMARY_V1",
                        "symbol": "SPY",
                        "decision": "SKIP",
                        "reason_codes": ["LIQPOL_NOTIONAL_ZERO"],
                        "metrics": {
                            "nav_total_cents": 0,
                            "target_notional_pct": "0.010000",
                            "est_notional_usd": "0.00",
                            "close": "0.00",
                            "est_shares": 0,
                            "adv_shares": 0,
                            "adv_dollar": "0.00",
                            "participation_pct_adv": "0.000000",
                            "est_slippage_bps": "0.00",
                            "caps": {
                                "max_participation_pct_adv": "0.010000",
                                "max_est_slippage_bps": "25.00",
                                "max_notional_per_symbol_usd": "25000",
                            },
                        },
                    }
                ],
                "totals": {"intents_total": 1, "intents_failed": 0, "intents_passed": 0, "intents_skipped": 1},
            },
            "gate_sha256": "5" * 64,
        },
    )
    return path


def test_liquidity_gate_replaces_stale_existing_output_when_inputs_change() -> None:
    with tempfile.TemporaryDirectory() as td:
        truth_root = Path(td) / "truth"
        day_utc = "2026-04-14"
        _write_market_data(
            truth_root,
            symbol="SPY",
            day_rows=[
                ("2026-03-26", "638.00"),
                ("2026-03-27", "639.00"),
                ("2026-03-30", "640.00"),
                ("2026-04-01", "641.00"),
                ("2026-04-02", "642.00"),
                ("2026-04-03", "643.00"),
                ("2026-04-06", "644.00"),
                ("2026-04-07", "645.00"),
                ("2026-04-08", "646.00"),
                ("2026-04-09", "647.00"),
                ("2026-04-10", "650.00"),
            ],
        )
        _write_intent(truth_root, day_utc)
        _write_nav(truth_root, day_utc, nav_total=100000)
        stale_path = _write_stale_gate(truth_root, day_utc)

        with patch.object(gate_module, "_require_supported_truth_root", return_value=truth_root):
            with patch.object(
                sys,
                "argv",
                [
                    "run_liquidity_slippage_gate_v1.py",
                    "--day_utc",
                    day_utc,
                    "--truth_root",
                    str(truth_root),
                ],
            ):
                rc = gate_module.main()
        assert rc == 0

        payload = json.loads(stale_path.read_text(encoding="utf-8"))
        row = payload["results"]["per_intent"][0]
        assert payload["status"] == "PASS"
        assert row["decision"] == "PASS"
        assert row["metrics"]["close"] == "650.00"
        assert row["metrics"]["nav_total_cents"] == 10000000
