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
from constellation_2.common.configuration_catalog_v1 import (
    build_active_configuration_v1,
    default_catalog_values_v1,
    write_active_configuration_artifacts_v1,
)


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


def _write_intent(
    truth_root: Path,
    day_utc: str,
    *,
    symbol: str = "SPY",
    engine_id: str | None = None,
    target_notional_pct: str = "0.01",
) -> None:
    payload = {
        "schema_id": "exposure_intent",
        "schema_version": "v1",
        "intent_id": f"intent:{symbol}:{engine_id or 'NO_ENGINE'}:{day_utc}",
        "underlying": {"symbol": symbol},
        "target_notional_pct": target_notional_pct,
    }
    if engine_id:
        payload["engine"] = {"engine_id": engine_id}
    _write_json(
        truth_root / "intents_v1" / "snapshots" / day_utc / f"{symbol.lower()}_{engine_id or 'no_engine'}.exposure_intent.v1.json",
        payload,
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
        assert payload["policy"]["config_version_used"] == "STATIC_GOVERNANCE_BASELINE"


def test_liquidity_gate_consumes_materialized_active_configuration_policy() -> None:
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
        _write_intent(truth_root, day_utc, target_notional_pct="0.01")
        _write_nav(truth_root, day_utc, nav_total=100000)

        values = default_catalog_values_v1(environment="PAPER")
        values["liquidity_max_notional_per_symbol_usd"] = "100"
        active_configuration = build_active_configuration_v1(
            config_version_base="NO_ACTIVE_CONFIGURATION",
            proposed_values=values,
            approved_diff=[
                {
                    "field": "liquidity_max_notional_per_symbol_usd",
                    "current_value": "25000",
                    "proposed_value": "100",
                }
            ],
            activated_by="test",
            truth_root=truth_root,
            runtime_root=truth_root.parent,
            prior_config_version=None,
        )
        write_active_configuration_artifacts_v1(
            active_configuration=active_configuration,
            truth_root=truth_root,
        )

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
        assert rc == 1

        payload = json.loads(
            (
                truth_root
                / "reports"
                / "liquidity_slippage_gate_v1"
                / day_utc
                / "liquidity_slippage_gate.v1.json"
            ).read_text(encoding="utf-8")
        )
        assert payload["status"] == "FAIL"
        assert payload["policy"]["config_version_used"] == active_configuration["config_version"]
        assert payload["policy"]["policy_artifact_used"] == "C2_LIQUIDITY_SLIPPAGE_POLICY_V1"
        assert {
            row["parameter_key"] for row in payload["policy"]["parameter_refs_used"]
        } >= {"liquidity_max_notional_per_symbol_usd"}
        assert payload["results"]["per_intent"][0]["reason_codes"] == ["LIQPOL_NOTIONAL_EXCEEDS_CAP"]


def test_liquidity_gate_skips_zero_cap_non_executable_engines() -> None:
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
        _write_intent(
            truth_root,
            day_utc,
            symbol="DBC",
            engine_id="C2_CROSS_ASSET_TREND_V1",
            target_notional_pct="0.10",
        )
        _write_intent(
            truth_root,
            day_utc,
            symbol="SPY",
            engine_id="C2_TREND_EQ_PRIMARY_V1",
            target_notional_pct="0.01",
        )
        _write_nav(truth_root, day_utc, nav_total=100000)

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
        payload = json.loads(
            (
                truth_root
                / "reports"
                / "liquidity_slippage_gate_v1"
                / day_utc
                / "liquidity_slippage_gate.v1.json"
            ).read_text(encoding="utf-8")
        )
        rows = {row["engine_id"]: row for row in payload["results"]["per_intent"]}
        assert payload["status"] == "PASS"
        assert rows["C2_CROSS_ASSET_TREND_V1"]["decision"] == "SKIP"
        assert rows["C2_CROSS_ASSET_TREND_V1"]["reason_codes"] == [
            "LIQPOL_ENGINE_NOT_EXECUTABLE_BY_CAPITAL_POLICY"
        ]
        assert "zero-cap non-executable" in rows["C2_CROSS_ASSET_TREND_V1"]["recovery_command"]
        assert rows["C2_TREND_EQ_PRIMARY_V1"]["decision"] == "PASS"


def test_liquidity_gate_keeps_real_executable_notional_reject_fail_closed() -> None:
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
        _write_intent(
            truth_root,
            day_utc,
            symbol="SPY",
            engine_id="C2_TREND_EQ_PRIMARY_V1",
            target_notional_pct="0.50",
        )
        _write_nav(truth_root, day_utc, nav_total=100000)

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

        assert rc == 1
        payload = json.loads(
            (
                truth_root
                / "reports"
                / "liquidity_slippage_gate_v1"
                / day_utc
                / "liquidity_slippage_gate.v1.json"
            ).read_text(encoding="utf-8")
        )
        row = payload["results"]["per_intent"][0]
        assert payload["status"] == "FAIL"
        assert row["decision"] == "FAIL"
        assert row["reason_codes"] == ["LIQPOL_NOTIONAL_EXCEEDS_CAP"]
        assert "approved governance" in row["recovery_command"]
        assert payload["recovery_commands"] == [row["recovery_command"]]
