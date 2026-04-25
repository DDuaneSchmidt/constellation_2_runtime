from __future__ import annotations

import json
import sys
import tempfile
from pathlib import Path
from unittest.mock import patch

SOURCE_ROOT = Path("/home/node/constellation")
if str(SOURCE_ROOT) not in sys.path:
    sys.path.insert(0, str(SOURCE_ROOT))

import ops.tools.run_startup_materialization_inputs_prep_v1 as prep_module
from constellation_2.common.paper_session_fact_plane_v1 import canonical_paper_session_id_v1
from constellation_2.phaseH.tools.c2_risk_transformer_offline_v1 import _load_nav_usd_from_accounting_day


def _write_json(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, sort_keys=True, separators=(",", ":")) + "\n", encoding="utf-8")


def _write_intent(truth_root: Path, day_utc: str, *, symbol: str = "SPY", target_notional_pct: str = "0.01") -> None:
    _write_json(
        truth_root / "intents_v1" / "snapshots" / day_utc / f"{symbol.lower()}.exposure_intent.v1.json",
        {
            "schema_id": "exposure_intent",
            "schema_version": "v1",
            "intent_id": f"intent:{symbol}:{day_utc}",
            "underlying": {"symbol": symbol},
            "target_notional_pct": target_notional_pct,
        },
    )


def _write_market_data(truth_root: Path, *, symbol: str, day_rows: list[tuple[str, ...]]) -> None:
    md_root = truth_root / "market_data_snapshot_v1"
    year_path = md_root / symbol / "2026.jsonl"
    year_path.parent.mkdir(parents=True, exist_ok=True)
    lines = [
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
                "ingested_utc": ingested_utc,
            },
            sort_keys=True,
            separators=(",", ":"),
        )
        for day, close, ingested_utc in (
            (
                row[0],
                row[1],
                row[2] if len(row) >= 3 else f"{row[0]}T14:00:00Z",
            )
            for row in day_rows
        )
    ]
    payload = "\n".join(lines) + "\n"
    year_path.write_text(payload, encoding="utf-8")
    import hashlib

    _write_json(
        md_root / "dataset_manifest.json",
        {
            "created_utc": "2026-04-08T00:00:00Z",
            "source_snapshot_utc": "2026-04-08T00:00:00Z",
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


def _write_liquidity_gate(truth_root: Path, day_utc: str, *, status: str, close: str = "650.00") -> Path:
    path = truth_root / "reports" / "liquidity_slippage_gate_v1" / day_utc / "liquidity_slippage_gate.v1.json"
    _write_json(
        path,
        {
            "schema_id": "liquidity_slippage_gate",
            "schema_version": "v1",
            "day_utc": day_utc,
            "produced_utc": f"{day_utc}T00:00:00Z",
            "producer": {"repo": "constellation", "module": "test", "git_sha": "a" * 40},
            "status": status,
            "reason_codes": ["LIQPOL_PASS"] if status == "PASS" else ["LIQPOL_FAIL_CLOSED_REQUIRED"],
            "input_manifest": [],
            "policy": {"path": "/tmp/policy.json", "sha256": "c" * 64, "schema_path": "/tmp/schema.json", "schema_sha256": "d" * 64},
            "results": {
                "per_intent": [
                    {
                        "intent_hash": "e" * 64,
                        "engine_id": "C2_TREND_EQ_PRIMARY_V1",
                        "symbol": "SPY",
                        "decision": status,
                        "reason_codes": ["LIQPOL_PASS"] if status == "PASS" else ["LIQPOL_FAIL_CLOSED_REQUIRED"],
                        "metrics": {
                            "nav_total_cents": 10000000,
                            "target_notional_pct": "0.010000",
                            "est_notional_usd": "1000.00",
                            "close": close,
                            "est_shares": 1,
                            "adv_shares": 100000,
                            "adv_dollar": "1000000.00",
                            "participation_pct_adv": "0.000010",
                            "est_slippage_bps": "1.00",
                            "caps": {
                                "max_participation_pct_adv": "0.050000",
                                "max_est_slippage_bps": "25.00",
                                "max_notional_per_symbol_usd": "1000000",
                            },
                        },
                    }
                ],
                "totals": {"intents_total": 1, "intents_failed": 0 if status == "PASS" else 1, "intents_passed": 1 if status == "PASS" else 0, "intents_skipped": 0},
            },
            "gate_sha256": "f" * 64,
        },
    )
    return path


def test_inputs_prep_uses_same_day_core_session_price_when_present_after_open() -> None:
    with tempfile.TemporaryDirectory() as td:
        truth_root = Path(td) / "truth"
        day_utc = "2026-04-08"
        _write_intent(truth_root, day_utc)
        _write_market_data(
            truth_root,
            symbol="SPY",
            day_rows=[("2026-04-08", "655.83", "2026-04-08T13:31:00Z")],
        )
        with patch.object(prep_module, "now_utc_iso_v1", return_value="2026-04-08T13:35:00Z"):
            rc = prep_module.main(["--day_utc", day_utc, "--truth_root", str(truth_root)])
        assert rc == 0
        payload = json.loads(
            (
                truth_root
                / "reports"
                / "startup_materialization_inputs_prep_v1"
                / day_utc
                / "startup_materialization_inputs_prep.v1.json"
            ).read_text(encoding="utf-8")
        )
        assert payload["status"] == "PASS"
        assert payload["session_id"] == canonical_paper_session_id_v1(day_utc)
        assert payload["default_equity_reference_price_source"] == "SAME_DAY_CORE_SESSION_PRICE"
        assert payload["default_equity_reference_price"] == "655.83"


def test_inputs_prep_rejects_same_day_price_before_core_session_open() -> None:
    with tempfile.TemporaryDirectory() as td:
        truth_root = Path(td) / "truth"
        day_utc = "2026-04-08"
        _write_intent(truth_root, day_utc)
        _write_market_data(
            truth_root,
            symbol="SPY",
            day_rows=[("2026-04-08", "655.83", "2026-04-08T13:25:00Z")],
        )
        with patch.object(prep_module, "now_utc_iso_v1", return_value="2026-04-08T13:29:00Z"):
            rc = prep_module.main(["--day_utc", day_utc, "--truth_root", str(truth_root)])
        assert rc == 2
        payload = json.loads(
            (
                truth_root
                / "reports"
                / "startup_materialization_inputs_prep_v1"
                / day_utc
                / "startup_materialization_inputs_prep.v1.json"
            ).read_text(encoding="utf-8")
        )
        assert payload["status"] == "BLOCKED_VALID"
        assert payload["default_equity_reference_price"] == ""
        assert "STARTUP_MATERIALIZATION_MISSING_DEPENDENCY:DEFAULT_EQUITY_REFERENCE_PRICE_UNAVAILABLE" in payload["blocking_codes"]


def test_inputs_prep_materializes_same_day_market_data_after_open_when_missing() -> None:
    with tempfile.TemporaryDirectory() as td:
        truth_root = Path(td) / "truth"
        day_utc = "2026-04-08"
        _write_intent(truth_root, day_utc)
        _write_market_data(truth_root, symbol="SPY", day_rows=[("2026-04-07", "650.00", "2026-04-07T20:00:00Z")])

        def _fake_refresh(*, day_utc: str, truth_root: Path, symbol: str, run_utc: str) -> dict:
            _write_market_data(
                truth_root,
                symbol=symbol,
                day_rows=[
                    ("2026-04-07", "650.00", "2026-04-07T20:00:00Z"),
                    ("2026-04-08", "655.83", "2026-04-08T13:35:00Z"),
                ],
            )
            return {"cmd": ["fake"], "returncode": 0, "stdout": "ok", "stderr": ""}

        with patch.object(prep_module, "now_utc_iso_v1", return_value="2026-04-08T13:35:00Z"), patch.object(
            prep_module,
            "_run_market_data_refresh_for_symbol",
            side_effect=_fake_refresh,
        ) as refresh_mock:
            rc = prep_module.main(["--day_utc", day_utc, "--truth_root", str(truth_root)])

        assert rc == 0
        refresh_mock.assert_called_once()
        payload = json.loads(
            (
                truth_root
                / "reports"
                / "startup_materialization_inputs_prep_v1"
                / day_utc
                / "startup_materialization_inputs_prep.v1.json"
            ).read_text(encoding="utf-8")
        )
        assert payload["status"] == "PASS"
        assert payload["default_equity_reference_price_source"] == "SAME_DAY_CORE_SESSION_PRICE"
        assert payload["default_equity_reference_price"] == "655.83"
        assert payload["default_equity_reference_price_artifact_path"].endswith("/market_data_snapshot_v1/SPY/2026.jsonl")


def test_inputs_prep_blocks_if_same_day_market_data_refresh_still_cannot_prove_price() -> None:
    with tempfile.TemporaryDirectory() as td:
        truth_root = Path(td) / "truth"
        day_utc = "2026-04-08"
        _write_intent(truth_root, day_utc)
        _write_market_data(truth_root, symbol="SPY", day_rows=[("2026-04-07", "650.00", "2026-04-07T20:00:00Z")])

        with patch.object(prep_module, "now_utc_iso_v1", return_value="2026-04-08T13:35:00Z"), patch.object(
            prep_module,
            "_run_market_data_refresh_for_symbol",
            return_value={"cmd": ["fake"], "returncode": 0, "stdout": "ok", "stderr": ""},
        ) as refresh_mock:
            rc = prep_module.main(["--day_utc", day_utc, "--truth_root", str(truth_root)])

        assert rc == 2
        refresh_mock.assert_called_once()
        payload = json.loads(
            (
                truth_root
                / "reports"
                / "startup_materialization_inputs_prep_v1"
                / day_utc
                / "startup_materialization_inputs_prep.v1.json"
            ).read_text(encoding="utf-8")
        )
        assert payload["status"] == "BLOCKED_VALID"
        assert payload["default_equity_reference_price"] == ""
        assert "STARTUP_MATERIALIZATION_MISSING_DEPENDENCY:DEFAULT_EQUITY_REFERENCE_PRICE_UNAVAILABLE" in payload["blocking_codes"]


def test_inputs_prep_rejects_stale_prior_day_price_and_does_not_use_liquidity_gate_fallback() -> None:
    with tempfile.TemporaryDirectory() as td:
        truth_root = Path(td) / "truth"
        day_utc = "2026-04-08"
        _write_intent(truth_root, day_utc)
        _write_market_data(truth_root, symbol="SPY", day_rows=[("2026-04-02", "650.00")])
        _write_liquidity_gate(truth_root, day_utc, status="FAIL", close="650.00")
        with patch.object(
            prep_module,
            "_run_liquidity_gate",
            side_effect=AssertionError("liquidity gate should not be used as a stale fallback price source"),
        ), patch.object(prep_module, "now_utc_iso_v1", return_value="2026-04-08T14:00:00Z"):
            rc = prep_module.main(["--day_utc", day_utc, "--truth_root", str(truth_root)])
        assert rc == 2
        payload = json.loads(
            (
                truth_root
                / "reports"
                / "startup_materialization_inputs_prep_v1"
                / day_utc
                / "startup_materialization_inputs_prep.v1.json"
            ).read_text(encoding="utf-8")
        )
        assert payload["status"] == "BLOCKED_VALID"
        assert "STARTUP_MATERIALIZATION_MISSING_DEPENDENCY:DEFAULT_EQUITY_REFERENCE_PRICE_UNAVAILABLE" in payload["blocking_codes"]
        assert payload["liquidity_gate_result"]["artifact_status"] == "NOT_REQUIRED"


def test_inputs_prep_fails_closed_when_same_day_price_is_nonpositive() -> None:
    with tempfile.TemporaryDirectory() as td:
        truth_root = Path(td) / "truth"
        day_utc = "2026-04-08"
        _write_intent(truth_root, day_utc)
        _write_market_data(
            truth_root,
            symbol="SPY",
            day_rows=[("2026-04-08", "0.00", "2026-04-08T13:31:00Z")],
        )
        with patch.object(prep_module, "now_utc_iso_v1", return_value="2026-04-08T14:00:00Z"):
            rc = prep_module.main(["--day_utc", day_utc, "--truth_root", str(truth_root)])
        assert rc == 2
        payload = json.loads(
            (
                truth_root
                / "reports"
                / "startup_materialization_inputs_prep_v1"
                / day_utc
                / "startup_materialization_inputs_prep.v1.json"
            ).read_text(encoding="utf-8")
        )
        assert payload["status"] == "BLOCKED_VALID"
        assert payload["default_equity_reference_price"] == ""
        assert "STARTUP_MATERIALIZATION_MISSING_DEPENDENCY:DEFAULT_EQUITY_REFERENCE_PRICE_UNAVAILABLE" in payload["blocking_codes"]


def test_risk_transformer_uses_accounting_v2_under_truth_root() -> None:
    with tempfile.TemporaryDirectory() as td:
        repo_root = Path(td) / "repo"
        truth_root = Path(td) / "truth"
        day_utc = "2026-04-08"
        _write_json(
            truth_root / "accounting_v2" / "nav" / day_utc / "nav.v2.json",
            {
                "nav": {
                    "nav_total": 100000,
                }
            },
        )
        nav_total, nav_path = _load_nav_usd_from_accounting_day(repo_root, day_utc, truth_root)
        assert nav_total == 100000
        assert nav_path.endswith("/accounting_v2/nav/2026-04-08/nav.v2.json")


def test_inputs_prep_rejects_nonpositive_liquidity_gate_price() -> None:
    with tempfile.TemporaryDirectory() as td:
        truth_root = Path(td) / "truth"
        day_utc = "2026-04-08"
        _write_intent(truth_root, day_utc)
        _write_market_data(truth_root, symbol="SPY", day_rows=[("2026-04-02", "650.00")])
        _write_liquidity_gate(truth_root, day_utc, status="PASS", close="0.00")
        with patch.object(
            prep_module,
            "_run_liquidity_gate",
            side_effect=AssertionError("liquidity gate should not be used as a stale fallback price source"),
        ), patch.object(prep_module, "now_utc_iso_v1", return_value="2026-04-08T14:00:00Z"):
            rc = prep_module.main(["--day_utc", day_utc, "--truth_root", str(truth_root)])
        assert rc == 2
        payload = json.loads(
            (
                truth_root
                / "reports"
                / "startup_materialization_inputs_prep_v1"
                / day_utc
                / "startup_materialization_inputs_prep.v1.json"
            ).read_text(encoding="utf-8")
        )
        assert payload["status"] == "BLOCKED_VALID"
        assert payload["default_equity_reference_price"] == ""
        assert "STARTUP_MATERIALIZATION_MISSING_DEPENDENCY:DEFAULT_EQUITY_REFERENCE_PRICE_UNAVAILABLE" in payload["blocking_codes"]
