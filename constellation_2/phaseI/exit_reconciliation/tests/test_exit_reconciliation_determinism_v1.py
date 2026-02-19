import json
import tempfile
from pathlib import Path

from constellation_2.phaseI.exit_reconciliation.run.run_exit_reconciliation_day_v1 import (
    build_exit_reconciliation,
)


def _write_json(p: Path, obj):
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_text(json.dumps(obj, sort_keys=True, separators=(",", ":")) + "\n", encoding="utf-8")


def test_exit_reconciliation_deterministic_obligations_ordering_and_content():
    """
    Determinism test:
    Given identical input bytes, obligations list and reason_codes ordering must be stable.
    We avoid produced_utc nondeterminism by comparing structural content excluding produced_utc.
    """
    with tempfile.TemporaryDirectory() as td:
        root = Path(td)

        positions_path = root / "positions_snapshot.v2.json"
        positions_obj = {
            "schema_id": "C2_POSITIONS_SNAPSHOT_V2",
            "schema_version": 2,
            "produced_utc": "2026-02-18T00:00:00Z",
            "day_utc": "2026-02-18",
            "producer": {"repo": "x", "git_sha": "abc1234", "module": "m"},
            "status": "OK",
            "reason_codes": [],
            "input_manifest": [{"type": "other", "path": "x", "sha256": "0" * 64, "day_utc": None, "producer": "x"}],
            "positions": {
                "currency": "USD",
                "asof_utc": "2026-02-18T00:00:00Z",
                "notes": [],
                "items": [
                    {
                        "position_id": "p" * 16,
                        "engine_id": "C2_TREND_EQ_PRIMARY_V1",
                        "instrument": {"kind": "EQUITY", "underlying": "SPY", "expiry": None, "strike": None, "right": None},
                        "qty": 1,
                        "avg_cost_cents": 100,
                        "market_exposure_type": "UNDEFINED_RISK",
                        "max_loss_cents": None,
                        "opened_day_utc": "2026-02-17",
                        "status": "OPEN",
                    },
                    {
                        "position_id": "q" * 16,
                        "engine_id": "C2_MEAN_REVERSION_EQ_V1",
                        "instrument": {"kind": "EQUITY", "underlying": "IWM", "expiry": None, "strike": None, "right": None},
                        "qty": 1,
                        "avg_cost_cents": 100,
                        "market_exposure_type": "UNDEFINED_RISK",
                        "max_loss_cents": None,
                        "opened_day_utc": "2026-02-17",
                        "status": "OPEN",
                    },
                ],
            },
        }
        _write_json(positions_path, positions_obj)

        # Intents day dir contains ONLY one exposure intent for TREND engine; MR is silent -> obligation expected for MR only.
        intents_dir = root / "intents" / "2026-02-18"
        intent_obj = {
            "schema_id": "exposure_intent",
            "schema_version": "v1",
            "intent_id": "x" * 16,
            "created_at_utc": "2026-02-18T00:00:00Z",
            "engine": {"engine_id": "C2_TREND_EQ_PRIMARY_V1", "suite": "C2_HYBRID_V1", "mode": "PAPER"},
            "underlying": {"symbol": "SPY", "currency": "USD"},
            "exposure_type": "LONG_EQUITY",
            "target_notional_pct": "0.5",
            "expected_holding_days": 5,
            "risk_class": "TREND",
        }
        _write_json(intents_dir / "intent.json", intent_obj)

        out1 = build_exit_reconciliation(
            repo_root=root,
            day_utc="2026-02-18",
            positions_path=positions_path,
            positions_obj=positions_obj,
            positions_sha256="a" * 64,
            intents_day_dir=intents_dir,
        )
        out2 = build_exit_reconciliation(
            repo_root=root,
            day_utc="2026-02-18",
            positions_path=positions_path,
            positions_obj=positions_obj,
            positions_sha256="a" * 64,
            intents_day_dir=intents_dir,
        )

        # produced_utc can differ; compare everything else.
        out1["produced_utc"] = "X"
        out2["produced_utc"] = "X"
        assert out1 == out2

        # Exactly one obligation (MR)
        assert len(out1["obligations"]) == 1
        assert out1["obligations"][0]["engine_id"] == "C2_MEAN_REVERSION_EQ_V1"
        assert out1["obligations"][0]["recommended_target_notional_pct"] == "0"
