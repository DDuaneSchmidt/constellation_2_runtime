from __future__ import annotations

import json
import sys
import tempfile
from pathlib import Path

SOURCE_ROOT = Path("/home/node/constellation")
if str(SOURCE_ROOT) not in sys.path:
    sys.path.insert(0, str(SOURCE_ROOT))

import ops.tools.run_phasec_risk_inputs_prep_v1 as risk_prep_module


def _write_json(path: Path, obj: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(obj, sort_keys=True, separators=(",", ":")) + "\n", encoding="utf-8")


def _write_nav_v2(truth_root: Path, *, day_utc: str, nav_total: int) -> None:
    _write_json(
        truth_root / "accounting_v2" / "nav" / day_utc / "nav.v2.json",
        {
            "schema_id": "C2_ACCOUNTING_NAV_V2",
            "schema_version": 2,
            "day_utc": day_utc,
            "produced_utc": f"{day_utc}T00:00:00Z",
            "producer": {"repo": "constellation", "module": "test", "git_sha": "abc1234"},
            "status": "ACTIVE",
            "reason_codes": [],
            "input_manifest": [],
            "history": {},
            "nav": {
                "currency": "USD",
                "nav_total": nav_total,
                "cash_total": nav_total,
                "gross_positions_value": 0,
                "realized_pnl_to_date": 0,
                "unrealized_pnl": 0,
                "components": [
                    {
                        "kind": "CASH",
                        "symbol": "USD",
                        "qty": str(nav_total),
                        "mv": nav_total,
                        "mark": {
                            "bid": None,
                            "ask": None,
                            "last": None,
                            "source": "CASH_LEDGER",
                            "asof_utc": f"{day_utc}T00:00:00Z",
                        },
                    }
                ],
                "notes": [],
            },
        },
    )


def test_phasec_risk_inputs_prep_derives_drawdown_from_nav_v2_history() -> None:
    with tempfile.TemporaryDirectory() as td:
        truth_root = Path(td) / "truth"
        _write_nav_v2(truth_root, day_utc="2026-04-07", nav_total=100000)
        _write_nav_v2(truth_root, day_utc="2026-04-08", nav_total=90000)
        rc = risk_prep_module.main(["--day_utc", "2026-04-08", "--truth_root", str(truth_root)])
        assert rc == 0
        compat_obj = json.loads(
            (truth_root / "accounting_compat_v1" / "nav" / "2026-04-08" / "nav_snapshot.v1.json").read_text(encoding="utf-8")
        )
        prep_obj = json.loads(
            (truth_root / "reports" / "phasec_risk_inputs_prep_v1" / "2026-04-08" / "phasec_risk_inputs_prep.v1.json").read_text(encoding="utf-8")
        )
        assert compat_obj["producer"]["repo"] == "constellation"
        assert compat_obj["history"]["peak_nav"] == 100000
        assert compat_obj["history"]["drawdown_abs"] == -10000
        assert compat_obj["history"]["drawdown_pct"] == "-0.100000"
        assert prep_obj["status"] == "PASS"
        assert prep_obj["drawdown_pct"] == "-0.100000"


def test_phasec_risk_inputs_prep_missing_nav_v2_fails_closed_exactly() -> None:
    with tempfile.TemporaryDirectory() as td:
        truth_root = Path(td) / "truth"
        rc = risk_prep_module.main(["--day_utc", "2026-04-08", "--truth_root", str(truth_root)])
        assert rc == 2
        payload = json.loads(
            (truth_root / "reports" / "phasec_risk_inputs_prep_v1" / "2026-04-08" / "phasec_risk_inputs_prep.v1.json").read_text(encoding="utf-8")
        )
        assert payload["status"] == "BLOCKED_VALID"
        assert payload["blocking_codes"] == ["PHASEC_RISK_INPUTS_PREP_MISSING_DEPENDENCY:ACCOUNTING_NAV_V2_MISSING"]


def test_phasec_risk_inputs_prep_no_positive_peak_fails_closed_exactly() -> None:
    with tempfile.TemporaryDirectory() as td:
        truth_root = Path(td) / "truth"
        _write_nav_v2(truth_root, day_utc="2026-04-06", nav_total=0)
        rc = risk_prep_module.main(["--day_utc", "2026-04-06", "--truth_root", str(truth_root)])
        assert rc == 2
        payload = json.loads(
            (truth_root / "reports" / "phasec_risk_inputs_prep_v1" / "2026-04-06" / "phasec_risk_inputs_prep.v1.json").read_text(encoding="utf-8")
        )
        assert payload["status"] == "BLOCKED_VALID"
        assert "PHASEC_RISK_INPUTS_PREP_MISSING_DEPENDENCY:NO_POSITIVE_PEAK_AVAILABLE_FOR_DRAWDOWN" in payload["blocking_codes"]
