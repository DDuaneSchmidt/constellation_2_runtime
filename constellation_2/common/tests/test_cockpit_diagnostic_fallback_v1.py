from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from constellation_2.phaseL.ui.server import c2_ops_cockpit_status_v2_collector_v1 as cockpit_module


def _write_json(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, sort_keys=True) + "\n", encoding="utf-8")


class CockpitDiagnosticFallbackTests(unittest.TestCase):
    def test_platform_readiness_latest_pointer_fallback_is_non_authoritative(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            truth_root = root / "constellation_2/runtime/truth"
            readiness_root = truth_root / "readiness_v1/constellation_platform_readiness_v1"
            target = readiness_root / "2026-03-10/constellation_platform_readiness.v1.json"
            _write_json(
                target,
                {
                    "day_utc": "2026-03-10",
                    "produced_utc": "2026-03-10T12:00:00Z",
                    "platform_readiness_state": "BLOCKED",
                    "platform_readiness_score": 12,
                    "platform_readiness_grade": "F",
                },
            )
            _write_json(
                readiness_root / "latest_pointer.v1.json",
                {
                    "target_path": str(target),
                    "target_sha256": cockpit_module._sha256_file(target),
                },
            )

            with patch.object(cockpit_module, "REPO_ROOT", root), patch.object(
                cockpit_module, "GLOBAL_RUNTIME_TRUTH_ROOT", truth_root
            ):
                payload = cockpit_module._load_platform_readiness(truth_root, "2026-03-17")

            self.assertTrue(payload["resolved_via_latest_pointer"])
            self.assertFalse(payload["authoritative_for_family"])
            self.assertEqual(payload["diagnostic_family_classification"], "LATEST_POINTER_FALLBACK_NON_AUTHORITATIVE")
            self.assertEqual(payload["resolution_mode"], "LATEST_POINTER_FALLBACK")

    def test_operational_readiness_fails_closed_when_selected_day_inputs_are_missing(self) -> None:
        payload = cockpit_module._derive_operational_readiness(
            day="2026-03-17",
            platform_readiness={
                "requested_day_present": False,
                "authoritative_for_family": False,
                "resolved_day": "2026-03-11",
                "resolved_via_latest_pointer": True,
                "resolution_mode": "LATEST_POINTER_FALLBACK",
            },
            signal_activity={
                "engine_heartbeats": {
                    "expected_count": 7,
                    "present_count": 0,
                },
                "upstream_data_status": {
                    "symbols": [
                        {"symbol": "IWM", "same_day_present": False},
                        {"symbol": "SPY", "same_day_present": False},
                        {"symbol": "QQQ", "same_day_present": False},
                    ]
                },
                "trading_day_outcome": {
                    "gate_stack": {"status": "FAIL"},
                    "kill_switch": {"state": "INACTIVE", "allow_entries": True},
                },
            },
            trading_day_state={
                "heartbeat_status": "FAIL",
            },
            day_start_blocked={
                "blocked": False,
            },
        )

        self.assertEqual(payload["state"], "NOT_READY")
        self.assertIn("STRUCTURAL_READINESS_STALE_LATEST_POINTER", payload["reason_codes"])
        self.assertIn("UPSTREAM_DATA_SELECTED_DAY_MISSING", payload["reason_codes"])
        self.assertIn("ENGINE_HEARTBEATS_INCOMPLETE:0/7", payload["reason_codes"])
        self.assertIn("GATE_STACK_FAIL", payload["reason_codes"])
        self.assertIn("TRADING_DAY_HEARTBEAT_FAIL", payload["reason_codes"])
        self.assertTrue(payload["same_day_authoritative_inputs_only"])


if __name__ == "__main__":
    unittest.main()
