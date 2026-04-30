from __future__ import annotations

import json
import importlib.util
from pathlib import Path


DAY = "2026-04-23"
PREV_DAY = "2026-04-22"
_RETURNS_MODULE_PATH = Path("/home/node/constellation/ops/tools/run_engine_daily_returns_day_v1.py")
_SPEC = importlib.util.spec_from_file_location("ops_run_engine_daily_returns_day_v1_under_test", _RETURNS_MODULE_PATH)
assert _SPEC and _SPEC.loader
returns_module = importlib.util.module_from_spec(_SPEC)
_SPEC.loader.exec_module(returns_module)


def _write_json(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, sort_keys=True) + "\n", encoding="utf-8")


def _attribution_payload(*, day_utc: str, status: str, by_engine: list[dict]) -> dict:
    return {
        "schema_id": "C2_ACCOUNTING_ENGINE_ATTRIBUTION_V2",
        "schema_version": 2,
        "produced_utc": f"{day_utc}T00:00:00Z",
        "day_utc": day_utc,
        "producer": {"repo": "constellation", "git_sha": "a" * 40, "module": "test"},
        "status": status,
        "reason_codes": [],
        "input_manifest": [],
        "attribution": {
            "currency": "USD",
            "by_engine": by_engine,
            "notes": [],
        },
    }


def _nav_payload(*, day_utc: str, nav_total: int) -> dict:
    return {
        "schema_id": "C2_ACCOUNTING_NAV_V2",
        "schema_version": 2,
        "produced_utc": f"{day_utc}T00:00:00Z",
        "day_utc": day_utc,
        "producer": {"repo": "constellation", "git_sha": "a" * 40, "module": "test"},
        "status": "ACTIVE",
        "reason_codes": [],
        "input_manifest": [],
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
    }


def test_engine_daily_returns_computes_when_basis_proven(tmp_path: Path) -> None:
    truth_root = tmp_path / "truth"
    _write_json(
        truth_root / "accounting_v2" / "attribution" / PREV_DAY / "engine_attribution.v2.json",
        _attribution_payload(
            day_utc=PREV_DAY,
            status="ACTIVE",
            by_engine=[{"engine_id": "ENG_A", "realized_pnl_to_date": 100, "unrealized_pnl": 0}],
        ),
    )
    _write_json(
        truth_root / "accounting_v2" / "attribution" / DAY / "engine_attribution.v2.json",
        _attribution_payload(
            day_utc=DAY,
            status="ACTIVE",
            by_engine=[{"engine_id": "ENG_A", "realized_pnl_to_date": 110, "unrealized_pnl": 0}],
        ),
    )
    _write_json(
        truth_root / "accounting_v2" / "nav" / PREV_DAY / "nav.v2.json",
        _nav_payload(day_utc=PREV_DAY, nav_total=1000),
    )

    rc = returns_module.main(["--day_utc", DAY, "--prev_day_utc", PREV_DAY, "--truth_root", str(truth_root)])
    assert rc == 0

    out_path = truth_root / "monitoring_v1" / "engine_daily_returns_v1" / DAY / "engine_daily_returns.v1.json"
    payload = json.loads(out_path.read_text(encoding="utf-8"))
    assert payload["status"] == "ACTIVE"
    assert payload["reason_codes"] == []
    assert payload["returns"] == [{"engine_id": "ENG_A", "daily_return": "0.01000000"}]


def test_engine_daily_returns_missing_nav_emits_explicit_reason(tmp_path: Path) -> None:
    truth_root = tmp_path / "truth"
    _write_json(
        truth_root / "accounting_v2" / "attribution" / PREV_DAY / "engine_attribution.v2.json",
        _attribution_payload(
            day_utc=PREV_DAY,
            status="ACTIVE",
            by_engine=[{"engine_id": "ENG_A", "realized_pnl_to_date": 100, "unrealized_pnl": 0}],
        ),
    )
    _write_json(
        truth_root / "accounting_v2" / "attribution" / DAY / "engine_attribution.v2.json",
        _attribution_payload(
            day_utc=DAY,
            status="ACTIVE",
            by_engine=[{"engine_id": "ENG_A", "realized_pnl_to_date": 110, "unrealized_pnl": 0}],
        ),
    )

    rc = returns_module.main(["--day_utc", DAY, "--prev_day_utc", PREV_DAY, "--truth_root", str(truth_root)])
    assert rc == 0

    out_path = truth_root / "monitoring_v1" / "engine_daily_returns_v1" / DAY / "engine_daily_returns.v1.json"
    payload = json.loads(out_path.read_text(encoding="utf-8"))
    assert payload["status"] == "NOT_AVAILABLE"
    assert "MISSING_NAV_PREV" in payload["reason_codes"]


def test_engine_daily_returns_no_engine_rows_emits_safe_idle_reason(tmp_path: Path) -> None:
    truth_root = tmp_path / "truth"
    _write_json(
        truth_root / "accounting_v2" / "attribution" / PREV_DAY / "engine_attribution.v2.json",
        _attribution_payload(day_utc=PREV_DAY, status="ACTIVE", by_engine=[]),
    )
    _write_json(
        truth_root / "accounting_v2" / "attribution" / DAY / "engine_attribution.v2.json",
        _attribution_payload(day_utc=DAY, status="ACTIVE", by_engine=[]),
    )
    _write_json(
        truth_root / "accounting_v2" / "nav" / PREV_DAY / "nav.v2.json",
        _nav_payload(day_utc=PREV_DAY, nav_total=1000),
    )

    rc = returns_module.main(["--day_utc", DAY, "--prev_day_utc", PREV_DAY, "--truth_root", str(truth_root)])
    assert rc == 0

    out_path = truth_root / "monitoring_v1" / "engine_daily_returns_v1" / DAY / "engine_daily_returns.v1.json"
    payload = json.loads(out_path.read_text(encoding="utf-8"))
    assert payload["status"] == "NOT_AVAILABLE"
    assert "NO_ENGINE_DATA_SAFE_IDLE" in payload["reason_codes"]
    assert payload["returns"] == []
