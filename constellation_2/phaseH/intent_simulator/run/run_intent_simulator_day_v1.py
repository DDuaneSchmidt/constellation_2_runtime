#!/usr/bin/env python3
"""
run_intent_simulator_day_v1.py

C2_INTENT_SIMULATOR_V1
Deterministic structural intent wave generator (NOT an alpha engine).

FAIL-CLOSED CONTRACTS
- Requires --produced_utc YYYY-MM-DDTHH:MM:SSZ (UTC, exact Z)
- Convert produced_utc to America/New_York and REQUIRE exact 10:00:00
- Exactly one run per day_utc: refuse if day output dir already exists
- Writes only under canonical truth:
    constellation_2/runtime/truth/intents_v1/snapshots/<DAY_UTC>/
- One file per scenario:
    <INTENT_HASH>.exposure_intent.v1.json
  where INTENT_HASH = sha256(file bytes)

DETERMINISM
- canonical JSON (phaseD canonical_json_bytes_v1) + newline
- intent_id = sha256(engine_id + "|" + scenario_name + "|" + day_utc)
- canonical_json_hash follows C2 convention:
    sha256(canonical JSON with canonical_json_hash forced to null)
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

try:
    from zoneinfo import ZoneInfo
except Exception as e:  # noqa: BLE001
    raise RuntimeError(f"ZONEINFO_UNAVAILABLE: {e}") from e

from constellation_2.phaseD.lib.canon_json_v1 import (
    CanonicalizationError,
    canonical_hash_for_c2_artifact_v1,
    canonical_json_bytes_v1,
)
from constellation_2.phaseD.lib.validate_against_schema_v1 import SchemaValidationError, validate_against_repo_schema_v1


ENGINE_ID = "C2_INTENT_SIMULATOR_V1"
ENGINE_SUITE = "C2_SYSTEM_VALIDATION_V1"
MODE = "PAPER"

REPO_ROOT = Path("/home/node/constellation_2_runtime").resolve()
TRUTH_ROOT = (REPO_ROOT / "constellation_2" / "runtime" / "truth").resolve()
INTENTS_ROOT = (TRUTH_ROOT / "intents_v1" / "snapshots").resolve()

ENGINE_REGISTRY_PATH = (REPO_ROOT / "governance" / "02_REGISTRIES" / "ENGINE_MODEL_REGISTRY_V1.json").resolve()
EXPOSURE_INTENT_SCHEMA_RELPATH = "constellation_2/schemas/exposure_intent.v1.schema.json"

_UTC_RE = re.compile(r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}Z$")


class IntentSimulatorError(RuntimeError):
    pass


def _sha256_bytes(b: bytes) -> str:
    return hashlib.sha256(b).hexdigest()


def _sha256_file(path: Path) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def _parse_produced_utc(s: str) -> datetime:
    if not _UTC_RE.match(s):
        raise IntentSimulatorError(f"BAD_PRODUCED_UTC_FORMAT: {s} (expected YYYY-MM-DDTHH:MM:SSZ)")
    try:
        dt = datetime.strptime(s, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=timezone.utc)
    except Exception as e:  # noqa: BLE001
        raise IntentSimulatorError(f"BAD_PRODUCED_UTC_PARSE: {s}: {e}") from e
    return dt


def _require_exact_time_lock(dt_utc: datetime) -> None:
    ny = ZoneInfo("America/New_York")
    dt_local = dt_utc.astimezone(ny)
    if not (dt_local.hour == 10 and dt_local.minute == 0 and dt_local.second == 0):
        raise IntentSimulatorError(
            "TIME_LOCK_VIOLATION: produced_utc not exactly 10:00:00 America/New_York: "
            + json.dumps(
                {
                    "produced_utc": dt_utc.strftime("%Y-%m-%dT%H:%M:%SZ"),
                    "local_time": dt_local.strftime("%Y-%m-%dT%H:%M:%S%z"),
                    "required_local_time": "10:00:00",
                    "required_tz": "America/New_York",
                },
                sort_keys=True,
                separators=(",", ":"),
            )
        )


def _load_engine_registry(expected_sha256: str) -> Dict[str, Any]:
    if not ENGINE_REGISTRY_PATH.exists():
        raise IntentSimulatorError(f"ENGINE_REGISTRY_MISSING: {ENGINE_REGISTRY_PATH}")
    actual = _sha256_file(ENGINE_REGISTRY_PATH)
    if actual != expected_sha256:
        raise IntentSimulatorError(f"ENGINE_REGISTRY_SHA_MISMATCH: expected={expected_sha256} actual={actual}")
    try:
        return json.loads(ENGINE_REGISTRY_PATH.read_text(encoding="utf-8"))
    except Exception as e:  # noqa: BLE001
        raise IntentSimulatorError(f"ENGINE_REGISTRY_JSON_PARSE_FAILED: {e}") from e


def _require_engine_active(reg: Dict[str, Any], engine_id: str) -> None:
    engines = reg.get("engines", [])
    for e in engines:
        if e.get("engine_id") == engine_id:
            if e.get("activation_status") != "ACTIVE":
                raise IntentSimulatorError(f"ENGINE_NOT_ACTIVE: {engine_id} status={e.get('activation_status')}")
            return
    raise IntentSimulatorError(f"ENGINE_NOT_IN_REGISTRY: {engine_id}")


def _intent_id(engine_id: str, scenario_name: str, day_utc: str) -> str:
    preimage = (engine_id + "|" + scenario_name + "|" + day_utc).encode("utf-8")
    return hashlib.sha256(preimage).hexdigest()


def _build_intent_obj(
    *,
    day_utc: str,
    produced_utc: str,
    scenario_name: str,
    engine_id: str,
    symbol: str,
    exposure_type: str,
    target_notional_pct: str,
    expected_holding_days: int,
    risk_class: str,
    option: Optional[Dict[str, str]],
) -> Dict[str, Any]:
    obj: Dict[str, Any] = {
        "schema_id": "exposure_intent",
        "schema_version": "v1",
        "intent_id": _intent_id(engine_id, scenario_name, day_utc),
        "created_at_utc": produced_utc,
        "engine": {"engine_id": engine_id, "suite": ENGINE_SUITE, "mode": MODE},
        "underlying": {"symbol": symbol, "currency": "USD"},
        "exposure_type": exposure_type,
        "target_notional_pct": target_notional_pct,
        "expected_holding_days": int(expected_holding_days),
        "risk_class": risk_class,
        "constraints": None if target_notional_pct == "0" else {"max_risk_pct": "0.01"},
        "canonical_json_hash": None,
    }
    if option is not None:
        obj["option"] = option
    obj["canonical_json_hash"] = canonical_hash_for_c2_artifact_v1(obj)
    return obj


def _atomic_write_dir_refuse_overwrite(day_dir: Path, tmp_dir: Path, files: List[Tuple[str, bytes]]) -> None:
    if day_dir.exists():
        raise IntentSimulatorError(f"INTENTS_ALREADY_EXIST_FOR_DAY: {day_dir}")
    if tmp_dir.exists():
        raise IntentSimulatorError(f"TEMP_DIR_ALREADY_EXISTS: {tmp_dir}")

    tmp_dir.mkdir(parents=False, exist_ok=False)

    try:
        for fname, payload in files:
            out_path = (tmp_dir / fname).resolve()
            if out_path.exists():
                raise IntentSimulatorError(f"REFUSE_OVERWRITE: {out_path}")
            with open(out_path, "xb") as f:
                f.write(payload)
                f.flush()
                os.fsync(f.fileno())

        tmp_dir.rename(day_dir)
    except Exception:
        try:
            for p in tmp_dir.glob("*"):
                try:
                    p.unlink()
                except Exception:
                    pass
            try:
                tmp_dir.rmdir()
            except Exception:
                pass
        except Exception:
            pass
        raise


def main() -> int:
    ap = argparse.ArgumentParser(prog="run_intent_simulator_day_v1")
    ap.add_argument("--produced_utc", required=True, help="UTC timestamp YYYY-MM-DDTHH:MM:SSZ (exact Z)")
    ap.add_argument(
        "--override_time_lock",
        default="NO",
        choices=["YES", "NO"],
        help="YES bypasses exact 10:00:00 America/New_York time-lock (manual structural testing only). Default NO.",
    )
    ap.add_argument(
        "--engine_registry_sha256",
        required=True,
        help="Expected sha256 of governance/02_REGISTRIES/ENGINE_MODEL_REGISTRY_V1.json",
    )
    args = ap.parse_args()

    produced_utc = str(args.produced_utc).strip()
    dt_utc = _parse_produced_utc(produced_utc)
    override = str(args.override_time_lock).strip().upper()
    if override not in ("YES", "NO"):
        raise IntentSimulatorError(f"BAD_OVERRIDE_TIME_LOCK: {override!r} (expected YES|NO)")
    if override != "YES":
        _require_exact_time_lock(dt_utc)
    day_utc = dt_utc.strftime("%Y-%m-%d")

    reg = _load_engine_registry(str(args.engine_registry_sha256).strip())

    for eid in [
        "C2_TREND_EQ_PRIMARY_V1",
        "C2_MEAN_REVERSION_EQ_V1",
        "C2_VOL_INCOME_DEFINED_RISK_V1",
        "C2_DEFENSIVE_TAIL_V1",
        "C2_EVENT_DISLOCATION_V1",
    ]:
        _require_engine_active(reg, eid)

    symbol = "SPY"

    scenarios: List[Dict[str, Any]] = []

    scenarios.append(
        _build_intent_obj(
            day_utc=day_utc,
            produced_utc=produced_utc,
            scenario_name="TREND_EQUITY_OPEN",
            engine_id="C2_TREND_EQ_PRIMARY_V1",
            symbol=symbol,
            exposure_type="LONG_EQUITY",
            target_notional_pct="0.01",
            expected_holding_days=5,
            risk_class="TREND",
            option=None,
        )
    )

    scenarios.append(
        _build_intent_obj(
            day_utc=day_utc,
            produced_utc=produced_utc,
            scenario_name="TREND_EQUITY_CLOSE",
            engine_id="C2_TREND_EQ_PRIMARY_V1",
            symbol=symbol,
            exposure_type="LONG_EQUITY",
            target_notional_pct="0",
            expected_holding_days=0,
            risk_class="TREND_EXIT",
            option=None,
        )
    )

    scenarios.append(
        _build_intent_obj(
            day_utc=day_utc,
            produced_utc=produced_utc,
            scenario_name="MEAN_REVERSION_OPEN",
            engine_id="C2_MEAN_REVERSION_EQ_V1",
            symbol=symbol,
            exposure_type="LONG_EQUITY",
            target_notional_pct="0.01",
            expected_holding_days=3,
            risk_class="MEAN_REVERSION",
            option=None,
        )
    )

    scenarios.append(
        _build_intent_obj(
            day_utc=day_utc,
            produced_utc=produced_utc,
            scenario_name="VOL_INCOME_SHORT_PUT",
            engine_id="C2_VOL_INCOME_DEFINED_RISK_V1",
            symbol=symbol,
            exposure_type="SHORT_VOL_DEFINED",
            target_notional_pct="0.01",
            expected_holding_days=7,
            risk_class="VOL_INCOME_DEFINED",
            option={"structure": "PUT", "direction": "SELL"},
        )
    )

    scenarios.append(
        _build_intent_obj(
            day_utc=day_utc,
            produced_utc=produced_utc,
            scenario_name="DEFENSIVE_TAIL_LONG_PUT",
            engine_id="C2_DEFENSIVE_TAIL_V1",
            symbol=symbol,
            exposure_type="SHORT_VOL_DEFINED",
            target_notional_pct="0.01",
            expected_holding_days=14,
            risk_class="DEFENSIVE_TAIL",
            option={"structure": "PUT", "direction": "BUY"},
        )
    )

    scenarios.append(
        _build_intent_obj(
            day_utc=day_utc,
            produced_utc=produced_utc,
            scenario_name="EVENT_DISLOCATION_CALL",
            engine_id="C2_EVENT_DISLOCATION_V1",
            symbol=symbol,
            exposure_type="SHORT_VOL_DEFINED",
            target_notional_pct="0.01",
            expected_holding_days=2,
            risk_class="EVENT_DISLOCATION",
            option={"structure": "CALL", "direction": "BUY"},
        )
    )

    scenarios.append(
        _build_intent_obj(
            day_utc=day_utc,
            produced_utc=produced_utc,
            scenario_name="EXIT_OBLIGATION_EQUITY_CLOSE",
            engine_id="C2_MEAN_REVERSION_EQ_V1",
            symbol=symbol,
            exposure_type="LONG_EQUITY",
            target_notional_pct="0",
            expected_holding_days=0,
            risk_class="EXIT_OBLIGATION",
            option=None,
        )
    )

    if len(scenarios) != 7:
        raise IntentSimulatorError(f"SCENARIO_COUNT_MISMATCH: expected=7 actual={len(scenarios)}")

    files: List[Tuple[str, bytes]] = []
    for obj in scenarios:
        try:
            validate_against_repo_schema_v1(obj, REPO_ROOT, EXPOSURE_INTENT_SCHEMA_RELPATH)
        except SchemaValidationError as e:
            raise IntentSimulatorError(str(e)) from e
        except Exception as e:  # noqa: BLE001
            raise IntentSimulatorError(f"SCHEMA_VALIDATION_FAILED: {e}") from e

        try:
            payload = canonical_json_bytes_v1(obj) + b"\n"
        except CanonicalizationError as e:
            raise IntentSimulatorError(f"CANONICALIZATION_FAILED: {e}") from e

        intent_hash = _sha256_bytes(payload)
        fname = f"{intent_hash}.exposure_intent.v1.json"
        files.append((fname, payload))

    day_dir = (INTENTS_ROOT / day_utc).resolve()
    tmp_dir = (INTENTS_ROOT / f"{day_utc}.__intent_simulator_tmp__").resolve()

    _atomic_write_dir_refuse_overwrite(day_dir, tmp_dir, files)

    print(
        "OK: INTENT_SIMULATOR_WRITTEN "
        + json.dumps(
            {
                "engine_id": ENGINE_ID,
                "suite": ENGINE_SUITE,
                "mode": MODE,
                "produced_utc": produced_utc,
                "day_utc": day_utc,
                "out_dir": str(day_dir),
                "scenarios": len(files),
            },
            sort_keys=True,
            separators=(",", ":"),
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
