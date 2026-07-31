#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any
from urllib.error import URLError
from urllib.request import Request, urlopen


def _fetch_json(url: str, *, timeout_seconds: float = 30.0) -> tuple[int, dict[str, Any], str]:
    request = Request(url, headers={"Accept": "application/json"})
    try:
        with urlopen(request, timeout=timeout_seconds) as response:  # noqa: S310 - localhost operator smoke probe
            status = int(response.status)
            body = response.read().decode("utf-8", errors="replace")
    except (TimeoutError, URLError) as exc:
        return 0, {}, str(exc)
    try:
        payload = json.loads(body)
    except json.JSONDecodeError as exc:
        return status, {}, f"PAYLOAD_INVALID:{exc}"
    return status, payload if isinstance(payload, dict) else {}, body


def _safe_int(value: Any, default: int = 0) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def _first_int_value(*values: Any, default: int = 0) -> int:
    for value in values:
        if value is None or value == "":
            continue
        return _safe_int(value, default)
    return default


def _load_json(path: Path) -> dict[str, Any]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}
    return payload if isinstance(payload, dict) else {}


def _expected_paths(truth_root: Path, day: str) -> list[dict[str, Any]]:
    specs = [
        ("candidate_generation_diagnostics", "aegis_candidate_generation_diagnostics_v1", "candidate_generation_diagnostics.v1.json", f"TARGET_DAY={day} npm run aegis:candidate-diagnostics"),
        ("candidate_contracts", "aegis_candidate_contracts_v1", "candidate_contracts.v1.json", f"TARGET_DAY={day} npm run aegis:candidate-contracts"),
        ("paper_review_queue", "aegis_paper_review_queue_v1", "paper_review_queue.v1.json", f"TARGET_DAY={day} npm run aegis:paper:review-queue"),
        ("run_history", "aegis_run_history_v1", "run_history.v1.json", f"TARGET_DAY={day} npm run aegis:run-history"),
        ("canonical_operator_state", "aegis_canonical_operator_state_v1", "canonical_operator_state.v1.json", f"TARGET_DAY={day} npm run aegis:canonical-operator-state"),
    ]
    rows = []
    for source, family, filename, repair in specs:
        path = (truth_root / "reports" / family / day / filename).resolve()
        rows.append({"source": source, "path": str(path), "exists": path.exists(), "repair_command": repair})
    return rows


def _projection_present_or_missing_paths(payload: dict[str, Any], truth_root: Path, day: str) -> tuple[bool, list[dict[str, Any]]]:
    projection = payload.get("candidate_ui_projection") if isinstance(payload.get("candidate_ui_projection"), dict) else {}
    if projection and projection.get("projection_status") != "MISSING_PROJECTION":
        return True, []
    debug = payload.get("candidate_projection_debug") if isinstance(payload.get("candidate_projection_debug"), dict) else {}
    missing = []
    for row in _expected_paths(truth_root, day):
        key = f"{row['source']}_exists"
        exists = bool(debug.get(key, row["exists"]))
        if not exists:
            missing.append({**row, "exists": exists})
    if not missing and projection:
        return True, []
    return False, missing or _expected_paths(truth_root, day)


def _dashboard_matches_diagnostics(cockpit: dict[str, Any], diagnostics: dict[str, Any]) -> tuple[bool, dict[str, Any]]:
    projection = cockpit.get("candidate_ui_projection") if isinstance(cockpit.get("candidate_ui_projection"), dict) else {}
    run = projection.get("run_summary") if isinstance(projection.get("run_summary"), dict) else {}
    actual = {
        "sleeves_run": _safe_int(run.get("sleeves_run", projection.get("sleeves_run"))),
        "sleeves_expected": _safe_int(run.get("sleeves_expected", projection.get("sleeves_expected"))),
        "raw_signals": _safe_int(run.get("raw_signals", projection.get("raw_signal_count"))),
        "diagnostic_candidate_outputs": _safe_int(run.get("diagnostic_candidate_outputs", projection.get("diagnostic_candidate_outputs"))),
        "rejected_count": _safe_int(run.get("rejected_count", projection.get("rejected_count"))),
    }
    expected = {
        "sleeves_run": _safe_int(diagnostics.get("total_sleeves_run")),
        "sleeves_expected": _first_int_value(diagnostics.get("total_sleeves_expected"), diagnostics.get("total_sleeves_expected_today")),
        "raw_signals": _safe_int(diagnostics.get("total_raw_signals")),
        "diagnostic_candidate_outputs": _first_int_value(diagnostics.get("diagnostic_candidate_outputs"), diagnostics.get("total_candidates_generated")),
        "rejected_count": _safe_int(diagnostics.get("total_candidates_rejected")),
    }
    return actual == expected, {"actual": actual, "expected": expected}


def main() -> int:
    parser = argparse.ArgumentParser(prog="run_aegis_portal_smoke_v1")
    parser.add_argument("--base-url", default="http://127.0.0.1:8787")
    parser.add_argument("--truth-root", default="/home/node/constellation_runtime_data/truth")
    parser.add_argument("--day", default="")
    parser.add_argument("--timeout-seconds", type=float, default=30.0, help="Per-endpoint timeout. Operator cockpit and state snapshot are comprehensive JSON read models and can be large on historical sessions.")
    args = parser.parse_args()

    day = args.day or __import__("datetime").date.today().isoformat()
    truth_root = Path(args.truth_root).expanduser().resolve()
    endpoints = {
        "state_snapshot_latest": "/api/aegis/operator/state-snapshot/latest",
        "operator_cockpit": "/api/aegis/operator-cockpit",
        "runtime_debug": "/api/aegis/runtime-debug",
    }
    responses: dict[str, dict[str, Any]] = {}
    checks: list[dict[str, Any]] = []
    for name, path in endpoints.items():
        separator = "&" if "?" in path else "?"
        url = f"{args.base_url.rstrip('/')}{path}{separator}day={day}"
        status, payload, raw_or_error = _fetch_json(url, timeout_seconds=float(args.timeout_seconds))
        responses[name] = payload
        checks.append({"check": f"{path} returns 200", "ok": status == 200, "status": status, "error": "" if status == 200 else raw_or_error[:500]})
        checks.append({"check": f"{path} has no payload ReferenceError", "ok": "payload is not defined" not in raw_or_error, "status": status})

    cockpit = responses.get("operator_cockpit", {})
    state_snapshot = responses.get("state_snapshot_latest", {})
    runtime_debug = responses.get("runtime_debug", {})
    projection_ok, missing_paths = _projection_present_or_missing_paths(cockpit, truth_root, day)
    checks.append({"check": "candidate_ui_projection present or exact missing paths shown", "ok": projection_ok or bool(missing_paths), "missing_paths": missing_paths})
    debug = runtime_debug.get("candidate_projection_debug") if isinstance(runtime_debug.get("candidate_projection_debug"), dict) else {}
    checks.append({"check": "day_path_invariant_ok true", "ok": debug.get("day_path_invariant_ok") is True, "value": debug.get("day_path_invariant_ok")})

    diagnostics_path = truth_root / "reports" / "aegis_candidate_generation_diagnostics_v1" / day / "candidate_generation_diagnostics.v1.json"
    diagnostics = _load_json(diagnostics_path)
    if diagnostics:
        matches, detail = _dashboard_matches_diagnostics(cockpit, diagnostics)
        checks.append({"check": "dashboard run_visibility matches diagnostics artifact counts", "ok": matches, **detail})
    else:
        checks.append({"check": "dashboard run_visibility matches diagnostics artifact counts", "ok": False, "missing_diagnostics_path": str(diagnostics_path)})

    checks.append({"check": "state snapshot latest returned requested/current day", "ok": str(state_snapshot.get("day_utc") or state_snapshot.get("requested_day") or "") == day or str(state_snapshot.get("source_day") or "") == day, "day": state_snapshot.get("day_utc"), "requested_day": state_snapshot.get("requested_day"), "source_day": state_snapshot.get("source_day")})
    checks.append({"check": "policy gates unchanged", "ok": cockpit.get("safety", {}).get("broker_submit_transmit_allowed") is False and cockpit.get("safety", {}).get("autonomous_execution_allowed") is False})

    ok = all(bool(row.get("ok")) for row in checks)
    result = {
        "ok": ok,
        "day_utc": day,
        "base_url": args.base_url,
        "truth_root": str(truth_root),
        "checks": checks,
        "candidate_projection_debug": debug,
    }
    print(json.dumps(result, indent=2, sort_keys=True))
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())
