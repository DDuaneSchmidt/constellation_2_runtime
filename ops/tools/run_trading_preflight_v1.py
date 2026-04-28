#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import re
import subprocess
import sys
from datetime import UTC, datetime
from pathlib import Path
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen
from zoneinfo import ZoneInfo

REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from ops.tools.repo_protection_common_v1 import RUNTIME_DATA_ROOT

DATE_RE = re.compile(r"^\d{4}-\d{2}-\d{2}$")
PACKET_GENERATED_RE = re.compile(r"^- generated_at_utc:\s*(\S+)\s*$", re.MULTILINE)
READY_VALUES = {"READY", "AUTHORIZED", "PASS", "OK", "HEALTHY", "INACTIVE"}
NY_TZ = ZoneInfo("America/New_York")
PREPARE_DECISION_PATH = (
    RUNTIME_DATA_ROOT / "runtime" / "process_state" / "trading_readiness_decision.json"
).resolve()
PREPARE_DECISION_SCHEMA = "trading_readiness_decision.v1"
PREPARE_REQUIRED_GATE_KEYS = [
    "submit_boundary_status",
    "aegis_day_closure_authority",
    "kill_switch",
    "capital_snapshot",
    "packet_freshness",
    "paper_mode",
]


def _utc_now() -> datetime:
    return datetime.now(UTC).replace(microsecond=0)


def _iso_utc(ts: datetime) -> str:
    return ts.isoformat().replace("+00:00", "Z")


def _today_local_day() -> str:
    return datetime.now(NY_TZ).date().isoformat()


def _read_json(path: Path) -> dict[str, Any] | None:
    if not path.exists() or not path.is_file():
        return None
    try:
        obj = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return None
    return obj if isinstance(obj, dict) else None


def _parse_iso_utc(raw: str) -> datetime | None:
    value = str(raw or "").strip()
    if not value:
        return None
    if value.endswith("Z"):
        value = value[:-1] + "+00:00"
    try:
        return datetime.fromisoformat(value).astimezone(UTC).replace(microsecond=0)
    except ValueError:
        return None


def _parse_day(raw: str) -> datetime | None:
    if not DATE_RE.match(raw):
        return None
    try:
        return datetime.strptime(raw, "%Y-%m-%d").replace(tzinfo=UTC)
    except ValueError:
        return None


def _extract_packet_generated(packet_path: Path) -> datetime | None:
    if not packet_path.exists() or not packet_path.is_file():
        return None
    text = packet_path.read_text(encoding="utf-8")
    match = PACKET_GENERATED_RE.search(text)
    if not match:
        return None
    return _parse_iso_utc(match.group(1))


def _fetch_json(url: str, timeout: float = 3.0) -> tuple[int, dict[str, Any] | None, str]:
    req = Request(url, method="GET")
    try:
        with urlopen(req, timeout=timeout) as resp:
            body = resp.read().decode("utf-8", errors="replace")
            code = int(resp.status)
    except HTTPError as exc:
        code = int(exc.code)
        try:
            body = exc.read().decode("utf-8", errors="replace")
        except Exception:
            body = ""
    except URLError as exc:
        return 0, None, f"{type(exc).__name__}:{exc}"
    except Exception as exc:
        return 0, None, f"{type(exc).__name__}:{exc}"

    if not body.strip():
        return code, None, ""
    try:
        payload = json.loads(body)
    except Exception:
        return code, None, "NON_JSON_RESPONSE"
    if not isinstance(payload, dict):
        return code, None, "TOP_LEVEL_NOT_OBJECT"
    return code, payload, ""


def _run_supervisor_status() -> tuple[int, dict[str, Any] | None, str]:
    cmd = ["python3", str((REPO_ROOT / "ops" / "runtime" / "supervisor.py").resolve()), "status"]
    proc = subprocess.run(
        cmd,
        cwd=str(REPO_ROOT),
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        check=False,
    )
    if proc.returncode != 0:
        return proc.returncode, None, proc.stderr.strip()
    text = proc.stdout.strip()
    if not text:
        return proc.returncode, None, "EMPTY_STATUS_STDOUT"
    try:
        obj = json.loads(text)
    except Exception:
        start = text.find("{")
        end = text.rfind("}")
        if start < 0 or end <= start:
            return proc.returncode, None, "STATUS_JSON_PARSE_FAILED"
        try:
            obj = json.loads(text[start : end + 1])
        except Exception:
            return proc.returncode, None, "STATUS_JSON_PARSE_FAILED"
    if not isinstance(obj, dict):
        return proc.returncode, None, "STATUS_JSON_NOT_OBJECT"
    return proc.returncode, obj, ""


def _market_calendar_entry(truth_root: Path, day_utc: str) -> tuple[dict[str, Any] | None, Path | None]:
    year = day_utc[:4]
    for exchange in ("NYSE", "NASDAQ"):
        candidate = (truth_root / "market_calendar_v1" / exchange / f"{year}.jsonl").resolve()
        if not candidate.exists() or not candidate.is_file():
            continue
        try:
            for raw in candidate.read_text(encoding="utf-8").splitlines():
                row = raw.strip()
                if not row:
                    continue
                obj = json.loads(row)
                if not isinstance(obj, dict):
                    continue
                if str(obj.get("day_utc") or "").strip() == day_utc:
                    return obj, candidate
        except Exception:
            continue
    return None, None


def _gate_row(
    *,
    gate: str,
    passed: bool,
    reason_code: str,
    evidence_path: str = "",
    detail: str = "",
) -> dict[str, Any]:
    return {
        "gate": gate,
        "passed": bool(passed),
        "reason_code": reason_code,
        "evidence_path": evidence_path,
        "detail": detail,
    }


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Fail-closed paper trading preflight")
    parser.add_argument("--day_utc", default=_today_local_day(), help="Target day (YYYY-MM-DD)")
    parser.add_argument("--mode", default="PAPER", help="Trading mode (must be PAPER)")
    parser.add_argument("--capital_max_age_days", type=int, default=2)
    parser.add_argument("--packet_max_age_hours", type=int, default=48)
    parser.add_argument(
        "--allow-missing-decision",
        action="store_true",
        help="Allow missing trading_readiness_decision.json (used by trading:prepare only).",
    )
    parser.add_argument(
        "--skip-decision-gate",
        action="store_true",
        help="Skip decision-artifact validation (used by trading:prepare only).",
    )
    parser.add_argument("--json", action="store_true", help="Emit JSON output (default)")
    return parser


def _validate_prepare_decision_gate(*, day_utc: str, mode: str, allow_missing: bool) -> dict[str, Any]:
    decision = _read_json(PREPARE_DECISION_PATH)
    if decision is None:
        return _gate_row(
            gate="prepare_decision_valid",
            passed=bool(allow_missing),
            reason_code="PREPARE_DECISION_OPTIONAL_MISSING" if allow_missing else "PREPARE_DECISION_MISSING",
            evidence_path=str(PREPARE_DECISION_PATH),
            detail="trading:prepare decision artifact not found",
        )

    errors: list[str] = []
    schema = str(decision.get("schema") or "").strip()
    if schema != PREPARE_DECISION_SCHEMA:
        errors.append(f"PREPARE_DECISION_SCHEMA_INVALID:schema={schema!r}")

    decision_value = str(decision.get("decision") or "").strip().upper()
    if decision_value != "GO":
        errors.append(f"PREPARE_DECISION_NOT_GO:decision={decision_value}")

    decision_mode = str(decision.get("mode") or "").strip().upper()
    if decision_mode != "PAPER":
        errors.append(f"PREPARE_DECISION_MODE_NOT_PAPER:mode={decision_mode}")
    if mode != "PAPER":
        errors.append(f"MODE_NOT_PAPER:mode={mode}")

    decision_day = str(decision.get("trading_day") or "").strip()
    if decision_day != day_utc:
        errors.append(f"PREPARE_DECISION_DAY_MISMATCH:decision_day={decision_day} target_day={day_utc}")

    expires_at_raw = str(decision.get("expires_at_utc") or "").strip()
    expires_at = _parse_iso_utc(expires_at_raw)
    if expires_at is None:
        errors.append("PREPARE_DECISION_EXPIRY_INVALID")
    elif expires_at <= _utc_now():
        errors.append(f"PREPARE_DECISION_EXPIRED:expires_at_utc={_iso_utc(expires_at)}")

    gates_obj = decision.get("gates")
    if not isinstance(gates_obj, dict):
        errors.append("PREPARE_DECISION_GATES_MISSING")
        gates_obj = {}
    for gate_key in PREPARE_REQUIRED_GATE_KEYS:
        gate_payload = gates_obj.get(gate_key)
        if not isinstance(gate_payload, dict):
            errors.append(f"PREPARE_DECISION_GATE_MISSING:{gate_key}")
            continue
        status = str(gate_payload.get("status") or "").strip().upper()
        if not status:
            errors.append(f"PREPARE_DECISION_GATE_STATUS_MISSING:{gate_key}")
        if gate_key != "paper_mode":
            evidence = str(gate_payload.get("evidence") or "").strip()
            if not evidence:
                errors.append(f"PREPARE_DECISION_GATE_EVIDENCE_MISSING:{gate_key}")
        if gate_key == "paper_mode":
            if status != "PASS":
                errors.append(f"PREPARE_DECISION_PAPER_MODE_FAIL:status={status}")
            declared_mode = str(gate_payload.get("mode") or "").strip().upper()
            if declared_mode != "PAPER":
                errors.append(f"PREPARE_DECISION_PAPER_MODE_MISMATCH:mode={declared_mode}")
        elif status not in READY_VALUES:
            errors.append(f"PREPARE_DECISION_GATE_NOT_READY:{gate_key}:{status}")
        if gate_key == "kill_switch" and gate_payload.get("allow_entries") is not True:
            errors.append("PREPARE_DECISION_KILL_SWITCH_NOT_ALLOWING_ENTRIES")

    if errors:
        return _gate_row(
            gate="prepare_decision_valid",
            passed=False,
            reason_code="PREPARE_DECISION_INVALID",
            evidence_path=str(PREPARE_DECISION_PATH),
            detail="; ".join(errors),
        )
    return _gate_row(
        gate="prepare_decision_valid",
        passed=True,
        reason_code="PREPARE_DECISION_GO",
        evidence_path=str(PREPARE_DECISION_PATH),
        detail=f"decision=GO mode=PAPER trading_day={day_utc}",
    )


def main(argv: list[str] | None = None) -> int:
    parser = _build_parser()
    args = parser.parse_args(argv)

    truth_root = (RUNTIME_DATA_ROOT / "truth").resolve()
    day_utc = str(args.day_utc or "").strip()
    mode = str(args.mode or "").strip().upper()
    gates: list[dict[str, Any]] = []

    day_dt = _parse_day(day_utc)
    if day_dt is None:
        gates.append(
            _gate_row(
                gate="target_day_resolved",
                passed=False,
                reason_code="INVALID_DAY_UTC",
                detail=f"day_utc={day_utc!r}",
            )
        )
    else:
        gates.append(
            _gate_row(
                gate="target_day_resolved",
                passed=True,
                reason_code="TARGET_DAY_OK",
                detail=day_utc,
            )
        )

    if mode != "PAPER":
        gates.append(
            _gate_row(
                gate="mode_is_paper_only",
                passed=False,
                reason_code="MODE_NOT_PAPER",
                detail=f"mode={mode}",
            )
        )
    else:
        gates.append(
            _gate_row(
                gate="mode_is_paper_only",
                passed=True,
                reason_code="MODE_PAPER_OK",
                detail=f"mode={mode}",
            )
        )

    if bool(args.skip_decision_gate):
        gates.append(
            _gate_row(
                gate="prepare_decision_valid",
                passed=True,
                reason_code="PREPARE_DECISION_GATE_SKIPPED",
                evidence_path=str(PREPARE_DECISION_PATH),
                detail="skipped for trading:prepare raw gate evaluation",
            )
        )
    else:
        gates.append(
            _validate_prepare_decision_gate(
                day_utc=day_utc,
                mode=mode,
                allow_missing=bool(args.allow_missing_decision),
            )
        )

    market_entry, market_path = _market_calendar_entry(truth_root, day_utc)
    if market_entry is None:
        gates.append(
            _gate_row(
                gate="market_calendar_trading_day",
                passed=False,
                reason_code="MISSING_MARKET_CALENDAR_DAY",
                evidence_path=str(market_path or ""),
                detail=f"day_utc={day_utc}",
            )
        )
    else:
        is_trading = market_entry.get("is_trading_session") is True
        gates.append(
            _gate_row(
                gate="market_calendar_trading_day",
                passed=is_trading,
                reason_code="TRADING_DAY_OK" if is_trading else "NON_TRADING_DAY",
                evidence_path=str(market_path or ""),
                detail=f"is_trading_session={market_entry.get('is_trading_session')!r}",
            )
        )

    submit_boundary_path = (
        truth_root / "reports" / "submit_boundary_status_v1" / day_utc / "submit_boundary_status.v1.json"
    ).resolve()
    submit_boundary = _read_json(submit_boundary_path)
    if submit_boundary is None:
        gates.append(
            _gate_row(
                gate="submit_boundary_ready_authorized",
                passed=False,
                reason_code="MISSING_SUBMIT_BOUNDARY_STATUS",
                evidence_path=str(submit_boundary_path),
            )
        )
    else:
        boundary_status = str(submit_boundary.get("boundary_status") or submit_boundary.get("status") or "").strip().upper()
        boundary_pass = boundary_status in {"READY", "AUTHORIZED"}
        blocker = str(submit_boundary.get("canonical_blocker") or "").strip()
        gates.append(
            _gate_row(
                gate="submit_boundary_ready_authorized",
                passed=boundary_pass,
                reason_code="SUBMIT_BOUNDARY_OK" if boundary_pass else "SUBMIT_BOUNDARY_NOT_AUTHORIZED",
                evidence_path=str(submit_boundary_path),
                detail=f"boundary_status={boundary_status} canonical_blocker={blocker}",
            )
        )

    closure_path = (
        truth_root / "reports" / "aegis_day_closure_authority_v1" / day_utc / "aegis_day_closure_authority.v1.json"
    ).resolve()
    closure_obj = _read_json(closure_path)
    if closure_obj is None:
        gates.append(
            _gate_row(
                gate="closure_authority_pass",
                passed=False,
                reason_code="MISSING_CLOSURE_AUTHORITY",
                evidence_path=str(closure_path),
            )
        )
    else:
        closure_status = str(closure_obj.get("status") or "").strip().upper()
        closure_pass = closure_status == "PASS"
        blocker = str(closure_obj.get("canonical_blocker") or "").strip()
        gates.append(
            _gate_row(
                gate="closure_authority_pass",
                passed=closure_pass,
                reason_code="CLOSURE_AUTHORITY_OK" if closure_pass else "CLOSURE_AUTHORITY_NOT_PASS",
                evidence_path=str(closure_path),
                detail=f"status={closure_status} canonical_blocker={blocker}",
            )
        )

    kill_switch_path = (truth_root / "risk_v1" / "kill_switch_v1" / day_utc / "global_kill_switch_state.v1.json").resolve()
    kill_switch = _read_json(kill_switch_path)
    if kill_switch is None:
        gates.append(
            _gate_row(
                gate="kill_switch_inactive_entries_allowed",
                passed=False,
                reason_code="MISSING_KILL_SWITCH_STATE",
                evidence_path=str(kill_switch_path),
            )
        )
    else:
        state = str(kill_switch.get("state") or "").strip().upper()
        allow_entries = kill_switch.get("allow_entries") is True
        kill_ok = state == "INACTIVE" and allow_entries
        gates.append(
            _gate_row(
                gate="kill_switch_inactive_entries_allowed",
                passed=kill_ok,
                reason_code="KILL_SWITCH_OK" if kill_ok else "KILL_SWITCH_NOT_ALLOWING_ENTRIES",
                evidence_path=str(kill_switch_path),
                detail=f"state={state} allow_entries={allow_entries}",
            )
        )

    packet_path = (RUNTIME_DATA_ROOT / "exports" / "aegis_state" / "latest" / "chatgpt_aegis_packet.md").resolve()
    packet_generated = _extract_packet_generated(packet_path)
    if packet_generated is None:
        gates.append(
            _gate_row(
                gate="packet_freshness_valid",
                passed=False,
                reason_code="PACKET_MISSING_OR_INVALID_GENERATED_AT",
                evidence_path=str(packet_path),
            )
        )
    else:
        age_hours = (_utc_now() - packet_generated).total_seconds() / 3600.0
        packet_ok = age_hours <= float(args.packet_max_age_hours)
        gates.append(
            _gate_row(
                gate="packet_freshness_valid",
                passed=packet_ok,
                reason_code="PACKET_FRESH" if packet_ok else "PACKET_STALE",
                evidence_path=str(packet_path),
                detail=f"generated_at_utc={_iso_utc(packet_generated)} age_hours={age_hours:.2f}",
            )
        )

    status_rc, status_obj, status_err = _run_supervisor_status()
    if status_rc != 0 or status_obj is None:
        gates.append(
            _gate_row(
                gate="app_runtime_status_ready",
                passed=False,
                reason_code="APP_STATUS_COMMAND_FAILED",
                evidence_path=str((REPO_ROOT / "ops" / "runtime" / "supervisor.py").resolve()),
                detail=status_err or f"rc={status_rc}",
            )
        )
    else:
        overall_status = str(status_obj.get("overall_status") or "").strip().upper()
        runtime_ok = overall_status == "READY"
        gates.append(
            _gate_row(
                gate="app_runtime_status_ready",
                passed=runtime_ok,
                reason_code="APP_RUNTIME_READY" if runtime_ok else "APP_RUNTIME_NOT_READY",
                evidence_path=str((RUNTIME_DATA_ROOT / "runtime" / "process_state" / "service_status.json").resolve()),
                detail=f"overall_status={overall_status}",
            )
        )

    health_url = "http://127.0.0.1:8787/health"
    health_code, health_payload, health_err = _fetch_json(health_url)
    if health_code != 200 or health_payload is None:
        gates.append(
            _gate_row(
                gate="health_endpoint_ready",
                passed=False,
                reason_code="HEALTH_ENDPOINT_NOT_READY",
                evidence_path=health_url,
                detail=f"http_status={health_code} error={health_err}",
            )
        )
    else:
        status_value = str(health_payload.get("status") or "").strip().upper()
        health_ok = status_value in READY_VALUES
        gates.append(
            _gate_row(
                gate="health_endpoint_ready",
                passed=health_ok,
                reason_code="HEALTH_ENDPOINT_READY" if health_ok else "HEALTH_ENDPOINT_STATUS_NOT_READY",
                evidence_path=health_url,
                detail=f"http_status={health_code} status={status_value}",
            )
        )

    capital_url = "http://127.0.0.1:8787/api/capital/overview"
    capital_code, capital_payload, capital_err = _fetch_json(capital_url)
    gates.append(
        _gate_row(
            gate="capital_api_http_200",
            passed=capital_code == 200 and capital_payload is not None,
            reason_code="CAPITAL_API_OK" if capital_code == 200 and capital_payload is not None else "CAPITAL_API_NOT_OK",
            evidence_path=capital_url,
            detail=f"http_status={capital_code} error={capital_err}",
        )
    )

    if capital_payload is None:
        gates.append(
            _gate_row(
                gate="capital_snapshot_fresh",
                passed=False,
                reason_code="CAPITAL_SNAPSHOT_UNPROVEN",
                evidence_path=capital_url,
                detail="capital overview payload missing",
            )
        )
    else:
        as_of_date = str(((capital_payload.get("basis") or {}).get("as_of_date")) or "").strip()
        fresh_status = str(((capital_payload.get("freshness_completeness") or {}).get("status")) or "").strip().upper()
        target_day = day_dt.date() if day_dt is not None else _utc_now().date()
        as_of_dt = _parse_day(as_of_date)
        if as_of_dt is None:
            gates.append(
                _gate_row(
                    gate="capital_snapshot_fresh",
                    passed=False,
                    reason_code="CAPITAL_AS_OF_DATE_INVALID",
                    evidence_path=capital_url,
                    detail=f"as_of_date={as_of_date!r} freshness_status={fresh_status}",
                )
            )
        else:
            age_days = (target_day - as_of_dt.date()).days
            capital_fresh = (
                age_days >= 0
                and age_days <= int(args.capital_max_age_days)
                and fresh_status in {"HEALTHY", "FRESH", "OK"}
            )
            gates.append(
                _gate_row(
                    gate="capital_snapshot_fresh",
                    passed=capital_fresh,
                    reason_code="CAPITAL_SNAPSHOT_FRESH" if capital_fresh else "CAPITAL_SNAPSHOT_STALE",
                    evidence_path=capital_url,
                    detail=(
                        f"as_of_date={as_of_date} target_day={target_day.isoformat()} "
                        f"age_days={age_days} freshness_status={fresh_status}"
                    ),
                )
            )

    go = all(bool(g.get("passed")) for g in gates)
    blockers = [str(g.get("reason_code") or "") for g in gates if not bool(g.get("passed"))]
    result = {
        "schema_id": "trading_preflight_v1",
        "generated_at_utc": _iso_utc(_utc_now()),
        "repo_root": str(REPO_ROOT),
        "truth_root": str(truth_root),
        "intended_day_utc": day_utc,
        "mode": mode,
        "status": "GO" if go else "NO_GO",
        "go": go,
        "gates": gates,
        "blocking_reason_codes": blockers,
    }
    print(json.dumps(result, indent=2, sort_keys=True))
    return 0 if go else 2


if __name__ == "__main__":
    raise SystemExit(main())
