#!/usr/bin/env python3
from __future__ import annotations

import argparse
from datetime import UTC, datetime
import json
from pathlib import Path
import sys
import time
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen

REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from constellation_2.aegis_truth.evidence_event_v1 import build_event
from constellation_2.aegis_truth.evidence_ledger_v1 import append_event


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--target-day", required=True)
    ap.add_argument("--truth-root", default="/home/node/constellation_runtime_data/truth")
    ap.add_argument("--environment", choices=["PAPER", "LIVE", "UNKNOWN"], default="UNKNOWN")
    ap.add_argument("--portal-url", default="https://portal.schmidtvault.com/healthz")
    ap.add_argument("--origin-health-url", default="")
    ap.add_argument("--timeout-seconds", type=float, default=5.0)
    ap.add_argument("--projection-max-age-seconds", type=int, default=300)
    ap.add_argument("--simulate-status", type=int, default=None)
    args = ap.parse_args()
    event = probe(args)
    append_event(event, truth_root=args.truth_root)
    print(json.dumps({"event_id": event["event_id"], "event_type": event["event_type"], "status": event["status"], "severity": event["severity"]}, sort_keys=True))
    return 0 if event["severity"] != "CRITICAL" else 2


def probe(args: argparse.Namespace) -> dict:
    started = time.time()
    payload: dict = {"portal_url": args.portal_url, "origin_health_url": args.origin_health_url or None}
    if args.simulate_status is not None:
        payload["http_status"] = args.simulate_status
        return _event_for_http_status(args, payload, elapsed=time.time() - started)
    try:
        request = Request(args.portal_url, headers={"User-Agent": "AegisPortalProbe/1.0 (+local-monitor)", "Accept": "application/json,text/plain,*/*"})
        with urlopen(request, timeout=args.timeout_seconds) as response:
            body = response.read(256_000)
            payload["http_status"] = response.status
            payload["body_bytes"] = len(body)
            if response.status != 200:
                return _event_for_http_status(args, payload, elapsed=time.time() - started)
            try:
                data = json.loads(body.decode("utf-8"))
            except Exception as exc:
                payload["parse_error"] = f"{type(exc).__name__}: {exc}"
                return _build(args, "PORTAL_HEALTH_SCHEMA_INVALID", "UNKNOWN", "portal health schema invalid", "ERROR", payload)
            payload["health"] = data
            if data.get("schema_version") != "aegis_portal_health.v1":
                return _build(args, "PORTAL_HEALTH_SCHEMA_INVALID", "UNKNOWN", "portal health schema invalid", "ERROR", payload)
            health_status = data.get("status")
            if health_status not in {"OK", "DEGRADED", "BLOCKED", "UNKNOWN"}:
                return _build(args, "PORTAL_HEALTH_SCHEMA_INVALID", "UNKNOWN", "portal health status invalid", "ERROR", payload)
            if health_status != "OK":
                severity = "CRITICAL" if health_status == "BLOCKED" else "ERROR"
                return _build(args, "PORTAL_AVAILABILITY_OBSERVED", health_status, data.get("blocker") or f"portal health reported {health_status}", severity, payload)
            projection_age = data.get("projection_age_seconds")
            if isinstance(projection_age, (int, float)) and projection_age > args.projection_max_age_seconds:
                payload["projection_age_seconds"] = projection_age
                severity = "CRITICAL" if projection_age > args.projection_max_age_seconds * 4 else "WARN"
                status = "BLOCKED" if severity == "CRITICAL" else "STALE"
                return _build(args, "PORTAL_PROJECTION_STALE", status, "portal projection stale", severity, payload)
            return _build(args, "PORTAL_AVAILABILITY_OBSERVED", "OK", None, "INFO", payload)
    except HTTPError as exc:
        payload["http_status"] = exc.code
        payload["error"] = str(exc)
        return _event_for_http_status(args, payload, elapsed=time.time() - started)
    except (TimeoutError, URLError) as exc:
        payload["error"] = f"{type(exc).__name__}: {exc}"
        return _build(args, "PORTAL_ORIGIN_UNREACHABLE", "BLOCKED", "portal origin timeout or unreachable", "CRITICAL", payload)


def _event_for_http_status(args: argparse.Namespace, payload: dict, elapsed: float) -> dict:
    payload["elapsed_seconds"] = round(elapsed, 3)
    code = int(payload.get("http_status") or 0)
    if code in {502, 503, 504}:
        return _build(args, "PORTAL_ORIGIN_UNREACHABLE", "BLOCKED", f"portal returned HTTP {code}", "CRITICAL", payload)
    if code == 404:
        return _build(args, "PORTAL_HEALTH_ENDPOINT_MISSING", "MISSING", "portal /healthz endpoint missing", "ERROR", payload)
    return _build(args, "PORTAL_AVAILABILITY_OBSERVED", "DEGRADED", f"portal returned HTTP {code}", "ERROR", payload)


def _build(args: argparse.Namespace, event_type: str, status: str, blocker: str | None, severity: str, payload: dict) -> dict:
    return build_event(
        event_type=event_type,
        producer="aegis.portal_availability_probe_v1",
        target_day=args.target_day,
        environment=args.environment,
        status=status,
        blocker=blocker,
        owner="portal",
        severity=severity,
        payload=payload,
        next_action="Repair portal origin/tunnel or refresh projection, then rerun portal probe." if severity in {"ERROR", "CRITICAL"} else "No operator action required.",
    )


if __name__ == "__main__":
    raise SystemExit(main())
