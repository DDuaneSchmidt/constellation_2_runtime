#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
from datetime import UTC, datetime
from pathlib import Path
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen

REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from constellation_2.common.runtime_contract_v1 import resolve_canonical_truth_root
from ops.runtime import supervisor


SCHEMA_VERSION = "ui_service_authority.v1"
DEFAULT_HOST = "127.0.0.1"
DEFAULT_PORT = 8787
PROJECTION_CONTRACT_VERSION = "aegis_ui_projection.v1"


def _utc_now() -> str:
    return datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _read_json_url(url: str, timeout_seconds: float = 2.0) -> dict[str, Any]:
    result: dict[str, Any] = {
        "ok": False,
        "http_status": None,
        "error": None,
        "payload": {},
        "url": url,
    }
    try:
        with urlopen(Request(url, method="GET"), timeout=timeout_seconds) as response:
            raw = response.read().decode("utf-8", errors="replace")
            result["http_status"] = int(response.status)
            payload = json.loads(raw) if raw else {}
            result["payload"] = payload if isinstance(payload, dict) else {}
            result["ok"] = 200 <= int(response.status) < 400
    except HTTPError as exc:
        result["http_status"] = int(exc.code)
        result["error"] = f"HTTP_{exc.code}"
    except URLError as exc:
        result["error"] = f"URL_ERROR:{exc.reason}"
    except Exception as exc:
        result["error"] = f"{type(exc).__name__}:{exc}"
    return result


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(payload, sort_keys=True, separators=(",", ":"), ensure_ascii=True) + "\n",
        encoding="utf-8",
    )


def build_ui_service_authority(
    *,
    day_utc: str,
    environment: str,
    host: str = DEFAULT_HOST,
    port: int = DEFAULT_PORT,
) -> dict[str, Any]:
    truth_root = resolve_canonical_truth_root().resolve()
    manifest = supervisor.DEFAULT_MANIFEST_PATH
    specs = supervisor.load_manifest(manifest)
    service = next((item for item in specs if item.name == "ops_dashboard"), None)
    snapshot = supervisor.probe_service(service) if service is not None else {}
    healthz = _read_json_url(f"http://{host}:{port}/healthz")
    readyz = _read_json_url(f"http://{host}:{port}/readyz")

    health_payload = healthz.get("payload") if isinstance(healthz.get("payload"), dict) else {}
    ready_payload = readyz.get("payload") if isinstance(readyz.get("payload"), dict) else {}
    route_status = snapshot.get("route_status") if isinstance(snapshot.get("route_status"), dict) else {}
    log_path = ""
    pid = None
    if service is not None:
        log_path = str(service.log_path)
    if isinstance(snapshot.get("pid"), int):
        pid = snapshot.get("pid")

    health_status = "PASS" if healthz.get("ok") and str(health_payload.get("status") or "").upper() in {"READY", "PASS", "OK"} else "FAIL"
    ready_status = str(ready_payload.get("status") or ("PASS" if readyz.get("ok") else "FAIL")).strip().upper()
    routes_status = str(route_status.get("status") or "FAIL").strip().upper()

    if health_status == "PASS" and ready_status == "PASS" and routes_status == "PASS":
        status = "PASS"
        next_action = "UI projection service is healthy."
    elif healthz.get("ok") or readyz.get("ok") or snapshot.get("pid_running"):
        status = "DEGRADED"
        next_action = "Inspect readyz/route status and run npm run aegis:ui:restart if the service does not recover."
    else:
        status = "FAIL"
        next_action = "Run npm run aegis:ui:restart and inspect /home/node/constellation_runtime_data/runtime/logs/ops_dashboard.log."

    latest_packet = Path("/home/node/constellation_runtime_data/exports/aegis_state/latest/chatgpt_aegis_packet.md").resolve()
    return {
        "schema_id": "ui_service_authority",
        "schema_version": SCHEMA_VERSION,
        "day_utc": day_utc,
        "environment": environment,
        "status": status,
        "host": host,
        "port": port,
        "pid": pid,
        "started_at_utc": str(health_payload.get("started_at_utc") or ""),
        "last_checked_at_utc": _utc_now(),
        "healthz_status": health_status,
        "readyz_status": ready_status,
        "route_status": route_status,
        "truth_root": str(truth_root),
        "latest_packet_path": str(latest_packet),
        "projection_contract_version": PROJECTION_CONTRACT_VERSION,
        "log_path": log_path,
        "operator_next_action": next_action,
        "evidence": {
            "healthz": healthz,
            "readyz": readyz,
            "supervisor_snapshot": snapshot,
        },
        "producer": "ops/tools/run_ui_service_authority_v1.py",
    }


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--day_utc", required=True)
    parser.add_argument("--environment", default="PAPER")
    parser.add_argument("--host", default=DEFAULT_HOST)
    parser.add_argument("--port", type=int, default=DEFAULT_PORT)
    args = parser.parse_args(argv)

    payload = build_ui_service_authority(
        day_utc=str(args.day_utc),
        environment=str(args.environment).strip().upper(),
        host=str(args.host),
        port=int(args.port),
    )
    out_path = (
        resolve_canonical_truth_root().resolve()
        / "reports"
        / "ui_service_authority_v1"
        / str(args.day_utc)
        / "ui_service_authority.v1.json"
    )
    _write_json(out_path, payload)
    print(json.dumps({"status": payload["status"], "path": str(out_path)}, sort_keys=True))
    return 0 if payload["status"] in {"PASS", "DEGRADED"} else 2


if __name__ == "__main__":
    raise SystemExit(main())
