#!/usr/bin/env python3
"""
Repo-native runtime supervisor for Constellation local services.

Single-writer contract:
- This module is the only writer for runtime/process_state/*.json runtime truth.
- Other scripts and UI readers may consume these files but must not write them.
"""

from __future__ import annotations

import argparse
import json
import os
import signal
import socket
import subprocess
import sys
import time
from collections import deque
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen


THIS_FILE = Path(__file__).resolve()
OPS_RUNTIME_ROOT = THIS_FILE.parent
REPO_ROOT = THIS_FILE.parents[2]
RUNTIME_ROOT = Path(os.environ.get("C2_RUNTIME_STATE_ROOT", "/home/node/constellation_runtime_data/runtime")).resolve()
PROCESS_STATE_ROOT = (RUNTIME_ROOT / "process_state").resolve()
LOG_ROOT = (RUNTIME_ROOT / "logs").resolve()
DEFAULT_MANIFEST_PATH = (OPS_RUNTIME_ROOT / "runtime_manifest.yaml").resolve()

SERVICE_STATUS_PATH = (PROCESS_STATE_ROOT / "service_status.json").resolve()
SUPERVISOR_STATE_PATH = (PROCESS_STATE_ROOT / "supervisor_state.json").resolve()
LAST_START_REPORT_PATH = (PROCESS_STATE_ROOT / "last_start_report.json").resolve()
SUPERVISOR_LOG_PATH = (LOG_ROOT / "supervisor.log").resolve()

STATE_READY = "READY"
STATE_NOT_RUNNING = "NOT_RUNNING"
STATE_DEGRADED = "DEGRADED"


class SupervisorError(RuntimeError):
    pass


@dataclass(frozen=True)
class ServiceSpec:
    name: str
    required: bool
    host: str
    port: int
    entrypoint: str
    entrypoint_path: Path
    health_url: str
    start_timeout_seconds: int
    stop_timeout_seconds: int

    @property
    def pid_path(self) -> Path:
        return (PROCESS_STATE_ROOT / f"{self.name}.pid").resolve()

    @property
    def log_path(self) -> Path:
        return (LOG_ROOT / f"{self.name}.log").resolve()


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _ensure_runtime_paths() -> None:
    PROCESS_STATE_ROOT.mkdir(parents=True, exist_ok=True)
    LOG_ROOT.mkdir(parents=True, exist_ok=True)


def _log(level: str, message: str) -> None:
    _ensure_runtime_paths()
    line = f"{utc_now_iso()} [{level}] {message}\n"
    with SUPERVISOR_LOG_PATH.open("a", encoding="utf-8") as handle:
        handle.write(line)
    sys.stderr.write(line)


def _write_json(path: Path, payload: Dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = path.with_suffix(path.suffix + ".tmp")
    tmp_path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    tmp_path.replace(path)


def _split_key_value(line: str, lineno: int) -> Tuple[str, str]:
    if ":" not in line:
        raise SupervisorError(f"MANIFEST_PARSE_ERROR line={lineno}: expected key:value")
    key, value = line.split(":", 1)
    return key.strip(), value.strip()


def _parse_scalar(raw: str) -> Any:
    value = raw.strip()
    if not value:
        return ""
    if (value.startswith('"') and value.endswith('"')) or (value.startswith("'") and value.endswith("'")):
        return value[1:-1]
    lowered = value.lower()
    if lowered == "true":
        return True
    if lowered == "false":
        return False
    try:
        return int(value)
    except ValueError:
        return value


def _normalize_manifest_rows(manifest_path: Path) -> List[Dict[str, Any]]:
    raw_services: List[Dict[str, Any]] = []
    current: Optional[Dict[str, Any]] = None
    in_services = False

    lines = manifest_path.read_text(encoding="utf-8").splitlines()
    for lineno, raw_line in enumerate(lines, start=1):
        if not raw_line.strip():
            continue
        stripped = raw_line.strip()
        if stripped.startswith("#"):
            continue
        indent = len(raw_line) - len(raw_line.lstrip(" "))

        if indent == 0 and stripped == "services:":
            in_services = True
            current = None
            continue

        if not in_services:
            continue

        if stripped.startswith("- "):
            current = {}
            raw_services.append(current)
            inline = stripped[2:].strip()
            if inline:
                key, value = _split_key_value(inline, lineno)
                current[key] = _parse_scalar(value)
            continue

        if current is None:
            raise SupervisorError(f"MANIFEST_PARSE_ERROR line={lineno}: field defined before any service item")
        key, value = _split_key_value(stripped, lineno)
        current[key] = _parse_scalar(value)

    if not raw_services:
        raise SupervisorError(f"MANIFEST_INVALID: no services found in {manifest_path}")
    return raw_services


def load_manifest(manifest_path: Path) -> List[ServiceSpec]:
    path = manifest_path.resolve()
    if not path.exists():
        raise SupervisorError(f"MANIFEST_MISSING: {path}")
    rows = _normalize_manifest_rows(path)
    specs: List[ServiceSpec] = []

    required_keys = {"name", "host", "port", "entrypoint", "health_url"}
    for row in rows:
        missing = sorted(required_keys.difference(row.keys()))
        if missing:
            raise SupervisorError(
                f"MANIFEST_INVALID_SERVICE: missing={','.join(missing)} service={row.get('name', '<unknown>')}"
            )

        name = str(row["name"]).strip()
        if not name:
            raise SupervisorError("MANIFEST_INVALID_SERVICE: service name is empty")

        host = str(row["host"]).strip()
        if not host:
            raise SupervisorError(f"MANIFEST_INVALID_SERVICE: host is empty for service={name}")

        try:
            port = int(row["port"])
        except (TypeError, ValueError) as exc:
            raise SupervisorError(f"MANIFEST_INVALID_SERVICE: port must be int for service={name}") from exc
        if port <= 0 or port > 65535:
            raise SupervisorError(f"MANIFEST_INVALID_SERVICE: port out of range for service={name}")

        entrypoint = str(row["entrypoint"]).strip()
        if not entrypoint:
            raise SupervisorError(f"MANIFEST_INVALID_SERVICE: entrypoint is empty for service={name}")

        entrypoint_path = (REPO_ROOT / entrypoint).resolve()
        if not entrypoint_path.exists():
            raise SupervisorError(f"SERVICE_ENTRYPOINT_MISSING: service={name} path={entrypoint_path}")
        if not str(entrypoint_path).startswith(str(REPO_ROOT) + os.sep):
            raise SupervisorError(f"SERVICE_ENTRYPOINT_OUTSIDE_REPO: service={name} path={entrypoint_path}")

        health_url = str(row["health_url"]).strip()
        if not health_url:
            raise SupervisorError(f"MANIFEST_INVALID_SERVICE: health_url is empty for service={name}")

        required = bool(row.get("required", True))
        start_timeout_seconds = int(row.get("start_timeout_seconds", 45))
        stop_timeout_seconds = int(row.get("stop_timeout_seconds", 15))

        specs.append(
            ServiceSpec(
                name=name,
                required=required,
                host=host,
                port=port,
                entrypoint=entrypoint,
                entrypoint_path=entrypoint_path,
                health_url=health_url,
                start_timeout_seconds=start_timeout_seconds,
                stop_timeout_seconds=stop_timeout_seconds,
            )
        )

    return specs


def _process_exists(pid: int) -> bool:
    if pid <= 0:
        return False
    try:
        os.kill(pid, 0)
        return True
    except OSError:
        return False


def _read_pid_file(path: Path) -> Optional[int]:
    if not path.exists():
        return None
    try:
        raw = path.read_text(encoding="utf-8").strip()
        return int(raw)
    except Exception:
        return None


def _read_cmdline(pid: int) -> List[str]:
    cmdline_path = Path("/proc") / str(pid) / "cmdline"
    try:
        raw = cmdline_path.read_bytes()
    except Exception:
        return []
    parts = [p for p in raw.decode("utf-8", errors="ignore").split("\x00") if p]
    return parts


def _pid_owned_by_service(pid: int, service: ServiceSpec) -> bool:
    parts = _read_cmdline(pid)
    if not parts:
        return False
    absolute_entry = str(service.entrypoint_path)
    relative_entry = service.entrypoint
    for part in parts:
        if part == absolute_entry or part.endswith(relative_entry):
            return True
    return False


def _is_port_listening(host: str, port: int, timeout_seconds: float = 0.35) -> bool:
    try:
        with socket.create_connection((host, port), timeout=timeout_seconds):
            return True
    except OSError:
        return False


def _health_probe(service: ServiceSpec, timeout_seconds: float = 1.5) -> Dict[str, Any]:
    result: Dict[str, Any] = {
        "url": service.health_url,
        "ok": False,
        "http_status": None,
        "error": None,
        "status_value": None,
    }
    req = Request(service.health_url, method="GET")
    try:
        with urlopen(req, timeout=timeout_seconds) as response:
            body = response.read().decode("utf-8", errors="replace")
            result["http_status"] = int(response.status)
            payload: Any = None
            try:
                payload = json.loads(body)
            except json.JSONDecodeError:
                payload = None
            if isinstance(payload, dict):
                status_value = str(payload.get("status", "")).strip()
                result["status_value"] = status_value
                result["ok"] = response.status == 200 and status_value.upper() in {"READY", "OK", "HEALTHY", "RUNNING"}
            else:
                result["ok"] = response.status == 200
    except HTTPError as exc:
        result["http_status"] = int(exc.code)
        result["error"] = f"HTTP_{exc.code}"
    except URLError as exc:
        result["error"] = f"URL_ERROR:{exc.reason}"
    except Exception as exc:
        result["error"] = f"HEALTH_ERROR:{exc}"
    return result


def _route_probe(service: ServiceSpec, timeout_seconds: float = 1.5) -> Dict[str, Any]:
    route_paths = ["/healthz", "/readyz", "/runtime-status", "/api/runtime-status", "/aegis-runtime"]
    routes: Dict[str, Any] = {}
    for route_path in route_paths:
        url = f"http://{service.host}:{service.port}{route_path}"
        row: Dict[str, Any] = {"url": url, "ok": False, "http_status": None, "error": None}
        req = Request(url, method="GET")
        try:
            with urlopen(req, timeout=timeout_seconds) as response:
                row["http_status"] = int(response.status)
                row["ok"] = 200 <= int(response.status) < 400
        except HTTPError as exc:
            row["http_status"] = int(exc.code)
            row["error"] = f"HTTP_{exc.code}"
        except URLError as exc:
            row["error"] = f"URL_ERROR:{exc.reason}"
        except Exception as exc:
            row["error"] = f"ROUTE_ERROR:{exc}"
        routes[route_path] = row
    return {
        "status": "PASS" if all(bool(row.get("ok")) for row in routes.values()) else "DEGRADED",
        "routes": routes,
    }


def probe_service(service: ServiceSpec) -> Dict[str, Any]:
    pid = _read_pid_file(service.pid_path)
    pid_running = _process_exists(pid) if isinstance(pid, int) else False
    owned_by_supervisor = False
    ownership_note = None

    if isinstance(pid, int):
        if pid_running:
            owned_by_supervisor = _pid_owned_by_service(pid, service)
            if not owned_by_supervisor:
                ownership_note = "PID_OWNERSHIP_MISMATCH"
        else:
            ownership_note = "PID_NOT_RUNNING"

    port_open = _is_port_listening(service.host, service.port)
    health = _health_probe(service) if port_open else {"url": service.health_url, "ok": False, "http_status": None, "error": "PORT_NOT_OPEN", "status_value": None}
    route_status = _route_probe(service) if port_open else {"status": "FAIL", "routes": {}}

    if owned_by_supervisor and pid_running and port_open and health.get("ok"):
        state = STATE_READY
    elif not pid_running and not port_open:
        state = STATE_NOT_RUNNING
    else:
        state = STATE_DEGRADED

    return {
        "name": service.name,
        "required": service.required,
        "state": state,
        "host": service.host,
        "port": service.port,
        "entrypoint": service.entrypoint,
        "health_url": service.health_url,
        "pid": pid,
        "pid_running": pid_running,
        "owned_by_supervisor": owned_by_supervisor,
        "ownership_note": ownership_note,
        "pid_file": str(service.pid_path),
        "log_file": str(service.log_path),
        "port_open": port_open,
        "health": health,
        "route_status": route_status,
    }


def summarize_overall_status(service_snapshots: List[Dict[str, Any]]) -> str:
    required = [s for s in service_snapshots if bool(s.get("required"))]
    if not required:
        return STATE_READY
    states = {str(s.get("state")) for s in required}
    if states == {STATE_READY}:
        return STATE_READY
    if states == {STATE_NOT_RUNNING}:
        return STATE_NOT_RUNNING
    return STATE_DEGRADED


def _collect_snapshots(specs: List[ServiceSpec]) -> List[Dict[str, Any]]:
    return [probe_service(spec) for spec in specs]


def _write_runtime_truth(
    manifest_path: Path,
    snapshots: List[Dict[str, Any]],
    action: str,
    start_report: Optional[Dict[str, Any]] = None,
) -> None:
    generated_utc = utc_now_iso()
    service_payload = {
        "schema_id": "runtime_service_status.v1",
        "generated_utc": generated_utc,
        "runtime_root": str(RUNTIME_ROOT),
        "manifest_path": str(manifest_path),
        "services": snapshots,
    }
    supervisor_payload = {
        "schema_id": "runtime_supervisor_state.v1",
        "generated_utc": generated_utc,
        "runtime_root": str(RUNTIME_ROOT),
        "manifest_path": str(manifest_path),
        "overall_status": summarize_overall_status(snapshots),
        "last_action": action,
        "service_count": len(snapshots),
        "required_service_count": sum(1 for s in snapshots if bool(s.get("required"))),
    }
    _write_json(SERVICE_STATUS_PATH, service_payload)
    _write_json(SUPERVISOR_STATE_PATH, supervisor_payload)
    if start_report is not None:
        _write_json(LAST_START_REPORT_PATH, start_report)


def _terminate_pid(pid: int, timeout_seconds: int) -> bool:
    if not _process_exists(pid):
        return True
    try:
        os.killpg(pid, signal.SIGTERM)
    except Exception:
        try:
            os.kill(pid, signal.SIGTERM)
        except Exception:
            return False

    deadline = time.monotonic() + max(1, timeout_seconds)
    while time.monotonic() < deadline:
        if not _process_exists(pid):
            return True
        time.sleep(0.2)

    try:
        os.killpg(pid, signal.SIGKILL)
    except Exception:
        try:
            os.kill(pid, signal.SIGKILL)
        except Exception:
            return False

    deadline = time.monotonic() + 2.0
    while time.monotonic() < deadline:
        if not _process_exists(pid):
            return True
        time.sleep(0.1)
    return not _process_exists(pid)


def _remove_pid_if_stale(service: ServiceSpec) -> None:
    pid = _read_pid_file(service.pid_path)
    if pid is None:
        return
    if not _process_exists(pid):
        service.pid_path.unlink(missing_ok=True)


def _wait_until_ready(service: ServiceSpec) -> Tuple[bool, Dict[str, Any]]:
    deadline = time.monotonic() + max(1, service.start_timeout_seconds)
    last_snapshot = probe_service(service)
    while time.monotonic() < deadline:
        snapshot = probe_service(service)
        last_snapshot = snapshot
        if snapshot["state"] == STATE_READY:
            return True, snapshot
        time.sleep(0.5)
    return False, last_snapshot


def _start_service(service: ServiceSpec) -> Dict[str, Any]:
    _remove_pid_if_stale(service)
    pre = probe_service(service)
    if pre["state"] == STATE_READY:
        return {"service": service.name, "ok": True, "action": "already_ready", "snapshot": pre}

    if pre["port_open"] and not pre["owned_by_supervisor"]:
        return {
            "service": service.name,
            "ok": False,
            "action": "start_blocked",
            "reason": "PORT_OCCUPIED_BY_UNMANAGED_PROCESS",
            "snapshot": pre,
        }

    command = [sys.executable, str(service.entrypoint_path), "--host", service.host, "--port", str(service.port)]
    env = dict(os.environ)
    env["PYTHONUNBUFFERED"] = "1"
    with service.log_path.open("ab") as log_handle:
        process = subprocess.Popen(
            command,
            cwd=str(REPO_ROOT),
            env=env,
            stdout=log_handle,
            stderr=log_handle,
            start_new_session=True,
        )
    service.pid_path.write_text(f"{process.pid}\n", encoding="utf-8")
    _log("INFO", f"START service={service.name} pid={process.pid} host={service.host} port={service.port}")

    ready, snapshot = _wait_until_ready(service)
    if ready:
        return {"service": service.name, "ok": True, "action": "started", "snapshot": snapshot}

    _log("ERROR", f"START_FAIL service={service.name} pid={process.pid} reason=HEALTH_TIMEOUT")
    _terminate_pid(process.pid, service.stop_timeout_seconds)
    service.pid_path.unlink(missing_ok=True)
    failed_snapshot = probe_service(service)
    return {
        "service": service.name,
        "ok": False,
        "action": "start_failed",
        "reason": "HEALTH_TIMEOUT",
        "snapshot": failed_snapshot,
    }


def _stop_service(service: ServiceSpec) -> Dict[str, Any]:
    snapshot = probe_service(service)
    pid = snapshot.get("pid")
    if not isinstance(pid, int):
        service.pid_path.unlink(missing_ok=True)
        return {"service": service.name, "ok": True, "action": "already_stopped", "snapshot": snapshot}

    if not snapshot.get("pid_running"):
        service.pid_path.unlink(missing_ok=True)
        return {"service": service.name, "ok": True, "action": "stale_pid_removed", "snapshot": snapshot}

    if not snapshot.get("owned_by_supervisor"):
        return {
            "service": service.name,
            "ok": False,
            "action": "stop_blocked",
            "reason": "PID_OWNERSHIP_MISMATCH",
            "snapshot": snapshot,
        }

    terminated = _terminate_pid(pid, service.stop_timeout_seconds)
    if terminated:
        _log("INFO", f"STOP service={service.name} pid={pid}")
    else:
        _log("ERROR", f"STOP_FAIL service={service.name} pid={pid}")
    service.pid_path.unlink(missing_ok=True)
    final_snapshot = probe_service(service)
    return {
        "service": service.name,
        "ok": terminated and final_snapshot["state"] == STATE_NOT_RUNNING,
        "action": "stopped" if terminated else "stop_failed",
        "snapshot": final_snapshot,
    }


def _command_status(manifest_path: Path, specs: List[ServiceSpec]) -> int:
    snapshots = _collect_snapshots(specs)
    _write_runtime_truth(manifest_path, snapshots, action="status")
    payload = {
        "ok": True,
        "generated_utc": utc_now_iso(),
        "runtime_root": str(RUNTIME_ROOT),
        "manifest_path": str(manifest_path),
        "overall_status": summarize_overall_status(snapshots),
        "services": snapshots,
    }
    print(json.dumps(payload, indent=2, sort_keys=True))
    return 0


def _command_start(manifest_path: Path, specs: List[ServiceSpec]) -> int:
    attempts: List[Dict[str, Any]] = []
    started_specs: List[ServiceSpec] = []
    required_specs = [spec for spec in specs if spec.required]

    for spec in required_specs:
        result = _start_service(spec)
        attempts.append(result)
        if result.get("ok"):
            if result.get("action") == "started":
                started_specs.append(spec)
            continue

        _log("ERROR", f"START_ABORT service={spec.name} reason={result.get('reason', 'UNKNOWN')}")
        for started in reversed(started_specs):
            _stop_service(started)

        snapshots = _collect_snapshots(specs)
        report = {
            "schema_id": "runtime_last_start_report.v1",
            "generated_utc": utc_now_iso(),
            "runtime_root": str(RUNTIME_ROOT),
            "manifest_path": str(manifest_path),
            "status": "FAILED",
            "overall_status": summarize_overall_status(snapshots),
            "attempts": attempts,
        }
        _write_runtime_truth(manifest_path, snapshots, action="start", start_report=report)
        print(json.dumps(report, indent=2, sort_keys=True))
        return 1

    snapshots = _collect_snapshots(specs)
    overall = summarize_overall_status(snapshots)
    status = "READY" if overall == STATE_READY else "FAILED"
    report = {
        "schema_id": "runtime_last_start_report.v1",
        "generated_utc": utc_now_iso(),
        "runtime_root": str(RUNTIME_ROOT),
        "manifest_path": str(manifest_path),
        "status": status,
        "overall_status": overall,
        "attempts": attempts,
    }
    _write_runtime_truth(manifest_path, snapshots, action="start", start_report=report)
    print(json.dumps(report, indent=2, sort_keys=True))
    return 0 if status == "READY" else 1


def _command_stop(manifest_path: Path, specs: List[ServiceSpec]) -> int:
    results = [_stop_service(spec) for spec in specs]
    snapshots = _collect_snapshots(specs)
    _write_runtime_truth(manifest_path, snapshots, action="stop")
    payload = {
        "ok": all(bool(item.get("ok")) for item in results),
        "generated_utc": utc_now_iso(),
        "runtime_root": str(RUNTIME_ROOT),
        "manifest_path": str(manifest_path),
        "overall_status": summarize_overall_status(snapshots),
        "results": results,
    }
    print(json.dumps(payload, indent=2, sort_keys=True))
    return 0 if payload["ok"] else 1


def _command_restart(manifest_path: Path, specs: List[ServiceSpec]) -> int:
    stop_code = _command_stop(manifest_path, specs)
    if stop_code != 0:
        return stop_code
    return _command_start(manifest_path, specs)


def _tail_log_lines(path: Path, line_count: int) -> str:
    with path.open("rb") as handle:
        lines = deque(handle, maxlen=max(1, line_count))
    return b"".join(lines).decode("utf-8", errors="replace")


def _command_logs(specs: List[ServiceSpec], line_count: int) -> int:
    targets = [SUPERVISOR_LOG_PATH] + [spec.log_path for spec in specs]
    emitted_any = False
    for target in targets:
        if not target.exists():
            continue
        emitted_any = True
        print(f"===== {target} =====")
        print(_tail_log_lines(target, line_count).rstrip())
        print("")
    if not emitted_any:
        print(json.dumps({"ok": True, "message": "No logs yet.", "log_root": str(LOG_ROOT)}, indent=2, sort_keys=True))
    return 0


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Constellation local runtime supervisor")
    parser.add_argument(
        "--manifest",
        default=str(DEFAULT_MANIFEST_PATH),
        help="Path to runtime manifest YAML.",
    )
    subparsers = parser.add_subparsers(dest="command", required=True)
    subparsers.add_parser("start")
    subparsers.add_parser("stop")
    subparsers.add_parser("restart")
    subparsers.add_parser("status")
    logs = subparsers.add_parser("logs")
    logs.add_argument("--lines", type=int, default=120)
    return parser


def main(argv: Optional[List[str]] = None) -> int:
    parser = _build_parser()
    args = parser.parse_args(argv)
    manifest_path = Path(args.manifest).resolve()
    _ensure_runtime_paths()

    try:
        specs = load_manifest(manifest_path)
    except Exception as exc:
        _log("ERROR", str(exc))
        error_payload = {
            "ok": False,
            "generated_utc": utc_now_iso(),
            "error": str(exc),
            "manifest_path": str(manifest_path),
        }
        print(json.dumps(error_payload, indent=2, sort_keys=True))
        return 2

    if args.command == "start":
        return _command_start(manifest_path, specs)
    if args.command == "stop":
        return _command_stop(manifest_path, specs)
    if args.command == "restart":
        return _command_restart(manifest_path, specs)
    if args.command == "status":
        return _command_status(manifest_path, specs)
    if args.command == "logs":
        return _command_logs(specs, line_count=max(1, int(args.lines)))
    raise SupervisorError(f"Unsupported command: {args.command}")


if __name__ == "__main__":
    raise SystemExit(main())
