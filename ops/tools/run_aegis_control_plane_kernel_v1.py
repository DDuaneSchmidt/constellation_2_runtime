#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
from pathlib import Path
import sys
from typing import Any

REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from constellation_2.aegis_truth.evidence_event_v1 import utc_now_iso
from constellation_2.aegis_truth.projection_writer_v1 import atomic_write_json
from constellation_2.aegis_truth.state_machines.alert_lifecycle_state_v1 import build_alert_lifecycle_state, write_alert_lifecycle_state
from constellation_2.aegis_truth.state_machines.kernel_state_types_v1 import FINAL_STATUS_RANK
from constellation_2.aegis_truth.state_machines.portal_surface_state_v1 import build_portal_surface_state, write_portal_surface_state
from constellation_2.aegis_truth.state_machines.projection_freshness_state_v1 import build_projection_freshness_state, write_projection_freshness_state
from constellation_2.aegis_truth.state_machines.repo_protection_state_v1 import build_repo_protection_state, write_repo_protection_state
from constellation_2.aegis_truth.state_machines.trading_readiness_state_v1 import build_trading_readiness_state, write_trading_readiness_state

SCHEMA_VERSION = "aegis_control_plane_kernel.v1"
REPORT_NAME = "aegis_control_plane_kernel_v1"
FILENAME = "control_plane_kernel.v1.json"


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--target-day", required=True)
    ap.add_argument("--truth-root", default="/home/node/constellation_runtime_data/truth")
    ap.add_argument("--environment", choices=["PAPER", "LIVE", "UNKNOWN"], default="UNKNOWN")
    ap.add_argument("--write-domain-states", action="store_true", default=True)
    args = ap.parse_args()
    path = write_control_plane_kernel(target_day=args.target_day, truth_root=args.truth_root, environment=args.environment, write_domain_states=args.write_domain_states)
    state = json.loads(path.read_text(encoding="utf-8"))
    print(json.dumps({"wrote": str(path), "final_status": state["final_status"], "primary_blocker": state["primary_blocker"]}, sort_keys=True))
    return 0


def build_control_plane_kernel(*, target_day: str, truth_root: str | Path, environment: str = "UNKNOWN") -> dict[str, Any]:
    domain_states = {
        "trading_readiness": build_trading_readiness_state(target_day=target_day, truth_root=truth_root, environment=environment),
        "portal_surface": build_portal_surface_state(target_day=target_day, truth_root=truth_root, environment=environment),
        "projection_freshness": build_projection_freshness_state(target_day=target_day, truth_root=truth_root, environment=environment),
        "repo_protection": build_repo_protection_state(target_day=target_day, truth_root=truth_root, environment=environment),
        "alert_lifecycle": build_alert_lifecycle_state(target_day=target_day, truth_root=truth_root, environment=environment),
    }
    final_status, selected, secondary = _resolve_final(domain_states)
    active_alerts = domain_states["alert_lifecycle"].get("active_alerts", [])
    evidence_ids: list[str] = []
    evidence_paths: set[str] = set()
    for state in domain_states.values():
        evidence_ids.extend(state.get("evidence_event_ids", []))
        evidence_paths.update(state.get("evidence_paths", []))
    result = {
        "schema_version": SCHEMA_VERSION,
        "target_day": target_day,
        "environment": environment,
        "final_status": final_status,
        "primary_blocker": None if final_status == "READY" else selected.get("blocker") or selected.get("status"),
        "owner": None if final_status == "READY" else selected.get("owner"),
        "severity": "INFO" if final_status == "READY" else selected.get("severity", "ERROR"),
        "truth_confidence": _confidence(final_status, domain_states),
        "domain_states": domain_states,
        "active_alerts": active_alerts,
        "secondary_conditions": secondary,
        "operator_next_action": "No operator action required." if final_status == "READY" else selected.get("next_action", "Inspect kernel domain states and repair primary blocker."),
        "evidence_event_ids": sorted(set(evidence_ids)),
        "evidence_paths": sorted(evidence_paths),
        "generated_at_utc": utc_now_iso(),
    }
    return result


def write_control_plane_kernel(*, target_day: str, truth_root: str | Path, environment: str = "UNKNOWN", write_domain_states: bool = True) -> Path:
    if write_domain_states:
        write_trading_readiness_state(target_day=target_day, truth_root=truth_root, environment=environment)
        write_portal_surface_state(target_day=target_day, truth_root=truth_root, environment=environment)
        write_projection_freshness_state(target_day=target_day, truth_root=truth_root, environment=environment)
        write_repo_protection_state(target_day=target_day, truth_root=truth_root, environment=environment)
        write_alert_lifecycle_state(target_day=target_day, truth_root=truth_root, environment=environment)
    state = build_control_plane_kernel(target_day=target_day, truth_root=truth_root, environment=environment)
    path = Path(truth_root) / "reports" / REPORT_NAME / target_day / FILENAME
    return atomic_write_json(path, state)


def _resolve_final(domain_states: dict[str, dict[str, Any]]) -> tuple[str, dict[str, Any], list[dict[str, Any]]]:
    trading = domain_states["trading_readiness"]
    portal = domain_states["portal_surface"]
    projection = domain_states["projection_freshness"]
    repo = domain_states["repo_protection"]
    alerts = domain_states["alert_lifecycle"]
    secondary: list[dict[str, Any]] = []
    if trading["status"] == "FORBIDDEN":
        return "FORBIDDEN", trading, _secondary(domain_states, skip="trading_readiness")
    if trading["status"] == "BLOCKED":
        return "BLOCKED", trading, _secondary(domain_states, skip="trading_readiness")
    if repo["status"] == "BREACH_ATTEMPT" and repo.get("severity") == "CRITICAL":
        return "BLOCKED", repo, _secondary(domain_states, skip="repo_protection")
    if trading["status"] == "UNKNOWN":
        secondary.extend(_conditions([portal, projection, repo, alerts]))
        return "UNKNOWN", trading, secondary
    if portal["status"] == "DOWN":
        return "DEGRADED", portal, _secondary(domain_states, skip="portal_surface")
    if projection["status"] in {"STALE", "FAILED"}:
        return "DEGRADED", projection, _secondary(domain_states, skip="projection_freshness")
    if repo["status"] in {"UNLOCKED", "MUTATION_OBSERVED", "UNKNOWN"}:
        final = "UNKNOWN" if repo["status"] == "UNKNOWN" else "DEGRADED"
        return final, repo, _secondary(domain_states, skip="repo_protection")
    active_critical = [alert for alert in alerts.get("active_alerts", []) if alert.get("severity") == "CRITICAL"]
    if active_critical:
        selected = {"status": "BLOCKED", "blocker": active_critical[0].get("message") or active_critical[0].get("title"), "owner": "alert_lifecycle", "severity": "CRITICAL", "next_action": "Resolve active critical alert."}
        return "BLOCKED", selected, _secondary(domain_states)
    if alerts.get("active_alerts"):
        selected = {"status": "DEGRADED", "blocker": alerts["active_alerts"][0].get("message") or alerts["active_alerts"][0].get("title"), "owner": "alert_lifecycle", "severity": alerts["active_alerts"][0].get("severity", "ERROR"), "next_action": "Resolve active alerts."}
        return "DEGRADED", selected, _secondary(domain_states)
    if all([trading["status"] == "READY", portal["status"] in {"OK", "RECOVERED"}, projection["status"] == "FRESH", repo["status"] == "PROTECTED"]):
        return "READY", {"owner": None, "severity": "INFO", "next_action": "No operator action required."}, []
    selected = max(domain_states.values(), key=lambda state: FINAL_STATUS_RANK.get(_map_domain_status(state["status"]), 0))
    return _map_domain_status(selected["status"]), selected, _secondary(domain_states)


def _map_domain_status(status: str) -> str:
    if status in {"FORBIDDEN"}:
        return "FORBIDDEN"
    if status in {"BLOCKED", "BREACH_ATTEMPT"}:
        return "BLOCKED"
    if status in {"UNKNOWN"}:
        return "UNKNOWN"
    if status in {"DOWN", "DEGRADED", "STALE", "FAILED", "UNLOCKED", "MUTATION_OBSERVED", "OPEN"}:
        return "DEGRADED"
    return "READY"


def _secondary(domain_states: dict[str, dict[str, Any]], skip: str | None = None) -> list[dict[str, Any]]:
    return _conditions([state | {"domain": name} for name, state in domain_states.items() if name != skip])


def _conditions(states: list[dict[str, Any]]) -> list[dict[str, Any]]:
    out = []
    for state in states:
        if state.get("status") in {"READY", "OK", "FRESH", "PROTECTED", "CLOSED"}:
            continue
        out.append({"domain": state.get("domain") or state.get("owner"), "status": state.get("status"), "severity": state.get("severity"), "blocker": state.get("blocker")})
    return out


def _confidence(final_status: str, domain_states: dict[str, dict[str, Any]]) -> str:
    if final_status == "READY":
        return "HIGH"
    if any(state.get("status") == "UNKNOWN" for state in domain_states.values()):
        return "LOW"
    if final_status in {"BLOCKED", "FORBIDDEN", "DEGRADED"}:
        return "MEDIUM"
    return "LOW"


if __name__ == "__main__":
    raise SystemExit(main())
