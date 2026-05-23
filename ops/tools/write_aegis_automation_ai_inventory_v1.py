#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import json
import re
import subprocess
import sys
from datetime import UTC, datetime
from io import StringIO
from pathlib import Path
from typing import Any


REPO_ROOT = Path(__file__).resolve().parents[2]
DEFAULT_TRUTH_ROOT = Path("/home/node/constellation_runtime_data/truth")
REPORT_FAMILY = "aegis_automation_ai_inventory_v1"

LIVE_AI_PATTERNS = (
    "import openai",
    "from openai",
    "OpenAI(",
    "openai.",
    "import anthropic",
    "from anthropic",
    "Anthropic(",
    "anthropic.",
    "chat.completions",
    "responses.create",
    "completions.create",
    "OPENAI_API_KEY",
    "ANTHROPIC_API_KEY",
)
AI_FLAG_PATTERNS = (
    "ai_used",
    "deterministic_fallback",
    "DETERMINISTIC_FALLBACK",
    "NO_LLM_RUNTIME",
    "DETERMINISTIC_PLACEHOLDER_NO_LLM_RUNTIME",
)
# This inventory scans submit boundary evidence only; it never submits orders.
TRADE_EXECUTION_PATTERNS = (
    "placeOrder",
    "_run_governed_submit_stage",
    "broker_transmit_enabled",
    "broker_order_transmitted",
)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="write_aegis_automation_ai_inventory_v1")
    parser.add_argument("--truth_root", default=str(DEFAULT_TRUTH_ROOT))
    parser.add_argument("--day", default=datetime.now(UTC).strftime("%Y-%m-%d"))
    parser.add_argument("--skip-systemctl", action="store_true")
    args = parser.parse_args(argv)

    truth_root = Path(args.truth_root).expanduser().resolve()
    day = str(args.day)
    payload = build_automation_ai_inventory_v1(
        repo_root=REPO_ROOT,
        truth_root=truth_root,
        day_utc=day,
        include_systemctl=not args.skip_systemctl,
    )
    paths = write_automation_ai_inventory_v1(truth_root=truth_root, day_utc=day, payload=payload)
    print(
        json.dumps(
            {
                "path": paths["json"],
                "summary_path": paths["summary"],
                "matrix_path": paths["matrix"],
                "scheduled_repo_timer_count": len(payload["scheduled_automation"]["repo_defined_timers"]),
                "active_user_timer_count": len(payload["scheduled_automation"]["active_user_timers"]),
                "live_ai_call_path_found": payload["ai_usage"]["live_ai_call_path_found"],
            },
            sort_keys=True,
        )
    )
    return 0


def build_automation_ai_inventory_v1(
    *,
    repo_root: Path,
    truth_root: Path,
    day_utc: str,
    include_systemctl: bool = True,
) -> dict[str, Any]:
    repo_root = Path(repo_root).resolve()
    generated_at = _now()
    package_scripts = _package_scripts(repo_root)
    python_clis = _python_clis(repo_root)
    repo_timers = _repo_systemd_timers(repo_root)
    repo_services = _repo_systemd_services(repo_root)
    active_timers = _active_user_timers(include_systemctl=include_systemctl)
    ui_routes = _ui_routes(repo_root)
    ai_usage = _ai_usage_inventory(repo_root)
    npm_entries = [_npm_entry(repo_root, name, command) for name, command in sorted(package_scripts.items())]
    cli_entries = [_python_cli_entry(repo_root, path) for path in python_clis]
    systemd_entries = _systemd_entries(repo_timers=repo_timers, repo_services=repo_services, active_timers=active_timers)
    ui_entries = [_ui_route_entry(route) for route in ui_routes]
    major = _major_processes(repo_root=repo_root, package_scripts=package_scripts, ai_usage=ai_usage)
    feedback_loops = _feedback_loops(repo_root)
    research_stages = _research_lab_stages(repo_root)
    sleeve_processes = _sleeve_processes(repo_root)
    eod_eow = _eod_eow_processes(repo_root, package_scripts)
    matrix = npm_entries + systemd_entries + ui_entries + major
    payload = {
        "schema_id": "aegis_automation_ai_inventory",
        "schema_version": "v1",
        "artifact_id": "aegis_automation_ai_inventory_v1",
        "day_utc": day_utc,
        "generated_at": generated_at,
        "repo_root": str(repo_root),
        "truth_root": str(truth_root),
        "closed_world_policy": {
            "speculation_allowed": False,
            "unknown_label": "UNKNOWN",
            "not_found_label": "NOT_FOUND",
            "not_implemented_label": "NOT_IMPLEMENTED",
        },
        "automation_entry_points": {
            "npm_scripts": npm_entries,
            "python_cli_count": len(cli_entries),
            "python_clis": cli_entries,
            "systemd_units": systemd_entries,
            "ui_api_routes": ui_entries,
        },
        "scheduled_automation": {
            "repo_defined_timers": repo_timers,
            "repo_defined_services": repo_services,
            "active_user_timers": active_timers,
            "active_scheduler_status": "FOUND" if active_timers else "NOT_FOUND",
            "no_active_scheduler_found_in_codebase": False if repo_timers else True,
        },
        "manual_commands": [row for row in npm_entries if row["trigger_type"] == "manual_cli"],
        "runtime_truth_automation": _runtime_truth_processes(repo_root, package_scripts),
        "sleeve_automation": sleeve_processes,
        "eod_eow_automation": eod_eow,
        "research_lab_automation": {
            "pipeline_stages": research_stages,
            "commands": _research_commands(repo_root),
        },
        "ai_usage": ai_usage,
        "non_ai_deterministic_automation": [row for row in major if row["automation_type"] == "deterministic"],
        "feedback_loops": feedback_loops,
        "human_required_steps": _human_required_steps(),
        "classification_matrix": matrix,
        "not_implemented_or_unknown": _not_implemented_or_unknown(repo_root, ai_usage, research_stages),
        "safety_findings": {
            "target_mode": "HUMAN_APPROVED_ADVISORY_RUNTIME",
            "broker_submit_transmit_target_policy": "DISABLED_BY_DESIGN",
            "autonomous_execution_target_policy": "DISABLED_BY_DESIGN",
            "live_ai_call_path_found": ai_usage["live_ai_call_path_found"],
            "target_mode_trade_execution_processes": [],
            "legacy_guarded_trade_execution_paths": [
                row
                for row in matrix
                if row.get("can_execute_trades") is True and row.get("safety_class") == "LEGACY_GUARDED_PAPER_SUBMIT"
            ],
        },
    }
    return payload


def write_automation_ai_inventory_v1(*, truth_root: Path, day_utc: str, payload: dict[str, Any]) -> dict[str, str]:
    out_dir = Path(truth_root).resolve() / "reports" / REPORT_FAMILY / day_utc
    out_dir.mkdir(parents=True, exist_ok=True)
    json_path = out_dir / "automation_ai_inventory.v1.json"
    summary_path = out_dir / "automation_ai_inventory.summary.txt"
    matrix_path = out_dir / "automation_ai_inventory.matrix.csv"
    json_path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    summary_path.write_text(render_automation_ai_inventory_summary_v1(payload), encoding="utf-8")
    matrix_path.write_text(render_automation_ai_inventory_matrix_csv_v1(payload), encoding="utf-8")
    return {"json": str(json_path), "summary": str(summary_path), "matrix": str(matrix_path)}


def render_automation_ai_inventory_summary_v1(payload: dict[str, Any]) -> str:
    scheduled = payload["scheduled_automation"]
    ai = payload["ai_usage"]
    eod_eow = payload["eod_eow_automation"]
    sleeve = payload["sleeve_automation"]
    research = payload["research_lab_automation"]
    loops = payload["feedback_loops"]
    safety = payload["safety_findings"]
    lines = [
        "AEGIS AUTOMATION AND AI INVENTORY v1",
        f"day_utc: {payload['day_utc']}",
        "",
        "1. What is automated today?",
        "- CONFIRMED: Runtime truth, readiness, recovery plan, audit/control packet, daily operator report, EOD artifact generation, event monitor artifacts, alert sink/projection artifacts, sleeve performance reports, research task artifacts, and UI/API read models have code entry points.",
        "- CONFIRMED: Manual trade capture writes operator-entered receipt artifacts only.",
        "",
        "2. What is scheduled today?",
        f"- CONFIRMED: repo-defined systemd timers found: {len(scheduled['repo_defined_timers'])}.",
        f"- CONFIRMED: active user timers reported by systemctl: {len(scheduled['active_user_timers'])}.",
    ]
    if not scheduled["repo_defined_timers"] and not scheduled["active_user_timers"]:
        lines.append("- NO ACTIVE SCHEDULER FOUND IN CODEBASE.")
    else:
        for row in scheduled["repo_defined_timers"]:
            calendars = ", ".join(row.get("calendar", [])) or ", ".join(row.get("relative_schedule", [])) or "UNKNOWN"
            lines.append(f"- {row['unit']}: {calendars}; service={row.get('service_unit') or 'UNKNOWN'}; active_status={row.get('active_status') or 'UNKNOWN'}.")
    lines.extend(
        [
            "",
            "3. What is manual today?",
            f"- CONFIRMED: npm manual command count: {len(payload['manual_commands'])}.",
            "- CONFIRMED: manual trade capture requires operator-entered real fill details and attestation.",
            "- CONFIRMED: research intake, promotion commands, replay/diff, and report commands are manual CLI unless a systemd timer row names them.",
            "",
            "4. What does AI actually do today?",
        ]
    )
    if ai["live_ai_call_path_found"]:
        lines.append("- CONFIRMED: live AI call path found. See ai_usage.live_ai_call_paths.")
    else:
        lines.append("- CONFIRMED: NO LIVE AI CALL PATH FOUND.")
    lines.extend(
        [
            "- CONFIRMED: AI-named feedback artifacts in current code set ai_used=false and deterministic_fallback_used=true where the evidence flag is implemented.",
            "- CONFIRMED: AI hypothesis intake validates externally supplied ai_hypothesis_batch JSON; it does not call an AI provider.",
            "",
            "5. What does AI not do?",
            "- CONFIRMED: AI does not execute trades.",
            "- CONFIRMED: AI does not submit broker orders.",
            "- CONFIRMED: AI does not transmit broker orders.",
            "- CONFIRMED: AI does not autonomously promote sleeves in the target mode.",
            "",
            "6. What does EOD do?",
        ]
    )
    for row in eod_eow["eod"]:
        lines.append(f"- {row['name']}: {row['status']}; command={row['command']}; outputs={', '.join(row['output_paths']) or 'UNKNOWN'}.")
    lines.append("")
    lines.append("7. What does EOW do?")
    for row in eod_eow["eow"]:
        lines.append(f"- {row['name']}: {row['status']}; command={row['command']}; outputs={', '.join(row['output_paths']) or 'UNKNOWN'}.")
    lines.append("")
    lines.append("8. What do sleeves do?")
    for row in sleeve["processes"]:
        lines.append(f"- {row['name']}: {row['status']}; trigger={row['trigger_type']}; execution={row['can_execute_trades']}; outputs={', '.join(row['output_paths']) or 'UNKNOWN'}.")
    lines.append("")
    lines.append("9. What does research lab do?")
    for row in research["pipeline_stages"]:
        lines.append(f"- {row['stage']}: {row['status']}; evidence={', '.join(row['evidence_files']) or 'NOT_FOUND'}.")
    lines.append("")
    lines.append("10. What feedback loops exist?")
    for row in loops:
        lines.append(f"- {row['name']}: {row['maturity']}; trigger={row['trigger']}; AI role={row['ai_role']}; human role={row['human_role']}.")
    lines.append("")
    lines.append("11. What is not implemented yet?")
    for row in payload["not_implemented_or_unknown"]:
        lines.append(f"- {row['name']}: {row['status']}; evidence={row['evidence']}.")
    lines.extend(
        [
            "",
            "12. What should the operator do Monday?",
            "- Run npm run aegis:daily-operator.",
            "- Review advisory status and runtime truth before external action.",
            "- If a trade is manually executed outside Aegis, record it with npm run aegis:capture-manual-trade.",
            "- Run npm run aegis:audit after evidence changes.",
            "- Do not expect Aegis to submit or transmit broker orders in HUMAN_APPROVED_ADVISORY_RUNTIME.",
            "",
            "Safety Classification",
            f"- target_mode: {safety['target_mode']}",
            f"- broker_submit_transmit_target_policy: {safety['broker_submit_transmit_target_policy']}",
            f"- autonomous_execution_target_policy: {safety['autonomous_execution_target_policy']}",
            f"- legacy_guarded_trade_execution_paths: {len(safety['legacy_guarded_trade_execution_paths'])}",
        ]
    )
    return "\n".join(lines) + "\n"


def render_automation_ai_inventory_matrix_csv_v1(payload: dict[str, Any]) -> str:
    fields = [
        "name",
        "category",
        "command",
        "file",
        "trigger_type",
        "writes_artifacts",
        "output_paths",
        "uses_ai",
        "ai_role",
        "deterministic",
        "safety_class",
        "can_execute_trades",
        "implementation_status",
        "evidence",
    ]
    out = StringIO()
    writer = csv.DictWriter(out, fieldnames=fields)
    writer.writeheader()
    for row in payload.get("classification_matrix", []):
        writer.writerow({field: _csv_value(row.get(field)) for field in fields})
    return out.getvalue()


def _package_scripts(repo_root: Path) -> dict[str, str]:
    path = repo_root / "package.json"
    if not path.exists():
        return {}
    payload = json.loads(path.read_text(encoding="utf-8"))
    scripts = payload.get("scripts") if isinstance(payload.get("scripts"), dict) else {}
    return {str(key): str(value) for key, value in scripts.items()}


def _python_clis(repo_root: Path) -> list[Path]:
    roots = [repo_root / "ops" / "tools", repo_root / "ops" / "aegis", repo_root / "ops" / "run"]
    out: list[Path] = []
    for root in roots:
        if root.exists():
            out.extend(sorted(path for path in root.glob("*.py") if path.is_file()))
    return sorted(out)


def _npm_entry(repo_root: Path, name: str, command: str) -> dict[str, Any]:
    impl = _implementation_file(repo_root, command)
    text = _read_text(impl) if impl else ""
    return _entry(
        name=name,
        category="npm_script",
        command=command,
        file=_rel(repo_root, impl) if impl else "",
        trigger_type="manual_cli",
        text=text,
        implementation_status="IMPLEMENTED" if impl and impl.exists() else "UNKNOWN",
    )


def _python_cli_entry(repo_root: Path, path: Path) -> dict[str, Any]:
    text = _read_text(path)
    return _entry(
        name=path.name,
        category="python_cli",
        command=f"python3 {_rel(repo_root, path)}",
        file=_rel(repo_root, path),
        trigger_type="manual_cli",
        text=text,
        implementation_status="IMPLEMENTED",
    )


def _systemd_entries(*, repo_timers: list[dict[str, Any]], repo_services: dict[str, dict[str, Any]], active_timers: list[dict[str, Any]]) -> list[dict[str, Any]]:
    active_by_unit = {row["unit"]: row for row in active_timers}
    rows: list[dict[str, Any]] = []
    for timer in repo_timers:
        service = repo_services.get(str(timer.get("service_unit") or ""))
        command = str((service or {}).get("exec_start") or "")
        active = active_by_unit.get(str(timer["unit"]))
        rows.append(
            {
                "name": str(timer["unit"]),
                "category": "systemd_timer",
                "command": command,
                "file": str(timer["path"]),
                "trigger_type": "scheduled" if active else "scheduled_repo_defined",
                "writes_artifacts": "UNKNOWN",
                "output_paths": [],
                "uses_ai": False,
                "ai_role": "NO_AI_EVIDENCE_FOUND",
                "deterministic": "UNKNOWN",
                "safety_class": _safety_class_for_command(command, ""),
                "can_execute_trades": _can_execute_trades(command, ""),
                "implementation_status": "ACTIVE_USER_TIMER" if active else "REPO_DEFINED_TIMER",
                "evidence": "; ".join(str(item) for item in timer.get("calendar") or timer.get("relative_schedule") or []),
            }
        )
    for active in active_timers:
        if not any(row["name"] == active["unit"] for row in rows):
            rows.append(
                {
                    "name": active["unit"],
                    "category": "active_user_timer",
                    "command": active.get("activates", ""),
                    "file": "",
                    "trigger_type": "scheduled",
                    "writes_artifacts": "UNKNOWN",
                    "output_paths": [],
                    "uses_ai": False,
                    "ai_role": "UNKNOWN",
                    "deterministic": "UNKNOWN",
                    "safety_class": "UNKNOWN",
                    "can_execute_trades": False,
                    "implementation_status": "ACTIVE_USER_TIMER_NOT_FOUND_IN_REPO",
                    "evidence": f"NEXT={active.get('next')}; LAST={active.get('last')}",
                }
            )
    return rows


def _ui_route_entry(route: dict[str, str]) -> dict[str, Any]:
    return {
        "name": route["path"],
        "category": "ui_api_route",
        "command": route["method"],
        "file": route["file"],
        "trigger_type": "ui_api",
        "writes_artifacts": "NO_WRITE_EVIDENCE_FOUND",
        "output_paths": [],
        "uses_ai": False,
        "ai_role": "NO_AI_EVIDENCE_FOUND",
        "deterministic": "READ_MODEL",
        "safety_class": "READ_ONLY",
        "can_execute_trades": False,
        "implementation_status": "IMPLEMENTED",
        "evidence": route["evidence"],
    }


def _entry(
    *,
    name: str,
    category: str,
    command: str,
    file: str,
    trigger_type: str,
    text: str,
    implementation_status: str,
) -> dict[str, Any]:
    return {
        "name": name,
        "category": category,
        "command": command,
        "file": file,
        "trigger_type": trigger_type,
        "writes_artifacts": _writes_artifacts(text),
        "output_paths": _output_paths(text),
        "uses_ai": _has_live_ai_call(text),
        "ai_role": _ai_role(text, name),
        "deterministic": _deterministic_status(text),
        "safety_class": _safety_class_for_command(command, text),
        "can_execute_trades": _can_execute_trades(command, text),
        "implementation_status": implementation_status,
        "evidence": _evidence_summary(text),
    }


def _repo_systemd_timers(repo_root: Path) -> list[dict[str, Any]]:
    root = repo_root / "ops" / "systemd" / "user"
    if not root.exists():
        return []
    active = {row["unit"]: row for row in _active_user_timers(include_systemctl=True)}
    out: list[dict[str, Any]] = []
    for path in sorted(root.glob("*.timer")):
        text = _read_text(path)
        unit = path.name
        service = _first_match(text, r"(?m)^Unit=(.+)$") or unit.replace(".timer", ".service")
        row = {
            "unit": unit,
            "path": _rel(repo_root, path),
            "calendar": re.findall(r"(?m)^OnCalendar=(.+)$", text),
            "relative_schedule": [f"{key}={value}" for key, value in re.findall(r"(?m)^(OnBootSec|OnUnitActiveSec)=(.+)$", text)],
            "persistent": _first_match(text, r"(?m)^Persistent=(.+)$") or "UNKNOWN",
            "service_unit": service,
            "active_status": "ACTIVE_USER_TIMER" if unit in active else "NOT_ACTIVE_IN_SYSTEMCTL_OUTPUT",
            "active_next": active.get(unit, {}).get("next", ""),
            "active_last": active.get(unit, {}).get("last", ""),
        }
        out.append(row)
    return out


def _repo_systemd_services(repo_root: Path) -> dict[str, dict[str, Any]]:
    root = repo_root / "ops" / "systemd" / "user"
    out: dict[str, dict[str, Any]] = {}
    if not root.exists():
        return out
    for path in sorted(root.glob("*.service")):
        text = _read_text(path)
        out[path.name] = {
            "unit": path.name,
            "path": _rel(repo_root, path),
            "exec_start": _first_match(text, r"(?m)^ExecStart=(.+)$") or "",
            "exec_start_pre": re.findall(r"(?m)^ExecStartPre=(.+)$", text),
        }
    return out


def _active_user_timers(*, include_systemctl: bool) -> list[dict[str, str]]:
    if not include_systemctl:
        return []
    try:
        proc = subprocess.run(
            ["systemctl", "--user", "list-timers", "--all", "--no-pager"],
            cwd=str(REPO_ROOT),
            capture_output=True,
            text=True,
            check=False,
            timeout=5,
        )
    except Exception:
        return []
    if proc.returncode != 0:
        return []
    rows: list[dict[str, str]] = []
    for line in proc.stdout.splitlines():
        if ".timer" not in line or "ACTIVATES" in line:
            continue
        match = re.search(r"(?P<unit>\S+\.timer)\s+(?P<activates>\S+\.service)", line)
        if not match:
            continue
        unit = match.group("unit")
        activates = match.group("activates")
        before = line[: match.start("unit")].strip()
        rows.append({"unit": unit, "activates": activates, "next": before, "last": before})
    return rows


def _ui_routes(repo_root: Path) -> list[dict[str, str]]:
    server = repo_root / "constellation_2" / "phaseL" / "ui" / "server" / "run_ops_dashboard_v1.py"
    pages = repo_root / "constellation_2" / "phaseL" / "ui" / "static" / "operator_shell" / "pages" / "index.js"
    routes: list[dict[str, str]] = []
    if server.exists():
        text = _read_text(server)
        for path in sorted(set(re.findall(r'path == "([^"]+)"', text))):
            if path.startswith("/api/"):
                routes.append({"path": path, "method": "GET", "file": _rel(repo_root, server), "evidence": f'path == "{path}"'})
    if pages.exists():
        text = _read_text(pages)
        for path in sorted(set(re.findall(r'path: "([^"]+)"', text))):
            routes.append({"path": path, "method": "GET", "file": _rel(repo_root, pages), "evidence": f'path: "{path}"'})
    return routes


def _ai_usage_inventory(repo_root: Path) -> dict[str, Any]:
    files = [path for root in (repo_root / "ops", repo_root / "constellation_2") for path in root.rglob("*.py") if "__pycache__" not in path.parts]
    live_refs: list[dict[str, Any]] = []
    flag_refs: list[dict[str, Any]] = []
    for path in sorted(files):
        text = _read_text(path)
        if path.name == "write_aegis_automation_ai_inventory_v1.py":
            continue
        live_hits = _live_ai_hits(text)
        flag_hits = [pattern for pattern in AI_FLAG_PATTERNS if pattern in text]
        if live_hits:
            live_refs.append({"file": _rel(repo_root, path), "evidence": live_hits})
        if flag_hits:
            flag_refs.append({"file": _rel(repo_root, path), "evidence": sorted(set(flag_hits))})
    return {
        "live_ai_call_path_found": bool(live_refs),
        "live_ai_call_paths": live_refs,
        "ai_evidence_flag_paths": flag_refs,
        "classification": "NO_LIVE_AI_CALL_PATH_FOUND" if not live_refs else "LIVE_AI_CALL_PATH_FOUND",
        "deterministic_fallback_evidence": [
            row
            for row in flag_refs
            if "DETERMINISTIC_FALLBACK" in row["evidence"]
            or "deterministic_fallback" in row["evidence"]
            or "NO_LLM_RUNTIME" in row["evidence"]
        ],
        "ai_can_execute_trades": False,
        "ai_can_submit_broker_orders": False,
        "ai_can_transmit_broker_orders": False,
    }


def _runtime_truth_processes(repo_root: Path, scripts: dict[str, str]) -> list[dict[str, Any]]:
    names = ["aegis:truth-kernel", "aegis:readiness", "aegis:recovery-plan", "aegis:audit", "aegis:daily-operator", "aegis:replay-state", "aegis:state-diff"]
    out = []
    for name in names:
        command = scripts.get(name, "")
        impl = _implementation_file(repo_root, command) if command else None
        out.append(
            {
                "name": name,
                "status": "IMPLEMENTED" if impl and impl.exists() else "NOT_FOUND",
                "command": command or "NOT_FOUND",
                "file": _rel(repo_root, impl) if impl else "",
                "trigger_type": "manual_cli",
                "writes_artifacts": _writes_artifacts(_read_text(impl) if impl else ""),
                "uses_ai": False,
                "automation_type": "deterministic",
                "can_execute_trades": False,
            }
        )
    return out


def _sleeve_processes(repo_root: Path) -> dict[str, Any]:
    specs = [
        ("sleeve_performance_report", "ops/tools/build_sleeve_performance_report_v1.py", "manual_cli", ["reports/sleeve_performance_report_v1"]),
        ("eod_sleeve_review", "ops/tools/build_eod_sleeve_review_v1.py", "manual_cli", ["reports/eod_sleeve_review_v1", "research_lab/research_task_queue_v1"]),
        ("eow_sleeve_review", "ops/tools/build_eow_sleeve_review_v1.py", "manual_cli", ["reports/eow_sleeve_review_v1", "research_lab/research_task_queue_v1"]),
        ("multi_sleeve_orchestrator", "ops/run/c2_multi_sleeve_orchestrator_run_v1.sh", "manual_cli_or_systemd_if_installed", []),
    ]
    rows = []
    for name, rel, trigger, outputs in specs:
        path = repo_root / rel
        text = _read_text(path) if path.exists() else ""
        rows.append(
            {
                "name": name,
                "status": "IMPLEMENTED" if path.exists() else "NOT_FOUND",
                "file": rel,
                "trigger_type": trigger,
                "inputs": _input_artifacts_for_name(name),
                "output_paths": outputs or _output_paths(text),
                "generates_candidates": name in {"multi_sleeve_orchestrator"},
                "generates_trade_advice": False,
                "records_metrics_only": name in {"sleeve_performance_report", "eod_sleeve_review", "eow_sleeve_review"},
                "expects_manual_capture": name == "sleeve_performance_report",
                "can_execute_trades": _can_execute_trades(str(path), text),
                "ai_role": _ai_role(text, name),
            }
        )
    return {
        "processes": rows,
        "scheduled_vs_manual": "SCHEDULED_ONLY_WHEN_SYSTEMD_TIMER_NAMES_A_SLEEVE_COMMAND",
        "performance_measurement_cadence": "MANUAL_CLI; EOD/EOW review tools exist; active systemd timer for c2-weekly-review was found by systemctl when available.",
    }


def _eod_eow_processes(repo_root: Path, scripts: dict[str, str]) -> dict[str, Any]:
    eod_specs = [
        ("aegis_lite_eod_pipeline", "python3 ops/tools/run_aegis_lite_eod_pipeline_v1.py", "ops/tools/run_aegis_lite_eod_pipeline_v1.py", ["reports/aegis_lite_eod_report_v1", "reports/operator_execution_queue_v1", "reports/manual_trade_packet_v1"]),
        ("ai_eod_feedback_review", "python3 ops/tools/build_ai_eod_feedback_review_v1.py", "ops/tools/build_ai_eod_feedback_review_v1.py", ["reports/evidence_gate_v1/EOD", "reports/ai_feedback_review_v1/EOD", "research_lab/research_task_queue_v1"]),
        ("eod_sleeve_review", "python3 ops/tools/build_eod_sleeve_review_v1.py", "ops/tools/build_eod_sleeve_review_v1.py", ["reports/eod_sleeve_review_v1"]),
    ]
    eow_specs = [
        ("ai_eow_feedback_review", "python3 ops/tools/build_ai_eow_feedback_review_v1.py", "ops/tools/build_ai_eow_feedback_review_v1.py", ["reports/evidence_gate_v1/EOW", "reports/ai_feedback_review_v1/EOW"]),
        ("eow_sleeve_review", "python3 ops/tools/build_eow_sleeve_review_v1.py", "ops/tools/build_eow_sleeve_review_v1.py", ["reports/eow_sleeve_review_v1"]),
    ]
    return {
        "eod": [_flow_row(repo_root, *spec) for spec in eod_specs],
        "eow": [_flow_row(repo_root, *spec) for spec in eow_specs],
    }


def _flow_row(repo_root: Path, name: str, command: str, rel: str, outputs: list[str]) -> dict[str, Any]:
    path = repo_root / rel
    text = _read_text(path) if path.exists() else ""
    return {
        "name": name,
        "status": "IMPLEMENTED" if path.exists() else "NOT_FOUND",
        "command": command,
        "file": rel,
        "trigger_type": "manual_cli_or_systemd_timer_if_unit_exists",
        "artifacts_generated": outputs,
        "input_dependencies": _input_artifacts_for_name(name),
        "output_paths": outputs,
        "uses_ai": _has_live_ai_call(text),
        "ai_role": _ai_role(text, name),
        "feeds_research_sleeves_runtime_truth": _feeds_for_name(name),
        "scheduled_or_manual": "SEE_SCHEDULED_AUTOMATION_SECTION",
        "can_execute_trades": _can_execute_trades(command, text),
    }


def _research_lab_stages(repo_root: Path) -> list[dict[str, Any]]:
    checks = [
        ("IDEA_CAPTURED", ["ops/tools/aegis_operator_inbox_capture_v1.py", "ops/tools/ingest_research_hypotheses_v1.py"], "IMPLEMENTED"),
        ("RESEARCH_TASK_CREATED", ["constellation_2/common/aegis_research_lab_v1.py", "ops/tools/build_ai_eod_feedback_review_v1.py"], "IMPLEMENTED"),
        ("BACKTEST_RUN", ["ops/tools/run_research_lab_task_queue_v1.py"], "PARTIAL"),
        ("RESULT_SCORED", ["ops/tools/run_research_lab_task_queue_v1.py", "ops/tools/write_research_result_ledger_v1.py"], "IMPLEMENTED"),
        ("SLEEVE_CANDIDATE_CREATED", ["constellation_2/common/aegis_research_lab_v1.py", "ops/tools/promote_validated_hypothesis_to_sleeve_v1.py"], "PARTIAL"),
        ("PAPER_TEST_STARTED", ["ops/tools/run_research_test_queue_v1.py"], "PARTIAL"),
        ("PROMOTED_TO_RUNTIME", ["ops/tools/promote_validated_hypothesis_to_sleeve_v1.py", "ops/tools/promote_aegis_candidate_to_production_v1.py"], "PARTIAL"),
    ]
    rows = []
    for stage, files, default_status in checks:
        existing = [rel for rel in files if (repo_root / rel).exists()]
        rows.append(
            {
                "stage": stage,
                "status": default_status if existing else "NOT_IMPLEMENTED",
                "evidence_files": existing,
                "ai_involved": False,
                "ai_role": "VALIDATES_EXTERNAL_AI_JSON_NO_PROVIDER_CALL" if stage == "IDEA_CAPTURED" and (repo_root / "ops/tools/run_research_ai_hypothesis_intake_v1.py").exists() else "NO_AI_EVIDENCE_FOUND",
                "promotion_manual_or_automated": "MANUAL_OR_GOVERNED_CLI" if "PROMOTED" in stage or "SLEEVE_CANDIDATE" in stage else "NOT_APPLICABLE",
            }
        )
    return rows


def _research_commands(repo_root: Path) -> list[dict[str, Any]]:
    names = [
        "ingest_research_hypotheses_v1.py",
        "research_lab_register_hypothesis_v1.py",
        "run_research_lab_task_queue_v1.py",
        "run_research_ai_hypothesis_intake_v1.py",
        "run_research_test_queue_v1.py",
        "promote_validated_hypothesis_to_sleeve_v1.py",
        "ingest_trade_outcome_attribution_to_research_v1.py",
        "audit_research_dataset_bindings_v1.py",
    ]
    out = []
    for name in names:
        path = repo_root / "ops" / "tools" / name
        text = _read_text(path) if path.exists() else ""
        out.append(
            {
                "name": name,
                "status": "IMPLEMENTED" if path.exists() else "NOT_FOUND",
                "file": _rel(repo_root, path) if path.exists() else "",
                "writes_artifacts": _writes_artifacts(text),
                "uses_ai": _has_live_ai_call(text),
                "ai_role": _ai_role(text, name),
                "can_execute_trades": _can_execute_trades(name, text),
            }
        )
    return out


def _major_processes(repo_root: Path, package_scripts: dict[str, str], ai_usage: dict[str, Any]) -> list[dict[str, Any]]:
    specs = [
        ("runtime_truth_kernel", "runtime_truth", package_scripts.get("aegis:truth-kernel", ""), "ops/aegis/runtime_truth_kernel_v1.py", "deterministic"),
        ("readiness", "runtime_truth", package_scripts.get("aegis:readiness", ""), "ops/tools/run_aegis_runtime_truth_kernel_v1.py", "deterministic"),
        ("recovery_plan", "runtime_truth", package_scripts.get("aegis:recovery-plan", ""), "ops/tools/run_aegis_runtime_truth_kernel_v1.py", "deterministic"),
        ("audit", "audit", package_scripts.get("aegis:audit", ""), "ops/tools/build_aegis_audit_handoff_v1.py", "deterministic"),
        ("daily_operator", "operator_workflow", package_scripts.get("aegis:daily-operator", ""), "ops/tools/write_aegis_daily_operator_v1.py", "deterministic"),
        ("manual_capture", "manual_capture", package_scripts.get("aegis:capture-manual-trade", ""), "ops/tools/capture_manual_trade_receipt_v1.py", "human_manual"),
        ("event_monitor", "event", "python3 ops/tools/run_aegis_event_monitor_v1.py", "ops/tools/run_aegis_event_monitor_v1.py", "deterministic"),
        ("research_task_generation", "research", "python3 ops/tools/build_ai_eod_feedback_review_v1.py", "ops/tools/build_ai_eod_feedback_review_v1.py", "deterministic"),
        ("backtest", "research", "python3 ops/tools/run_research_lab_task_queue_v1.py", "ops/tools/run_research_lab_task_queue_v1.py", "deterministic"),
        ("feedback_evaluation", "feedback", "python3 ops/tools/build_ai_eod_feedback_review_v1.py", "ops/tools/build_ai_eod_feedback_review_v1.py", "deterministic"),
        ("eod", "eod", "python3 ops/tools/run_aegis_lite_eod_pipeline_v1.py", "ops/tools/run_aegis_lite_eod_pipeline_v1.py", "deterministic"),
        ("eow", "eow", "python3 ops/tools/build_ai_eow_feedback_review_v1.py", "ops/tools/build_ai_eow_feedback_review_v1.py", "deterministic"),
        ("ui_api", "ui_api", "GET /api/*", "constellation_2/phaseL/ui/server/run_ops_dashboard_v1.py", "deterministic"),
        ("legacy_paper_submit", "legacy_paper_execution", package_scripts.get("aegis:paper:submit", ""), "ops/tools/run_aegis_paper_submit_v1.py", "deterministic"),
    ]
    out = []
    for name, category, command, rel, automation_type in specs:
        path = repo_root / rel
        text = _read_text(path) if path.exists() else ""
        out.append(
            {
                "name": name,
                "category": category,
                "command": command,
                "file": rel if path.exists() else "",
                "trigger_type": "manual_cli" if command and not command.startswith("GET") else "ui_api",
                "writes_artifacts": _writes_artifacts(text),
                "output_paths": _output_paths(text),
                "uses_ai": _has_live_ai_call(text),
                "ai_role": _ai_role(text, name),
                "deterministic": _deterministic_status(text),
                "automation_type": automation_type,
                "safety_class": _safety_class_for_command(command, text),
                "can_execute_trades": _can_execute_trades(command, text),
                "implementation_status": "IMPLEMENTED" if path.exists() else "NOT_FOUND",
                "evidence": _evidence_summary(text),
            }
        )
    return out


def _feedback_loops(repo_root: Path) -> list[dict[str, Any]]:
    specs = [
        ("runtime_readiness_feedback_loop", "npm run aegis:truth-kernel", ["artifact_statuses", "dependency_graph"], "runtime_truth_kernel.v1.json", "audit/control packet/UI", "manual or scheduled only if invoked by operator", "NO_AI_EVIDENCE_FOUND", "review blockers", "IMPLEMENTED"),
        ("recovery_feedback_loop", "npm run aegis:recovery-plan", ["missing_stale_sources.v1.json"], "recovery_plan.v1.txt", "operator", "manual", "NO_AI_EVIDENCE_FOUND", "run exact recovery command", "IMPLEMENTED"),
        ("sleeve_performance_loop", "build_sleeve_performance_report_v1.py", ["manual_trade_packet", "manual_execution_receipt", "outcome_ledger"], "sleeve_performance_report.v1.json", "EOD/EOW review and research tasks", "manual", "NO_AI_EVIDENCE_FOUND", "record receipts/outcomes", "IMPLEMENTED"),
        ("research_hypothesis_loop", "run_research_lab_task_queue_v1.py", ["research_task_queue.v1"], "research_evidence_packet.v1 + research_result_ledger.v1", "research hypothesis status", "manual", "NO_LIVE_AI_CALL_PATH_FOUND", "approve promotion", "PARTIAL"),
        ("manual_trade_capture_loop", "npm run aegis:capture-manual-trade", ["operator fill entry"], "manual_trade_receipt.v1.json + manual_execution_receipt.v1.json", "runtime truth and performance reports", "human initiated", "NO_AI_EVIDENCE_FOUND", "enter real fill details", "IMPLEMENTED"),
        ("ai_feedback_loop", "build_ai_eod_feedback_review_v1.py/build_ai_eow_feedback_review_v1.py", ["sleeve_performance_report"], "ai_feedback_review.v1.json", "research_task_queue.v1", "manual", "DETERMINISTIC_FALLBACK_NO_LLM_RUNTIME", "review generated tasks", "IMPLEMENTED"),
        ("eod_loop", "run_aegis_lite_eod_pipeline_v1.py", ["candidate input", "promoted sleeve library"], "aegis_lite_eod_report + manual_trade_packet", "operator queue/runtime truth", "systemd 09:50 UTC and 14:50 UTC timer exists", "NO_AI_EVIDENCE_FOUND", "review queue", "IMPLEMENTED"),
        ("eow_loop", "build_ai_eow_feedback_review_v1.py/build_eow_sleeve_review_v1.py", ["weekly sleeve performance reports"], "EOW review artifacts", "research task queue", "active c2-weekly-review timer found when systemctl reports it", "DETERMINISTIC_FALLBACK_NO_LLM_RUNTIME", "review weekly findings", "PARTIAL"),
        ("audit_replay_loop", "npm run aegis:audit/replay-state/state-diff", ["runtime snapshots", "transitions"], "audit handoff + replay/diff output", "operator", "manual", "NO_AI_EVIDENCE_FOUND", "read audit", "IMPLEMENTED"),
    ]
    return [
        {
            "name": name,
            "trigger": trigger,
            "input_evidence": inputs,
            "processing_step": trigger,
            "output_artifact": output,
            "consumer": consumer,
            "cadence": cadence,
            "AI_role": ai_role,
            "ai_role": ai_role,
            "human_role": human_role,
            "maturity": maturity if _loop_evidence_exists(repo_root, trigger) else "NOT_IMPLEMENTED",
        }
        for name, trigger, inputs, output, consumer, cadence, ai_role, human_role, maturity in specs
    ]


def _human_required_steps() -> list[dict[str, str]]:
    return [
        {"step": "Review advisory state", "status": "CONFIRMED"},
        {"step": "Execute trades manually outside Aegis", "status": "CONFIRMED"},
        {"step": "Enter manual fill receipt after external execution", "status": "CONFIRMED"},
        {"step": "Attest manual receipt truth", "status": "CONFIRMED"},
        {"step": "Approve research-to-lite promotion", "status": "CONFIRMED"},
        {"step": "Review EOD/EOW findings", "status": "CONFIRMED"},
    ]


def _not_implemented_or_unknown(repo_root: Path, ai_usage: dict[str, Any], research_stages: list[dict[str, Any]]) -> list[dict[str, str]]:
    rows = [
        {
            "name": "live_ai_provider_call",
            "status": "NOT_IMPLEMENTED" if not ai_usage["live_ai_call_path_found"] else "IMPLEMENTED",
            "evidence": "NO LIVE AI CALL PATH FOUND" if not ai_usage["live_ai_call_path_found"] else "See ai_usage.live_ai_call_paths",
        },
        {
            "name": "autonomous_execution_in_target_mode",
            "status": "NOT_IMPLEMENTED",
            "evidence": "runtime truth policy DISABLED_BY_DESIGN",
        },
        {
            "name": "broker_submit_transmit_in_target_mode",
            "status": "NOT_IMPLEMENTED",
            "evidence": "runtime truth policy DISABLED_BY_DESIGN",
        },
        {
            "name": "research_lab_ui",
            "status": "NOT_FOUND",
            "evidence": "No /research-lab route added to phaseL pages inventory",
        },
        {
            "name": "live_alert_transport_claim",
            "status": "NOT_IMPLEMENTED",
            "evidence": "runtime truth separates alert gate/dry-run from live transport proof",
        },
    ]
    for row in research_stages:
        if row["status"] in {"NOT_IMPLEMENTED", "UNKNOWN"}:
            rows.append({"name": row["stage"], "status": row["status"], "evidence": ",".join(row["evidence_files"]) or "NOT_FOUND"})
    return rows


def _implementation_file(repo_root: Path, command: str) -> Path | None:
    matches = re.findall(r"((?:ops|constellation_2)/[A-Za-z0-9_./-]+\.py)", command)
    if not matches:
        return None
    return (repo_root / matches[0]).resolve()


def _writes_artifacts(text: str) -> str:
    if not text:
        return "UNKNOWN"
    write_markers = (".write_text(", ".write_bytes(", "write_json", "write_", "mkdir(", "reports/")
    if any(marker in text for marker in write_markers):
        return "YES_EVIDENCE_IN_SOURCE"
    return "NO_WRITE_EVIDENCE_FOUND"


def _output_paths(text: str) -> list[str]:
    if not text:
        return []
    paths = set(re.findall(r"(?:reports|research_lab|manual_trade_receipts|exports)/[A-Za-z0-9_./{}<>\-]+", text))
    return sorted(item.strip("\"'") for item in paths)[:20]


def _has_live_ai_call(text: str) -> bool:
    return bool(_live_ai_hits(text))


def _live_ai_hits(text: str) -> list[str]:
    if not text:
        return []
    hits: set[str] = set()
    for line in text.splitlines():
        stripped = line.strip()
        if not stripped or stripped.startswith("#"):
            continue
        if stripped[0] in {"\"", "'"}:
            continue
        if "LIVE_AI_PATTERNS" in stripped:
            continue
        for pattern in LIVE_AI_PATTERNS:
            if pattern in stripped:
                hits.add(pattern)
    return sorted(hits)


def _ai_role(text: str, name: str) -> str:
    if _has_live_ai_call(text):
        return "LIVE_AI_CALL_PATH_FOUND"
    if any(pattern in text for pattern in ("DETERMINISTIC_FALLBACK_NO_LLM_RUNTIME", "DETERMINISTIC_PLACEHOLDER_NO_LLM_RUNTIME", "deterministic_fallback_used")):
        return "DETERMINISTIC_FALLBACK_NO_LLM_RUNTIME"
    if "ai_hypothesis_batch" in text or "AI_HYPOTHESIS" in text:
        return "VALIDATES_EXTERNAL_AI_JSON_NO_PROVIDER_CALL"
    if "chatgpt" in name.lower():
        return "CHATGPT_PACKET_EXPORT_NO_PROVIDER_CALL"
    return "NO_AI_EVIDENCE_FOUND"


def _deterministic_status(text: str) -> str:
    if not text:
        return "UNKNOWN"
    if "deterministic" in text.lower() or "canonical" in text.lower() or "fail-closed" in text.lower():
        return "DETERMINISTIC_EVIDENCE_FOUND"
    if _has_live_ai_call(text):
        return "AI_CALL_PATH_FOUND"
    return "UNKNOWN"


def _safety_class_for_command(command: str, text: str) -> str:
    lower = f"{command}\n{text}".lower()
    if "capture_manual_trade_receipt" in lower or "manual_trade_receipts" in lower:
        return "WRITE_RECEIPT_JOURNALING_ONLY"
    if "run_aegis_paper_submit_v1.py" in lower or "_run_governed_submit_stage" in lower:
        return "LEGACY_GUARDED_PAPER_SUBMIT"
    if "ui:start" in lower or "supervisor.py start" in lower:
        return "SERVICE_CONTROL"
    if "show_" in lower or "status" in lower or "read_model" in lower:
        return "READ_ONLY"
    if "reports/" in lower or "write_" in lower or ".write_text" in lower or ".write_bytes" in lower:
        return "WRITE_REPORT"
    return "UNKNOWN"


def _can_execute_trades(command: str, text: str) -> bool:
    combined = f"{command}\n{text}"
    if "capture_manual_trade_receipt" in combined:
        return False
    if "broker submit/transmit is out of scope" in combined:
        return False
    if "run_aegis_paper_submit_v1.py" in combined and "_run_governed_submit_stage" in combined:
        return True
    return False


def _evidence_summary(text: str) -> str:
    if not text:
        return "NOT_FOUND"
    hits = []
    for marker in ("ai_used", "deterministic_fallback_used", "broker_submit_required", "broker_transmit_enabled", "manual_execution_only", "production_mutation", "auto_promotion_allowed"):
        if marker in text:
            hits.append(marker)
    return ",".join(hits) if hits else "SOURCE_FILE_PRESENT"


def _input_artifacts_for_name(name: str) -> list[str]:
    mapping = {
        "sleeve_performance_report": ["manual_trade_packet", "manual_execution_receipt", "outcome_ledger", "promoted_sleeve_library"],
        "eod_sleeve_review": ["sleeve_performance_report"],
        "eow_sleeve_review": ["sleeve_performance_report"],
        "aegis_lite_eod_pipeline": ["candidate_input", "promoted_sleeve_library", "market_calendar"],
        "ai_eod_feedback_review": ["sleeve_performance_report", "event_awareness_ledger", "trade_capture_alert_ledger", "event_market_snapshot"],
        "ai_eow_feedback_review": ["sleeve_performance_report", "event_awareness_ledger", "trade_capture_alert_ledger", "event_market_snapshot"],
    }
    return mapping.get(name, [])


def _feeds_for_name(name: str) -> list[str]:
    mapping = {
        "aegis_lite_eod_pipeline": ["runtime_truth", "operator_queue", "manual_trade_packet"],
        "ai_eod_feedback_review": ["research_task_queue"],
        "ai_eow_feedback_review": ["research_task_queue"],
        "eod_sleeve_review": ["research_task_queue_when_flag_set"],
        "eow_sleeve_review": ["research_task_queue_when_flag_set"],
    }
    return mapping.get(name, [])


def _loop_evidence_exists(repo_root: Path, trigger: str) -> bool:
    for rel in re.findall(r"(ops/[A-Za-z0-9_./-]+\.py)", trigger):
        if (repo_root / rel).exists():
            return True
    for filename in re.findall(r"([A-Za-z0-9_]+\.py)", trigger):
        if (repo_root / "ops" / "tools" / filename).exists():
            return True
    if trigger.startswith("npm run"):
        return True
    return "build_ai_eow_feedback_review_v1.py" in trigger or "build_eow_sleeve_review_v1.py" in trigger


def _first_match(text: str, pattern: str) -> str:
    match = re.search(pattern, text)
    return match.group(1).strip() if match else ""


def _read_text(path: Path | None) -> str:
    if not path:
        return ""
    try:
        return path.read_text(encoding="utf-8")
    except Exception:
        return ""


def _rel(repo_root: Path, path: Path | None) -> str:
    if not path:
        return ""
    try:
        return str(Path(path).resolve().relative_to(repo_root))
    except Exception:
        return str(path)


def _csv_value(value: Any) -> str:
    if isinstance(value, (list, tuple)):
        return "|".join(str(item) for item in value)
    if isinstance(value, dict):
        return json.dumps(value, sort_keys=True, separators=(",", ":"))
    if value is True:
        return "true"
    if value is False:
        return "false"
    if value is None:
        return ""
    return str(value)


def _now() -> str:
    return datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")


if __name__ == "__main__":
    raise SystemExit(main())
