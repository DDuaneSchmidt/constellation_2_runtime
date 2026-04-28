from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path
from typing import Any


AUTHORITY_DEFS: tuple[dict[str, Any], ...] = (
    {
        "authority_name": "portfolio_account_authority_v1",
        "filename": "portfolio_account_authority.v1.json",
        "phase": "PRE_MARKET",
        "dependencies": [],
        "producer_command": "python3 ops/tools/run_portfolio_account_authority_v1.py --day_utc {day_utc} --truth_root {truth_root}",
        "required_or_diagnostic": "diagnostic",
        "blocking_states": ["FAIL"],
        "allowed_states": ["PASS", "WARN"],
        "owning_subsystem": "portfolio_account_authority",
    },
    {
        "authority_name": "market_data_authority_v1",
        "filename": "market_data_authority.v1.json",
        "phase": "PRE_MARKET",
        "dependencies": [],
        "producer_command": "python3 ops/tools/run_market_data_authority_v1.py --day_utc {day_utc} --truth_root {truth_root}",
        "required_or_diagnostic": "required",
        "blocking_states": ["FAIL"],
        "allowed_states": ["PASS", "WARN"],
        "owning_subsystem": "market_data_authority",
    },
    {
        "authority_name": "strategy_decision_authority_v1",
        "filename": "strategy_decision_authority.v1.json",
        "phase": "INTENT",
        "dependencies": ["market_data_authority_v1"],
        "producer_command": "python3 ops/tools/run_strategy_decision_authority_v1.py --day_utc {day_utc} --truth_root {truth_root}",
        "required_or_diagnostic": "diagnostic",
        "blocking_states": ["FAIL"],
        "allowed_states": ["PASS"],
        "owning_subsystem": "strategy_decision_authority",
    },
    {
        "authority_name": "risk_sizing_authority_v1",
        "filename": "risk_sizing_authority.v1.json",
        "phase": "PRE_MARKET",
        "dependencies": ["portfolio_account_authority_v1"],
        "producer_command": "python3 ops/tools/run_risk_sizing_authority_v1.py --day_utc {day_utc} --truth_root {truth_root}",
        "required_or_diagnostic": "diagnostic",
        "blocking_states": ["FAIL"],
        "allowed_states": ["PASS"],
        "owning_subsystem": "risk_sizing_authority",
    },
    {
        "authority_name": "paper_trading_day_authority_v1",
        "filename": "paper_trading_day_authority.v1.json",
        "phase": "PRE_MARKET",
        "dependencies": ["market_data_authority_v1"],
        "producer_command": "python3 ops/tools/run_paper_trading_day_authority_v1.py --day_utc {day_utc} --truth_root {truth_root}",
        "required_or_diagnostic": "required",
        "blocking_states": ["PREFLIGHT_REQUIRED", "PREFLIGHT_BLOCKED", "INVALIDATED"],
        "allowed_states": ["OPEN_READY", "SUBMITTING", "CLOSED"],
        "owning_subsystem": "paper_trading_day_authority",
    },
    {
        "authority_name": "runtime_service_authority_v1",
        "filename": "runtime_service_authority.v1.json",
        "phase": "PRE_MARKET",
        "dependencies": ["execution_mode_authority_v1"],
        "producer_command": "python3 ops/tools/run_runtime_service_authority_v1.py --day_utc {day_utc} --truth_root {truth_root}",
        "required_or_diagnostic": "required",
        "blocking_states": ["FAIL"],
        "allowed_states": ["PASS", "WARN"],
        "owning_subsystem": "runtime_service_authority",
    },
    {
        "authority_name": "execution_mode_authority_v1",
        "filename": "execution_mode_authority.v1.json",
        "phase": "SUBMIT",
        "dependencies": [],
        "producer_command": "python3 ops/tools/run_execution_mode_authority_v1.py --day_utc {day_utc} --truth_root {truth_root}",
        "required_or_diagnostic": "required",
        "blocking_states": ["FAIL"],
        "allowed_states": ["PASS"],
        "owning_subsystem": "execution_mode_authority",
    },
    {
        "authority_name": "execution_lifecycle_authority_v1",
        "filename": "execution_lifecycle_authority.v1.json",
        "phase": "POST_SUBMIT",
        "dependencies": ["paper_trading_day_authority_v1"],
        "producer_command": "python3 ops/tools/run_execution_lifecycle_authority_v1.py --day_utc {day_utc} --truth_root {truth_root}",
        "required_or_diagnostic": "required",
        "blocking_states": ["FAIL"],
        "allowed_states": ["PASS", "WARN"],
        "owning_subsystem": "execution_lifecycle_authority",
        "root": "execution",
    },
    {
        "authority_name": "trade_lineage_graph_v1",
        "filename": "trade_lineage_graph.v1.json",
        "phase": "POST_SUBMIT",
        "dependencies": ["execution_lifecycle_authority_v1"],
        "producer_command": "python3 ops/tools/run_trade_lineage_graph_v1.py --day_utc {day_utc} --truth_root {truth_root}",
        "required_or_diagnostic": "diagnostic",
        "blocking_states": ["FAIL"],
        "allowed_states": ["PASS", "WARN"],
        "owning_subsystem": "trade_lineage_graph",
    },
    {
        "authority_name": "trading_day_closure_authority_v1",
        "filename": "trading_day_closure_authority.v1.json",
        "phase": "CLOSURE",
        "dependencies": ["execution_lifecycle_authority_v1", "trade_lineage_graph_v1"],
        "producer_command": "python3 ops/tools/run_trading_day_closure_authority_v1.py --day_utc {day_utc} --truth_root {truth_root}",
        "required_or_diagnostic": "required",
        "blocking_states": ["FAIL"],
        "allowed_states": ["PASS", "WARN"],
        "owning_subsystem": "trading_day_closure_authority",
    },
)

DEPENDENCY_ORDER = [
    "portfolio_account_authority_v1",
    "market_data_authority_v1",
    "strategy_decision_authority_v1",
    "risk_sizing_authority_v1",
    "paper_trading_day_authority_v1",
    "execution_mode_authority_v1",
    "runtime_service_authority_v1",
    "execution_lifecycle_authority_v1",
    "trade_lineage_graph_v1",
    "trading_day_closure_authority_v1",
]


def _utc_now_iso() -> str:
    return datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def authority_graph_output_path(*, truth_root: Path, day_utc: str) -> Path:
    return (
        Path(truth_root).resolve()
        / "reports"
        / "aegis_authority_graph_v1"
        / day_utc
        / "aegis_authority_graph.v1.json"
    ).resolve()


def authority_artifact_path(*, truth_root: Path, execution_root: Path, day_utc: str, authority_name: str, filename: str, root: str = "truth") -> Path:
    base = Path(execution_root if root == "execution" else truth_root).resolve()
    return (base / "reports" / authority_name / day_utc / filename).resolve()


def _producer_exists(repo_root: Path, producer_command: str | None) -> bool:
    if not producer_command:
        return False
    parts = producer_command.split()
    for token in parts:
        if token.startswith("ops/tools/") and token.endswith(".py"):
            return (Path(repo_root).resolve() / token).exists()
    return True


def _read_json(path: Path) -> dict[str, Any] | None:
    if not path.exists() or not path.is_file():
        return None
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return None
    return payload if isinstance(payload, dict) else None


def _observed_state(payload: dict[str, Any] | None) -> str:
    if not isinstance(payload, dict):
        return "MISSING"
    for key in (
        "status",
        "state",
        "mode_state",
        "service_state",
        "market_data_state",
        "risk_sizing_state",
        "strategy_decision_state",
        "account_state",
        "current_lifecycle_state",
        "closure_state",
        "identity_state",
    ):
        text = str(payload.get(key) or "").strip().upper()
        if text:
            return text
    return "PRESENT"


def build_aegis_authority_graph_v1(
    *,
    day_utc: str,
    truth_root: Path,
    execution_root: Path,
    repo_root: Path,
    produced_utc: str | None = None,
) -> dict[str, Any]:
    nodes: list[dict[str, Any]] = []
    blocking_nodes: list[dict[str, Any]] = []
    for item in AUTHORITY_DEFS:
        command_template = item.get("producer_command")
        producer_command = (
            str(command_template).format(day_utc=day_utc, truth_root=str(Path(truth_root).resolve()))
            if command_template
            else None
        )
        producer_available = _producer_exists(repo_root, producer_command)
        path = authority_artifact_path(
            truth_root=truth_root,
            execution_root=execution_root,
            day_utc=day_utc,
            authority_name=str(item["authority_name"]),
            filename=str(item["filename"]),
            root=str(item.get("root") or "truth"),
        )
        payload = _read_json(path)
        observed_state = _observed_state(payload)
        node_status = "PRESENT" if payload is not None else "MISSING"
        if not producer_available:
            node_status = "NOT_IMPLEMENTED"
            producer_command = None
        required_or_diagnostic = str(item["required_or_diagnostic"])
        is_blocking = bool(
            required_or_diagnostic == "required"
            and (
                node_status in {"MISSING", "NOT_IMPLEMENTED"}
                or observed_state in {str(x).upper() for x in item.get("blocking_states", [])}
            )
        )
        node = {
            "authority_name": item["authority_name"],
            "artifact_path": str(path),
            "producer_command": producer_command,
            "phase": item["phase"],
            "dependencies": list(item.get("dependencies") or []),
            "freshness_rule": "same_day",
            "required_or_diagnostic": required_or_diagnostic,
            "blocking_states": list(item.get("blocking_states") or []),
            "allowed_states": list(item.get("allowed_states") or []),
            "owning_subsystem": item["owning_subsystem"],
            "status": node_status,
            "observed_state": observed_state,
            "operator_actionable": bool(node_status in {"MISSING", "NOT_IMPLEMENTED"} or is_blocking),
            "blocks_daily_outcome": is_blocking,
        }
        nodes.append(node)
        if is_blocking:
            blocking_nodes.append(
                {
                    "authority_name": node["authority_name"],
                    "artifact_path": node["artifact_path"],
                    "observed_state": node["observed_state"],
                    "owner": node["owning_subsystem"],
                    "producer_command": node["producer_command"],
                }
            )
    return {
        "schema_id": "aegis_authority_graph",
        "schema_version": "v1",
        "day_utc": day_utc,
        "authority_nodes": nodes,
        "blocking_nodes": blocking_nodes,
        "dependency_order": list(DEPENDENCY_ORDER),
        "produced_utc": produced_utc or _utc_now_iso(),
    }


def write_aegis_authority_graph_v1(*, truth_root: Path, day_utc: str, payload: dict[str, Any]) -> Path:
    path = authority_graph_output_path(truth_root=truth_root, day_utc=day_utc)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, sort_keys=True, separators=(",", ":")) + "\n", encoding="utf-8")
    return path
