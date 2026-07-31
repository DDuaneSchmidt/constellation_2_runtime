#!/usr/bin/env python3
from __future__ import annotations

import argparse
import hashlib
import json
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

DEFAULT_TRUTH_ROOT = Path("/home/node/constellation_runtime_data/truth")


def _now() -> str:
    return datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _read_json(path: Path) -> dict[str, Any]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}
    return payload if isinstance(payload, dict) else {}


def _sha256(path: Path) -> str:
    try:
        return hashlib.sha256(path.read_bytes()).hexdigest()
    except OSError:
        return ""


def _source(root: Path, family: str, day: str, filename: str) -> dict[str, Any]:
    path = root / "reports" / family / day / filename
    return {
        "family": family,
        "path": str(path),
        "exists": path.exists(),
        "sha256": _sha256(path) if path.exists() else "",
        "payload": _read_json(path) if path.exists() else {},
    }


def _safe_list(value: Any) -> list[Any]:
    return value if isinstance(value, list) else []


def _count(value: Any) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return 0


def build_workspace(root: Path, day: str, generated_at: str) -> dict[str, Any]:
    sources = {
        "canonical_operator_state": _source(root, "aegis_canonical_operator_state_v1", day, "canonical_operator_state.v1.json"),
        "operator_state_snapshot": _source(root, "operator_state_snapshot_v1", day, "operator_state_snapshot.v1.json"),
        "operator_projection": _source(root, "aegis_operator_projection_v1", day, "projection.v1.json"),
        "paper_review_queue": _source(root, "aegis_paper_review_queue_v1", day, "paper_review_queue.v1.json"),
        "paper_position_ledger": _source(root, "aegis_paper_position_ledger_v1", day, "paper_position_ledger.v1.json"),
        "trading_lifecycle_state": _source(root, "aegis_trading_lifecycle_state_v1", day, "trading_lifecycle_state.v1.json"),
        "paper_pnl_report": _source(root, "aegis_paper_pnl_report_v1", day, "paper_pnl_report.v1.json"),
        "daily_paper_performance": _source(root, "aegis_daily_paper_performance_v1", day, "daily_paper_performance.v1.json"),
        "mode_readiness": _source(root, "aegis_mode_readiness_v1", day, "mode_readiness.v1.json"),
        "daily_operator": _source(root, "aegis_daily_operator_v1", day, "daily_operator.v1.json"),
    }
    canonical = sources["canonical_operator_state"]["payload"]
    daily = sources["daily_paper_performance"]["payload"]
    pnl = sources["paper_pnl_report"]["payload"]
    queue = sources["paper_review_queue"]["payload"]
    ledger = sources["paper_position_ledger"]["payload"]
    mode = sources["mode_readiness"]["payload"]
    candidate_projection = canonical.get("candidate_ui_projection") if isinstance(canonical.get("candidate_ui_projection"), dict) else {}
    run_summary = candidate_projection.get("run_summary") if isinstance(candidate_projection.get("run_summary"), dict) else {}
    source_artifacts = [
        {key: {k: v for k, v in row.items() if k != "payload"}}
        for key, row in sources.items()
    ]
    missing = [key for key, row in sources.items() if not row["exists"] and key in {"canonical_operator_state", "paper_review_queue", "paper_position_ledger", "daily_paper_performance", "mode_readiness"}]
    safety = canonical.get("safety") if isinstance(canonical.get("safety"), dict) else {}
    return {
        "schema_id": "aegis_daily_operator_workspace",
        "schema_version": "v1",
        "artifact_id": f"aegis_daily_operator_workspace_v1:{day}",
        "day_utc": day,
        "generated_at_utc": generated_at,
        "truth_root": str(root),
        "workspace_status": "AVAILABLE" if not missing else "DEGRADED_MISSING_SOURCES",
        "missing_required_sources": missing,
        "source_artifacts": source_artifacts,
        "navigation_routes": ["/aegis-command-center", "/aegis-positions", "/aegis-history", "/aegis-research-workspace"],
        "command_center": {
            "reviewable_candidate_count": _count(candidate_projection.get("reviewable_candidate_count") or canonical.get("reviewable_candidate_count")),
            "awaiting_review_count": _count(queue.get("awaiting_review_count") or candidate_projection.get("operator_awaiting_review_count")),
            "run_visibility_status": candidate_projection.get("run_visibility_status") or run_summary.get("run_visibility_status") or "UNKNOWN",
            "actions_required_count": len(_safe_list(canonical.get("actions_required"))),
        },
        "positions": {
            "open_position_count": _count(pnl.get("open_position_count") or ledger.get("open_position_count")),
            "closed_position_count": _count(pnl.get("closed_position_count") or ledger.get("closed_position_count")),
            "total_open_positions": _count(daily.get("total_open_positions") or pnl.get("open_position_count")),
            "positions_needing_attention": len(_safe_list(daily.get("positions_needing_operator_attention"))) or _count(daily.get("operator_attention_count")),
        },
        "performance": {
            "unrealized_pnl": str(daily.get("unrealized_pnl") or pnl.get("unrealized_pnl") or ""),
            "realized_pnl": str(daily.get("realized_pnl") or pnl.get("realized_pnl") or ""),
            "total_paper_pnl": str(daily.get("total_paper_pnl") or pnl.get("total_paper_pnl") or ""),
            "data_quality_status": daily.get("data_quality_status") or pnl.get("data_quality_status") or "UNKNOWN",
        },
        "readiness": {
            "active_mode": mode.get("active_mode") or canonical.get("active_mode") or "HUMAN_REVIEWED_PAPER_MODE",
            "active_mode_readiness_status": mode.get("active_mode_readiness_status") or canonical.get("active_mode_readiness_status") or "UNKNOWN",
            "runtime_truth_classification": canonical.get("runtime_truth_classification") or "UNKNOWN",
            "runtime_readiness_status": canonical.get("runtime_readiness_status") or canonical.get("highest_readiness_layer") or "UNKNOWN",
        },
        "safety": {
            "broker_execution_allowed": bool(safety.get("broker_execution_allowed", False)),
            "broker_submit_transmit_allowed": bool(safety.get("broker_submit_transmit_allowed", False)),
            "autonomous_execution_allowed": bool(safety.get("autonomous_execution_allowed", False)),
            "trade_advice_allowed": bool(canonical.get("trade_advice_allowed", False)),
        },
    }


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="build_aegis_daily_operator_workspace_v1")
    parser.add_argument("--truth-root", "--truth_root", default=str(DEFAULT_TRUTH_ROOT))
    parser.add_argument("--day", default=datetime.now(UTC).strftime("%Y-%m-%d"))
    parser.add_argument("--generated-at-utc", "--generated_at_utc", default="")
    args = parser.parse_args(argv)
    root = Path(args.truth_root).expanduser().resolve()
    day = str(args.day)
    payload = build_workspace(root, day, args.generated_at_utc or _now())
    out_dir = root / "reports" / "aegis_daily_operator_workspace_v1" / day
    out_dir.mkdir(parents=True, exist_ok=True)
    json_path = out_dir / "daily_operator_workspace.v1.json"
    json_path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    print(json.dumps({
        "json": str(json_path),
        "workspace_status": payload["workspace_status"],
        "broker_execution_allowed": False,
        "broker_submit_transmit_allowed": False,
        "autonomous_execution_allowed": False,
        "trade_advice_allowed": False,
    }, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
