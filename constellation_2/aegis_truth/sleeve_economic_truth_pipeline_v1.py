from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
import json
import os
from typing import Any

from constellation_2.aegis_truth.sleeve_effectiveness_v1 import materialize_sleeve_effectiveness_v1
from constellation_2.aegis_truth.sleeve_fill_quality_v1 import materialize_sleeve_fill_quality_v1
from constellation_2.aegis_truth.sleeve_realized_pnl_v1 import materialize_sleeve_realized_pnl_v1
from constellation_2.aegis_truth.sleeve_risk_realization_v1 import materialize_sleeve_risk_realization_v1
from constellation_2.aegis_truth.sleeve_scorecard_rollup_v1 import materialize_sleeve_scorecard_rollup_v1
from constellation_2.aegis_truth.sleeve_trade_fact_v1 import materialize_sleeve_trade_fact_v1
from constellation_2.aegis_truth.decision_price_snapshot_v1 import materialize_decision_price_snapshot_v1


NON_HARD_STOP_STATUSES = {"PASS", "NO_COMPLETED_TRADES", "INSUFFICIENT_EVIDENCE", "SCORECARD_NOT_ENOUGH_EVIDENCE"}


@dataclass(frozen=True)
class SleeveEconomicTruthPipelineResultV1:
    report_path: Path
    report: dict[str, Any]


def run_sleeve_economic_truth_pipeline_v1(
    *,
    day_utc: str,
    truth_root: Path,
    execution_root: Path,
    evaluation_utc: str | None = None,
    scheduled_run: bool = False,
) -> SleeveEconomicTruthPipelineResultV1:
    day = _require_day(day_utc)
    truth_root = Path(truth_root).resolve()
    execution_root = Path(execution_root).resolve()
    produced_at = _now(evaluation_utc)
    stages: list[dict[str, Any]] = []
    blockers: list[dict[str, Any]] = []

    trade = materialize_sleeve_trade_fact_v1(day_utc=day, truth_root=truth_root, execution_root=execution_root, evaluation_utc=produced_at, scheduled_run=scheduled_run)
    _append_stage(stages, "sleeve_trade_fact_v1", trade.report_path, trade.report)
    if _hard_blocked(trade.report):
        blockers.append(_blocker_from_stage(stages[-1], "Refresh post-trade lifecycle and broker fill evidence."))
        return _write_pipeline_report(day=day, truth_root=truth_root, execution_root=execution_root, produced_at=produced_at, scheduled_run=scheduled_run, stages=stages, blockers=blockers)

    pnl = materialize_sleeve_realized_pnl_v1(day_utc=day, truth_root=truth_root, evaluation_utc=produced_at, scheduled_run=scheduled_run)
    _append_stage(stages, "sleeve_realized_pnl_v1", pnl.report_path, pnl.report)
    if _hard_blocked(pnl.report):
        blockers.append(_blocker_from_stage(stages[-1], "Refresh broker-backed cash, NAV, positions, and post-trade reconciliation."))
        return _write_pipeline_report(day=day, truth_root=truth_root, execution_root=execution_root, produced_at=produced_at, scheduled_run=scheduled_run, stages=stages, blockers=blockers)

    risk = materialize_sleeve_risk_realization_v1(day_utc=day, truth_root=truth_root, evaluation_utc=produced_at, scheduled_run=scheduled_run)
    _append_stage(stages, "sleeve_risk_realization_v1", risk.report_path, risk.report)
    if _hard_blocked(risk.report):
        blockers.append(_blocker_from_stage(stages[-1], "Materialize expected risk and allocation evidence before risk realization."))
        return _write_pipeline_report(day=day, truth_root=truth_root, execution_root=execution_root, produced_at=produced_at, scheduled_run=scheduled_run, stages=stages, blockers=blockers)

    decision_price = materialize_decision_price_snapshot_v1(day_utc=day, truth_root=truth_root, execution_root=execution_root, evaluation_utc=produced_at, scheduled_run=scheduled_run)
    _append_stage(stages, "decision_price_snapshot_v1", decision_price.summary_path, decision_price.summary)

    fill = materialize_sleeve_fill_quality_v1(day_utc=day, truth_root=truth_root, evaluation_utc=produced_at, scheduled_run=scheduled_run)
    _append_stage(stages, "sleeve_fill_quality_v1", fill.report_path, fill.report)
    if _hard_blocked(fill.report):
        blockers.append(_blocker_from_stage(stages[-1], "Refresh market data and decision reference price evidence before fill quality."))
        return _write_pipeline_report(day=day, truth_root=truth_root, execution_root=execution_root, produced_at=produced_at, scheduled_run=scheduled_run, stages=stages, blockers=blockers)

    effectiveness = materialize_sleeve_effectiveness_v1(day_utc=day, truth_root=truth_root, evaluation_utc=produced_at, scheduled_run=scheduled_run)
    _append_stage(stages, "sleeve_effectiveness_v1", effectiveness.report_path, effectiveness.report)
    if _hard_blocked(effectiveness.report):
        blockers.append(_blocker_from_stage(stages[-1], "Clear upstream sleeve effectiveness inputs."))
        return _write_pipeline_report(day=day, truth_root=truth_root, execution_root=execution_root, produced_at=produced_at, scheduled_run=scheduled_run, stages=stages, blockers=blockers)

    scorecards = materialize_sleeve_scorecard_rollup_v1(day_utc=day, truth_root=truth_root, evaluation_utc=produced_at, scheduled_run=scheduled_run)
    _append_stage(stages, "sleeve_scorecard_daily_v1", scorecards.daily_path, scorecards.daily)
    _append_stage(stages, "sleeve_scorecard_weekly_v1", scorecards.weekly_path, scorecards.weekly)
    _append_stage(stages, "sleeve_scorecard_monthly_v1", scorecards.monthly_path, scorecards.monthly)
    return _write_pipeline_report(day=day, truth_root=truth_root, execution_root=execution_root, produced_at=produced_at, scheduled_run=scheduled_run, stages=stages, blockers=blockers)


def _write_pipeline_report(*, day: str, truth_root: Path, execution_root: Path, produced_at: str, scheduled_run: bool, stages: list[dict[str, Any]], blockers: list[dict[str, Any]]) -> SleeveEconomicTruthPipelineResultV1:
    final_status = "BLOCKED" if blockers else _final_nonblocked_status(stages)
    report = {
        "schema_version": "sleeve_economic_truth_pipeline.v1",
        "day_utc": day,
        "status": final_status,
        "first_blocker": blockers[0]["blocker_code"] if blockers else "",
        "produced_at_utc": produced_at,
        "producer": "sleeve_economic_truth_pipeline_v1",
        "scheduled_run": bool(scheduled_run),
        "truth_root": str(truth_root),
        "execution_root": str(execution_root),
        "stages": stages,
        "blockers": blockers,
        "operator_next_action": blockers[0]["operator_next_action"] if blockers else "Sleeve economic truth measurement completed.",
    }
    path = truth_root / "reports" / "sleeve_economic_truth_pipeline_v1" / day / "sleeve_economic_truth_pipeline.v1.json"
    _write_json(path, report)
    return SleeveEconomicTruthPipelineResultV1(report_path=path, report=report)


def _append_stage(stages: list[dict[str, Any]], stage_id: str, path: Path, report: dict[str, Any]) -> None:
    stages.append(
        {
            "stage_id": stage_id,
            "status": str(report.get("status") or ""),
            "first_blocker": str(report.get("first_blocker") or ""),
            "artifact_path": str(path),
            "scheduled_run": bool(report.get("scheduled_run")),
        }
    )


def _hard_blocked(report: dict[str, Any]) -> bool:
    return str(report.get("status") or "").upper() not in NON_HARD_STOP_STATUSES


def _final_nonblocked_status(stages: list[dict[str, Any]]) -> str:
    statuses = {str(stage.get("status") or "").upper() for stage in stages}
    if "NO_COMPLETED_TRADES" in statuses:
        return "NO_COMPLETED_TRADES"
    if "INSUFFICIENT_EVIDENCE" in statuses or "SCORECARD_NOT_ENOUGH_EVIDENCE" in statuses:
        return "INSUFFICIENT_EVIDENCE"
    return "PASS"


def _blocker_from_stage(stage: dict[str, Any], next_action: str) -> dict[str, Any]:
    return {
        "blocker_code": stage.get("first_blocker") or f"{stage.get('stage_id')}_BLOCKED",
        "stage_id": stage.get("stage_id"),
        "artifact_path": stage.get("artifact_path"),
        "operator_next_action": next_action,
    }


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    os.replace(tmp, path)


def _require_day(value: str) -> str:
    text = str(value or "").strip()
    datetime.fromisoformat(text)
    if len(text) != 10:
        raise ValueError("day_utc must be YYYY-MM-DD")
    return text


def _now(value: str | None) -> str:
    return str(value or "").strip() if value else datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")
