from __future__ import annotations

import argparse
import json
import sys
from datetime import date
from pathlib import Path
from typing import Any

from research_lab.acquisition.csv_readiness import build_csv_readiness_report, write_csv_readiness_report
from research_lab.acquisition.csv_staging import import_local_csvs as import_local_csvs_impl
from research_lab.acquisition.csv_template import create_csv_template as create_csv_template_impl
from research_lab.acquisition.download_guide import (
    csv_download_guide as csv_download_guide_impl,
    verify_downloaded_csvs as verify_downloaded_csvs_impl,
)
from research_lab.acquisition.expected_files import expected_csv_files_report
from research_lab.audit.audit_log import write_audit_event
from research_lab.breadth.breadth_registry import build_and_store_breadth_snapshot
from research_lab.backtests.backtest_plan import build_backtest_plan
from research_lab.backtests.backtest_plan_registry import store_backtest_plan
from research_lab.backtests.holding_period_backtester import run_holding_period_backtest
from research_lab.candidates.candidate_generator import generate_drop_reversion_candidates
from research_lab.candidates.candidate_registry import (
    candidate_ledger_summary as candidate_ledger_summary_impl,
    candidates_with_latest_status,
    load_candidate_batch,
    store_candidate_batch,
)
from research_lab.candidates.operator_decision import append_operator_decision, build_operator_decision
from research_lab.config.provider_config import provider_config_status as provider_config_status_impl
from research_lab.config.provider_config import provider_ready_or_raise
from research_lab.config.runtime_config import apply_runtime_config
from research_lab.challengers.challenger_comparison import write_challenger_comparison_report
from research_lab.challengers.challenger_evidence import evidence_quality_summary, write_challenger_evidence_batch
from research_lab.challengers.challenger_track import write_challenger_research_track
from research_lab.challengers.human_review_dossier import write_human_review_dossier
from research_lab.costs.cost_model_registry import create_default_cost_model_snapshot as create_default_cost_model_snapshot_impl
from research_lab.datasets.dataset_registry import (
    list_dataset_snapshots,
    store_dataset_snapshot,
    validate_dataset_snapshot,
)
from research_lab.datasets.coverage_report import load_coverage_report
from research_lab.datasets.dataset_snapshot import build_empty_dataset_snapshot
from research_lab.datasets.ohlcv_builder import build_ohlcv_dataset_snapshot
from research_lab.evidence.evidence_comparison import compare_evidence_packages, write_evidence_comparison
from research_lab.evidence.evidence_registry import list_evidence_packages, load_evidence_package_manifest
from research_lab.event_intake.event_intake_registry import (
    create_event_cluster as create_event_cluster_impl,
    capture_event_observation as capture_event_observation_impl,
    generate_hypothesis_proposal as generate_hypothesis_proposal_impl,
    generate_intent_candidate as generate_intent_candidate_impl,
    seed_event_families as seed_event_families_impl,
)
from research_lab.research_intake.intake_registry import (
    assess_hypothesis_readiness as assess_hypothesis_readiness_impl,
    build_and_store_research_intake_dossier as build_research_intake_dossier_impl,
    convert_hypothesis_proposal_to_research_plan as convert_hypothesis_proposal_to_research_plan_impl,
    review_hypothesis_proposal as review_hypothesis_proposal_impl,
)
from research_lab.research_intake.proposal_queue import hypothesis_proposal_queue as hypothesis_proposal_queue_impl
from research_lab.projections.projection_builder import rebuild_research_projections as rebuild_research_projections_impl
from research_lab.projections.projection_validator import validate_research_projections as validate_research_projections_impl
from research_lab.recovery.hypothesis_recovery import latest_hypothesis_recovery_report as latest_hypothesis_recovery_report_impl
from research_lab.recovery.hypothesis_recovery import recover_hypotheses as recover_hypotheses_impl
from research_lab.longitudinal.candidate_run import run_longitudinal_candidate_study
from research_lab.macro_events.macro_event_registry import import_and_store_macro_event_calendar
from research_lab.longitudinal.ranking_quality import write_ranking_quality_report
from research_lab.longitudinal.sleeve_learning_report import write_sleeve_learning_report
from research_lab.outcomes.measurement_runner import run_candidate_outcome_measurement
from research_lab.outcomes.outcome_registry import build_candidate_outcome_summary, load_outcomes
from research_lab.paper_trials.observation import record_paper_observation as record_paper_observation_impl
from research_lab.paper_trials.due_outcomes import list_paper_due_outcomes as list_paper_due_outcomes_impl
from research_lab.paper_trials.operations import (
    measure_due_paper_outcomes as measure_due_paper_outcomes_impl,
    record_next_paper_observation as record_next_paper_observation_impl,
)
from research_lab.paper_trials.operations_report import write_paper_trial_operations_report
from research_lab.paper_trials.outcome import measure_paper_outcomes as measure_paper_outcomes_impl
from research_lab.paper_trials.paper_trial import build_paper_trial
from research_lab.paper_trials.paper_trial_registry import (
    build_paper_trial_summary,
    change_paper_trial_status,
    store_paper_trial,
)
from research_lab.paper_trials.review import record_paper_trial_review
from research_lab.portfolio.backlog_priority import write_research_backlog_priority_report
from research_lab.portfolio.evidence_inventory import write_evidence_inventory
from research_lab.portfolio.paper_trial_inventory import write_paper_trial_inventory
from research_lab.portfolio.sleeve_comparison import write_sleeve_comparison_report
from research_lab.providers.factory import get_daily_ohlcv_provider, provider_diagnostics as get_provider_diagnostics
from research_lab.regimes.regime_builder import build_regime_snapshot as build_regime_snapshot_impl
from research_lab.research.research_plan import build_research_plan
from research_lab.research.research_plan_registry import store_research_plan
from research_lab.runtime.dependencies import research_dependency_health
from research_lab.runners.event_study_runner import run_event_study as run_event_study_runner
from research_lab.sleeves.sleeve_challenger import build_sleeve_challenge, store_sleeve_challenge
from research_lab.sleeves.sleeve_definition import build_sleeve_definition, build_sleeve_version
from research_lab.sleeves.sleeve_health import build_sleeve_health_snapshot, store_sleeve_health_snapshot
from research_lab.sleeves.sleeve_registry import store_sleeve_definition, store_sleeve_version
from research_lab.sleeves.sleeve_review import build_sleeve_review, sleeve_summary as sleeve_summary_impl, store_sleeve_review
from research_lab.stability.expectancy_drift import write_expectancy_drift_report
from research_lab.stability.regime_fragility import write_regime_fragility_report
from research_lab.stability.sleeve_stability import write_sleeve_stability_report
from research_lab.storage.duckdb_query import dataset_symbol_summary, load_dataset_snapshot_rows
from research_lab.storage.manifest_io import read_json
from research_lab.storage.paths import resolve_research_uri
from research_lab.storage.paths import ensure_store_layout, research_lab_root
from research_lab.universes.universe_builder import build_universe_snapshot
from research_lab.universes.universe_registry import (
    list_universe_snapshots,
    load_universe_snapshot,
    store_universe_snapshot,
    validate_universe_snapshot,
)
from research_lab.workflows.first_dataset_workflow import (
    build_from_present_csvs as build_from_present_csvs_impl,
    first_dataset_status as first_dataset_status_impl,
)
from research_lab.workflows.first_event_study_workflow import (
    create_standard_event_study as create_standard_event_study_impl,
    run_standard_event_study as run_standard_event_study_impl,
)
from research_lab.workflows.first_api_workflow import (
    build_first_api_evidence as build_first_api_evidence_impl,
    first_api_dataset_status as first_api_dataset_status_impl,
)


def _print(payload: Any) -> None:
    print(json.dumps(payload, indent=2, sort_keys=True))


def _store_root(args: argparse.Namespace) -> Path | None:
    raw = getattr(args, "store_root", None)
    return Path(raw).resolve() if raw else None


def _env_file(args: argparse.Namespace) -> Path | None:
    raw = getattr(args, "env_file", None)
    return Path(raw).resolve() if raw else None


def _apply_env_file(args: argparse.Namespace) -> None:
    env_file = _env_file(args)
    if env_file and env_file.exists() and env_file.name == ".env.local" and not _is_gitignored(env_file):
        raise RuntimeError(f"Provider env file is not gitignored: {env_file}")
    apply_runtime_config(env_file=_env_file(args))


def _validate_api_provider_ready(args: argparse.Namespace) -> None:
    provider = str(getattr(args, "provider", "") or "").strip().lower()
    if provider in {"alpha_vantage", "tiingo", "eodhd"}:
        provider_ready_or_raise(provider=provider, env_file=_env_file(args))


def _is_gitignored(path: Path) -> bool:
    resolved = path.resolve()
    for parent in [resolved.parent, *resolved.parents]:
        ignore_file = parent / ".gitignore"
        if not ignore_file.exists():
            continue
        try:
            relative = resolved.relative_to(parent).as_posix()
        except ValueError:
            continue
        for raw_line in ignore_file.read_text(encoding="utf-8").splitlines():
            pattern = raw_line.strip()
            if not pattern or pattern.startswith("#") or pattern.startswith("!"):
                continue
            if pattern.endswith("/") and relative.startswith(pattern.rstrip("/") + "/"):
                return True
            if pattern == resolved.name or pattern == relative:
                return True
    return False


def _resolve_input(path_text: str) -> Path:
    path = Path(path_text)
    if path.exists():
        return path.resolve()
    lab_path = research_lab_root() / path_text
    if lab_path.exists():
        return lab_path.resolve()
    raise FileNotFoundError(f"Input file not found: {path_text}")


def _audit_success(
    *,
    args: argparse.Namespace,
    entity_type: str,
    entity_id: str,
    action: str,
    new_state_hash: str,
    reason: str,
    metadata: dict[str, Any],
) -> dict[str, Any]:
    return write_audit_event(
        actor=getattr(args, "actor", "Aegis"),
        entity_type=entity_type,
        entity_id=entity_id,
        action=action,
        previous_state_hash="",
        new_state_hash=new_state_hash,
        reason=reason,
        metadata=metadata,
        store_root=_store_root(args),
    )


def create_universe_snapshot(args: argparse.Namespace) -> int:
    store = ensure_store_layout(_store_root(args))
    snapshot = build_universe_snapshot(
        name=args.name,
        version=args.version,
        input_path=_resolve_input(args.input),
        created_by=args.actor,
    )
    registry_row = store_universe_snapshot(snapshot, store_root=store)
    audit = _audit_success(
        args=args,
        entity_type="universe_snapshot",
        entity_id=snapshot["universe_snapshot_id"],
        action="universe_snapshot_created",
        new_state_hash=snapshot["content_hash"],
        reason="Created immutable universe snapshot.",
        metadata={"registry_row": registry_row},
    )
    _print({"snapshot": snapshot, "registry_row": registry_row, "audit_event": audit})
    return 0



EXPANDED_UNIVERSE_SPECS = [
    ("etf_research_expanded_v1.yaml", "etf_research_expanded", "v1"),
    ("oil_energy_research_v1.yaml", "oil_energy_research", "v1"),
    ("credit_rates_research_v1.yaml", "credit_rates_research", "v1"),
    ("volatility_research_v1.yaml", "volatility_research", "v1"),
    ("sector_research_v1.yaml", "sector_research", "v1"),
]


def _store_universe_snapshot_idempotent(snapshot: dict[str, Any], *, store_root: Path | None = None) -> dict[str, Any]:
    try:
        return store_universe_snapshot(snapshot, store_root=store_root)
    except FileExistsError:
        existing = load_universe_snapshot(snapshot["universe_snapshot_id"], store_root=store_root)
        if existing.get("content_hash") != snapshot.get("content_hash"):
            raise
        return {
            "universe_snapshot_id": existing["universe_snapshot_id"],
            "universe_name": existing["universe_name"],
            "universe_version": existing["universe_version"],
            "symbol_count": existing["symbol_count"],
            "content_hash": existing["content_hash"],
            "storage_uri": f"research://universes/{existing['universe_snapshot_id']}",
            "created_at": existing["created_at"],
            "schema_version": existing["schema_version"],
            "already_exists": True,
        }


def create_expanded_universe_snapshots(args: argparse.Namespace) -> int:
    store = ensure_store_layout(_store_root(args))
    snapshots: list[dict[str, Any]] = []
    registry_rows: list[dict[str, Any]] = []
    for filename, name, version in EXPANDED_UNIVERSE_SPECS:
        snapshot = build_universe_snapshot(
            name=name,
            version=version,
            input_path=research_lab_root() / "universes" / filename,
            created_by=args.actor,
        )
        row = _store_universe_snapshot_idempotent(snapshot, store_root=store)
        snapshots.append(snapshot)
        registry_rows.append(row)
    audit = _audit_success(
        args=args,
        entity_type="expanded_universe_snapshots",
        entity_id="packet31_expanded_universes_v1",
        action="expanded_universe_snapshots_created",
        new_state_hash=";".join(snapshot["content_hash"] for snapshot in snapshots),
        reason="Created Packet 31 expanded research universe snapshots without market data or trading permissions.",
        metadata={"universe_snapshot_ids": [snapshot["universe_snapshot_id"] for snapshot in snapshots]},
    )
    _print({"universe_snapshots": snapshots, "registry_rows": registry_rows, "audit_event": audit})
    return 0


def build_breadth_snapshot_cli(args: argparse.Namespace) -> int:
    result = build_and_store_breadth_snapshot(
        dataset_snapshot_id=args.dataset_snapshot_id,
        universe_snapshot_id=args.universe_snapshot_id,
        store_root=_store_root(args),
        actor=args.actor,
    )
    snapshot = result["breadth_snapshot"]
    _print({
        "breadth_snapshot_id": snapshot["breadth_snapshot_id"],
        "dataset_snapshot_id": snapshot["dataset_snapshot_id"],
        "universe_snapshot_id": snapshot["universe_snapshot_id"],
        "metric_count": snapshot["metric_count"],
        "breadth_regimes_present": snapshot["breadth_regimes_present"],
        "registry_row": result["registry_row"],
        "audit_event": result["audit_event"],
    })
    return 0


def import_macro_event_calendar_cli(args: argparse.Namespace) -> int:
    result = import_and_store_macro_event_calendar(input_path=_resolve_input(args.input), store_root=_store_root(args), actor=args.actor)
    snapshot = result["macro_event_calendar_snapshot"]
    _print({
        "macro_event_calendar_snapshot_id": snapshot["macro_event_calendar_snapshot_id"],
        "event_count": snapshot["event_count"],
        "event_types": snapshot["event_types"],
        "registry_row": result["registry_row"],
        "audit_event": result["audit_event"],
    })
    return 0


def reassess_hypothesis_readiness_cli(args: argparse.Namespace) -> int:
    return assess_hypothesis_readiness_cli(args)

def list_universes(args: argparse.Namespace) -> int:
    _print({"universe_snapshots": list_universe_snapshots(store_root=_store_root(args))})
    return 0


def validate_universe(args: argparse.Namespace) -> int:
    try:
        result = validate_universe_snapshot(args.universe_snapshot_id, store_root=_store_root(args))
        action = "universe_snapshot_validated" if result["valid"] else "snapshot_validation_failed"
        audit = write_audit_event(
            actor=args.actor,
            entity_type="universe_snapshot",
            entity_id=args.universe_snapshot_id,
            action=action,
            previous_state_hash="",
            new_state_hash=result["actual_content_hash"],
            reason="Validated immutable universe snapshot manifest.",
            metadata={"validation_result": result},
            store_root=_store_root(args),
        )
        _print({"validation": result, "audit_event": audit})
        return 0 if result["valid"] else 2
    except Exception as exc:
        write_audit_event(
            actor=args.actor,
            entity_type="universe_snapshot",
            entity_id=args.universe_snapshot_id,
            action="snapshot_validation_failed",
            previous_state_hash="",
            new_state_hash="",
            reason=str(exc),
            metadata={},
            store_root=_store_root(args),
        )
        raise


def create_empty_dataset_snapshot(args: argparse.Namespace) -> int:
    store = ensure_store_layout(_store_root(args))
    snapshot = build_empty_dataset_snapshot(
        dataset_type=args.dataset_type,
        provider=args.provider,
        interval=args.interval,
        bar_policy_version=args.bar_policy,
        universe_snapshot_id=args.universe_snapshot_id,
        start_date=args.start,
        end_date=args.end,
        provider_version=args.provider_version,
        created_by=args.actor,
        store_root=store,
    )
    registry_row = store_dataset_snapshot(snapshot, store_root=store)
    audit = _audit_success(
        args=args,
        entity_type="dataset_snapshot",
        entity_id=snapshot["dataset_snapshot_id"],
        action="dataset_snapshot_created",
        new_state_hash=snapshot["content_hash"],
        reason="Created immutable placeholder dataset snapshot envelope.",
        metadata={"registry_row": registry_row, "packet_scope": "metadata_only_no_ohlcv_rows"},
    )
    _print({"snapshot": snapshot, "registry_row": registry_row, "audit_event": audit})
    return 0


def list_datasets(args: argparse.Namespace) -> int:
    _print({"dataset_snapshots": list_dataset_snapshots(store_root=_store_root(args))})
    return 0


def validate_dataset(args: argparse.Namespace) -> int:
    try:
        result = validate_dataset_snapshot(args.dataset_snapshot_id, store_root=_store_root(args))
        action = "dataset_snapshot_validated" if result["valid"] else "snapshot_validation_failed"
        audit = write_audit_event(
            actor=args.actor,
            entity_type="dataset_snapshot",
            entity_id=args.dataset_snapshot_id,
            action=action,
            previous_state_hash="",
            new_state_hash=result["actual_content_hash"],
            reason="Validated immutable dataset snapshot manifest.",
            metadata={"validation_result": result},
            store_root=_store_root(args),
        )
        _print({"validation": result, "audit_event": audit})
        return 0 if result["valid"] else 2
    except Exception as exc:
        write_audit_event(
            actor=args.actor,
            entity_type="dataset_snapshot",
            entity_id=args.dataset_snapshot_id,
            action="snapshot_validation_failed",
            previous_state_hash="",
            new_state_hash="",
            reason=str(exc),
            metadata={},
            store_root=_store_root(args),
        )
        raise


def build_ohlcv_dataset(args: argparse.Namespace) -> int:
    _apply_env_file(args)
    _validate_api_provider_ready(args)
    command = "python -m research_lab.cli " + " ".join(sys.argv[1:])
    result = build_ohlcv_dataset_snapshot(
        dataset_type=args.dataset_type,
        provider_name=args.provider,
        interval=args.interval,
        bar_policy_version=args.bar_policy,
        universe_snapshot_id=args.universe_snapshot_id,
        start_date=args.start,
        end_date=args.end,
        store_root=_store_root(args),
        created_by=args.actor,
        command=command,
        allow_missing_symbols=str(args.allow_missing_symbols).lower() == "true",
        require_readiness_report=str(args.require_readiness_report).lower() == "true",
        readiness_report_path=Path(args.readiness_report_path).resolve() if args.readiness_report_path else None,
    )
    _print(result)
    return 0


def dataset_head(args: argparse.Namespace) -> int:
    rows = load_dataset_snapshot_rows(args.dataset_snapshot_id, store_root=_store_root(args), limit=args.limit)
    _print({"dataset_snapshot_id": args.dataset_snapshot_id, "limit": args.limit, "rows": rows})
    return 0


def dataset_summary(args: argparse.Namespace) -> int:
    _print({"dataset_snapshot_id": args.dataset_snapshot_id, "symbol_summary": dataset_symbol_summary(args.dataset_snapshot_id, store_root=_store_root(args))})
    return 0


def dataset_coverage(args: argparse.Namespace) -> int:
    _print({"dataset_snapshot_id": args.dataset_snapshot_id, "coverage_report": load_coverage_report(args.dataset_snapshot_id, store_root=_store_root(args))})
    return 0


def dependency_health(args: argparse.Namespace) -> int:
    payload = research_dependency_health()
    _print(payload)
    return 0 if payload["status"].startswith("READY") else 4


def provider_diagnostics(args: argparse.Namespace) -> int:
    _apply_env_file(args)
    payload = get_provider_diagnostics(args.provider)
    _print(payload)
    return 0 if payload["available"] else 4


def live_ohlcv_smoke_test(args: argparse.Namespace) -> int:
    _apply_env_file(args)
    _validate_api_provider_ready(args)
    store = ensure_store_layout(_store_root(args))
    write_audit_event(
        actor=args.actor,
        entity_type="ohlcv_live_smoke_test",
        entity_id=args.provider,
        action="ohlcv_live_smoke_test_started",
        reason="Started live OHLCV provider smoke test.",
        metadata={"provider": args.provider, "symbols": args.symbols, "start": args.start, "end": args.end},
        store_root=store,
    )
    try:
        provider = get_daily_ohlcv_provider(args.provider)
        result_rows: dict[str, int] = {}
        samples: dict[str, list[dict[str, Any]]] = {}
        for symbol in args.symbols:
            rows = provider.fetch_daily_ohlcv(symbol, date.fromisoformat(args.start), date.fromisoformat(args.end))
            result_rows[symbol.upper()] = len(rows)
            samples[symbol.upper()] = rows[:2]
        payload = {
            "status": "PASS",
            "provider": args.provider,
            "provider_version": provider.provider_version,
            "symbols": args.symbols,
            "row_counts": result_rows,
            "samples": samples,
            "snapshot_created": False,
            "schema_version": "ohlcv_live_smoke_test.v1",
        }
        write_audit_event(
            actor=args.actor,
            entity_type="ohlcv_live_smoke_test",
            entity_id=args.provider,
            action="ohlcv_live_smoke_test_completed",
            reason="Completed live OHLCV provider smoke test.",
            metadata=payload,
            store_root=store,
        )
        _print(payload)
        return 0
    except Exception as exc:
        payload = {
            "status": "FAIL_CLOSED",
            "provider": args.provider,
            "symbols": args.symbols,
            "error": str(exc),
            "provider_diagnostics": get_provider_diagnostics(args.provider),
            "snapshot_created": False,
            "schema_version": "ohlcv_live_smoke_test.v1",
        }
        write_audit_event(
            actor=args.actor,
            entity_type="ohlcv_live_smoke_test",
            entity_id=args.provider,
            action="ohlcv_live_smoke_test_failed",
            reason=str(exc),
            metadata=payload,
            store_root=store,
        )
        _print(payload)
        return 4


def _parse_forward_windows(value: str) -> list[int]:
    return [int(item.strip()) for item in value.split(",") if item.strip()]


def create_research_plan(args: argparse.Namespace) -> int:
    params: dict[str, Any] = {}
    if args.threshold is not None:
        params["threshold"] = float(args.threshold)
        params["return_column"] = args.return_column
    if args.percentile is not None:
        params["percentile"] = float(args.percentile)
    event_definition = {"type": args.event_type, "params": params}
    plan = build_research_plan(
        hypothesis_id=args.hypothesis_id,
        title=args.title,
        hypothesis=args.hypothesis or args.title,
        dataset_snapshot_id=args.dataset_snapshot_id,
        universe_snapshot_id=args.universe_snapshot_id,
        symbols=args.symbols,
        start=args.start,
        end=args.end,
        event_definition=event_definition,
        forward_return_windows=_parse_forward_windows(args.forward_windows),
        created_by=args.actor,
    )
    registry_row = store_research_plan(plan, store_root=_store_root(args))
    _print({"research_plan": plan, "registry_row": registry_row})
    return 0


def create_backtest_plan(args: argparse.Namespace) -> int:
    params: dict[str, Any] = {}
    if args.threshold is not None:
        params["threshold"] = float(args.threshold)
        params["return_column"] = args.return_column
    if args.percentile is not None:
        params["percentile"] = float(args.percentile)
    plan = build_backtest_plan(
        hypothesis_id=args.hypothesis_id,
        title=args.title,
        dataset_snapshot_id=args.dataset_snapshot_id,
        universe_snapshot_id=args.universe_snapshot_id,
        regime_snapshot_id=args.regime_snapshot_id,
        cost_model_snapshot_id=args.cost_model_snapshot_id,
        symbols=args.symbols,
        start=args.start,
        end=args.end,
        signal_rule={"type": args.signal_type, "params": params},
        holding_period=args.holding_period,
        max_positions=args.max_positions,
        benchmark_symbol=args.benchmark_symbol,
        created_by=args.actor,
    )
    registry_row = store_backtest_plan(plan, store_root=_store_root(args))
    _print({"backtest_plan": plan, "registry_row": registry_row})
    return 0


def run_backtest(args: argparse.Namespace) -> int:
    _print(run_holding_period_backtest(backtest_plan_id=args.backtest_plan_id, store_root=_store_root(args), actor=args.actor))
    return 0


def backtest_summary(args: argparse.Namespace) -> int:
    manifest = load_evidence_package_manifest(args.evidence_package_id, store_root=_store_root(args))
    summary_path = resolve_research_uri(manifest["summary_uri"], store_root=_store_root(args))
    _print({"evidence_manifest": manifest, "performance_summary": read_json(summary_path)})
    return 0


def build_first_backtest_evidence(args: argparse.Namespace) -> int:
    plan = build_backtest_plan(
        hypothesis_id="hyp_etf_drop_reversion_v1",
        title="ETF drop reversion holding-period backtest",
        dataset_snapshot_id=args.dataset_snapshot_id,
        universe_snapshot_id="us_local_etf_minimum_viable_v1_a3ccfc",
        regime_snapshot_id=args.regime_snapshot_id,
        cost_model_snapshot_id=args.cost_model_snapshot_id,
        symbols=["SPY", "QQQ", "IWM", "TLT", "GLD"],
        start="2015-01-01",
        end="2025-12-31",
        signal_rule={"type": "daily_return_below_threshold", "params": {"return_column": "adj_close", "threshold": -0.02}},
        holding_period=5,
        max_positions=5,
        benchmark_symbol="SPY",
        created_by=args.actor,
    )
    registry_row = store_backtest_plan(plan, store_root=_store_root(args))
    result = run_holding_period_backtest(backtest_plan_id=plan["backtest_plan_id"], store_root=_store_root(args), actor=args.actor)
    _print({"backtest_plan": plan, "plan_registry_row": registry_row, "backtest_result": result})
    return 0


def generate_candidates(args: argparse.Namespace) -> int:
    try:
        result = generate_drop_reversion_candidates(
            source_evidence_package_id=args.source_evidence_package_id,
            dataset_snapshot_id=args.dataset_snapshot_id,
            regime_snapshot_id=args.regime_snapshot_id,
            cost_model_snapshot_id=args.cost_model_snapshot_id,
            as_of_date=args.as_of_date,
            threshold=float(args.threshold),
            created_by=args.actor,
            store_root=_store_root(args),
        )
        registry_row = store_candidate_batch(
            candidate_batch=result["candidate_batch"],
            candidates=result["candidates"],
            generation_summary=result["generation_summary"],
            store_root=_store_root(args),
            actor=args.actor,
        )
        _print({**result, "registry_row": registry_row})
        return 0
    except Exception as exc:
        write_audit_event(
            actor=args.actor,
            entity_type="candidate_batch",
            entity_id=args.source_evidence_package_id,
            action="candidate_batch_failed",
            reason=str(exc),
            metadata={
                "source_evidence_package_id": args.source_evidence_package_id,
                "dataset_snapshot_id": args.dataset_snapshot_id,
                "as_of_date": args.as_of_date,
            },
            store_root=_store_root(args),
        )
        raise


def list_candidates(args: argparse.Namespace) -> int:
    _print(
        {
            "candidate_batch": load_candidate_batch(args.candidate_batch_id, store_root=_store_root(args)),
            "candidates": candidates_with_latest_status(args.candidate_batch_id, store_root=_store_root(args)),
        }
    )
    return 0


def record_operator_decision(args: argparse.Namespace) -> int:
    found = None
    for batch in candidates_with_latest_status(args.candidate_batch_id, store_root=_store_root(args)) if args.candidate_batch_id else []:
        if batch["candidate_id"] == args.candidate_id:
            found = batch
            break
    if found is None and not args.candidate_batch_id:
        from research_lab.candidates.candidate_registry import list_candidate_batches

        for row in list_candidate_batches(store_root=_store_root(args)):
            for candidate in candidates_with_latest_status(row["candidate_batch_id"], store_root=_store_root(args)):
                if candidate["candidate_id"] == args.candidate_id:
                    found = candidate
                    break
            if found:
                break
    if found is None:
        raise RuntimeError(f"Candidate not found: {args.candidate_id}")
    decision = build_operator_decision(
        candidate_id=args.candidate_id,
        candidate_batch_id=found["candidate_batch_id"],
        decision=args.decision,
        decision_reason=args.reason,
        decided_by=args.decided_by,
        portfolio_context_snapshot_id=args.portfolio_context_snapshot_id,
        notes=args.notes,
    )
    _print({"operator_decision": append_operator_decision(decision, store_root=_store_root(args), actor=args.decided_by)})
    return 0


def candidate_ledger_summary(args: argparse.Namespace) -> int:
    _print(candidate_ledger_summary_impl(args.candidate_batch_id, store_root=_store_root(args)))
    return 0


def _parse_windows(value: str) -> list[int]:
    return [int(item.strip()) for item in value.split(",") if item.strip()]


def measure_candidate_outcomes_cli(args: argparse.Namespace) -> int:
    result = run_candidate_outcome_measurement(
        candidate_batch_id=args.candidate_batch_id,
        dataset_snapshot_id=args.dataset_snapshot_id,
        cost_model_snapshot_id=args.cost_model_snapshot_id,
        benchmark_symbol=args.benchmark_symbol,
        windows=_parse_windows(args.windows),
        store_root=_store_root(args),
        actor=args.actor,
    )
    _print(result)
    return 0


def candidate_attribution_summary(args: argparse.Namespace) -> int:
    _print(build_candidate_outcome_summary(args.candidate_batch_id, store_root=_store_root(args)))
    return 0


def list_outcomes(args: argparse.Namespace) -> int:
    _print({"candidate_batch_id": args.candidate_batch_id, "outcomes": load_outcomes(args.candidate_batch_id, store_root=_store_root(args))})
    return 0


def create_sleeve(args: argparse.Namespace) -> int:
    try:
        definition = build_sleeve_definition(
            sleeve_id=args.sleeve_id,
            name=args.name,
            hypothesis_id=args.hypothesis_id,
            sleeve_type=args.sleeve_type,
            description=args.description,
            created_by=args.actor,
        )
        registry_row = store_sleeve_definition(definition, store_root=_store_root(args), actor=args.actor)
        _print({"sleeve_definition": definition, "registry_row": registry_row})
        return 0
    except Exception as exc:
        write_audit_event(actor=args.actor, entity_type="sleeve", entity_id=args.sleeve_id, action="sleeve_governance_failed", reason=str(exc), metadata={"command": "create-sleeve"}, store_root=_store_root(args))
        raise


def create_sleeve_version_cli(args: argparse.Namespace) -> int:
    try:
        version = build_sleeve_version(
            sleeve_id=args.sleeve_id,
            version=args.version,
            hypothesis_id=args.hypothesis_id,
            event_study_evidence_package_id=args.event_study_evidence_package_id,
            backtest_evidence_package_id=args.backtest_evidence_package_id,
            candidate_batch_id=args.candidate_batch_id,
            dataset_snapshot_id=args.dataset_snapshot_id,
            regime_snapshot_id=args.regime_snapshot_id,
            cost_model_snapshot_id=args.cost_model_snapshot_id,
            created_by=args.actor,
        )
        registry_row = store_sleeve_version(version, store_root=_store_root(args), actor=args.actor)
        _print({"sleeve_version": version, "registry_row": registry_row})
        return 0
    except Exception as exc:
        write_audit_event(actor=args.actor, entity_type="sleeve_version", entity_id=args.sleeve_id, action="sleeve_governance_failed", reason=str(exc), metadata={"command": "create-sleeve-version"}, store_root=_store_root(args))
        raise


def evaluate_sleeve_health(args: argparse.Namespace) -> int:
    snapshot = build_sleeve_health_snapshot(sleeve_id=args.sleeve_id, sleeve_version_id=args.sleeve_version_id, as_of_date=args.as_of_date, store_root=_store_root(args))
    registry_row = store_sleeve_health_snapshot(snapshot, store_root=_store_root(args), actor=args.actor)
    _print({"sleeve_health_snapshot": snapshot, "registry_row": registry_row})
    return 0


def challenge_sleeve(args: argparse.Namespace) -> int:
    challenge = build_sleeve_challenge(sleeve_id=args.sleeve_id, sleeve_version_id=args.sleeve_version_id, sleeve_health_snapshot_id=args.sleeve_health_snapshot_id, created_by=args.actor, store_root=_store_root(args))
    registry_row = store_sleeve_challenge(challenge, store_root=_store_root(args), actor=args.actor)
    _print({"sleeve_challenge": challenge, "registry_row": registry_row})
    return 0


def record_sleeve_review(args: argparse.Namespace) -> int:
    review = build_sleeve_review(
        sleeve_id=args.sleeve_id,
        sleeve_version_id=args.sleeve_version_id,
        sleeve_challenge_id=args.sleeve_challenge_id,
        review_decision=args.decision,
        review_reason=args.reason,
        reviewed_by=args.reviewed_by or args.actor,
        store_root=_store_root(args),
    )
    registry_row = store_sleeve_review(review, store_root=_store_root(args), actor=args.reviewed_by)
    _print({"sleeve_review": review, "registry_row": registry_row})
    return 0


def sleeve_summary_cli(args: argparse.Namespace) -> int:
    _print(sleeve_summary_impl(args.sleeve_id, store_root=_store_root(args)))
    return 0


def run_longitudinal_candidate_study_cli(args: argparse.Namespace) -> int:
    result = run_longitudinal_candidate_study(
        sleeve_id=args.sleeve_id,
        sleeve_version_id=args.sleeve_version_id,
        source_evidence_package_id=args.source_evidence_package_id,
        dataset_snapshot_id=args.dataset_snapshot_id,
        regime_snapshot_id=args.regime_snapshot_id,
        cost_model_snapshot_id=args.cost_model_snapshot_id,
        start=args.start,
        end=args.end,
        frequency=args.frequency,
        threshold=float(args.threshold),
        outcome_windows=_parse_windows(args.outcome_windows),
        store_root=_store_root(args),
        actor=args.actor,
    )
    _print(result)
    return 0


def ranking_quality_report_cli(args: argparse.Namespace) -> int:
    _print(write_ranking_quality_report(args.longitudinal_run_id, store_root=_store_root(args), actor=args.actor))
    return 0


def sleeve_learning_report_cli(args: argparse.Namespace) -> int:
    _print(write_sleeve_learning_report(args.sleeve_id, args.sleeve_version_id, store_root=_store_root(args), actor=args.actor))
    return 0


def expectancy_drift_report_cli(args: argparse.Namespace) -> int:
    report = write_expectancy_drift_report(
        sleeve_id=args.sleeve_id,
        sleeve_version_id=args.sleeve_version_id,
        longitudinal_run_id=args.longitudinal_run_id,
        rolling_window_size=args.rolling_window_size,
        store_root=_store_root(args),
        actor=args.actor,
    )
    _print(report)
    return 0


def regime_fragility_report_cli(args: argparse.Namespace) -> int:
    report = write_regime_fragility_report(
        sleeve_id=args.sleeve_id,
        sleeve_version_id=args.sleeve_version_id,
        longitudinal_run_id=args.longitudinal_run_id,
        store_root=_store_root(args),
        actor=args.actor,
    )
    _print(report)
    return 0


def sleeve_stability_report_cli(args: argparse.Namespace) -> int:
    report = write_sleeve_stability_report(
        sleeve_id=args.sleeve_id,
        sleeve_version_id=args.sleeve_version_id,
        expectancy_drift_report_id=args.expectancy_drift_report_id,
        regime_fragility_report_id=args.regime_fragility_report_id,
        store_root=_store_root(args),
        actor=args.actor,
    )
    _print(report)
    return 0


def stability_reports_cli(args: argparse.Namespace) -> int:
    drift = write_expectancy_drift_report(
        sleeve_id=args.sleeve_id,
        sleeve_version_id=args.sleeve_version_id,
        longitudinal_run_id=args.longitudinal_run_id,
        rolling_window_size=args.rolling_window_size,
        store_root=_store_root(args),
        actor=args.actor,
    )
    fragility = write_regime_fragility_report(
        sleeve_id=args.sleeve_id,
        sleeve_version_id=args.sleeve_version_id,
        longitudinal_run_id=args.longitudinal_run_id,
        store_root=_store_root(args),
        actor=args.actor,
    )
    stability = write_sleeve_stability_report(
        sleeve_id=args.sleeve_id,
        sleeve_version_id=args.sleeve_version_id,
        expectancy_drift_report_id=drift["expectancy_drift_report_id"],
        regime_fragility_report_id=fragility["regime_fragility_report_id"],
        store_root=_store_root(args),
        actor=args.actor,
    )
    _print(
        {
            "sleeve_id": args.sleeve_id,
            "expectancy_drift_report_id": drift["expectancy_drift_report_id"],
            "drift_status": drift["drift_status"],
            "regime_fragility_report_id": fragility["regime_fragility_report_id"],
            "fragility_status": fragility["fragility_status"],
            "sleeve_stability_report_id": stability["sleeve_stability_report_id"],
            "overall_stability_status": stability["overall_stability_status"],
            "recommended_action": stability["recommended_action"],
            "key_findings": stability["key_findings"],
            "research_label_present": bool(drift.get("research_label") and fragility.get("research_label") and stability.get("research_label")),
        }
    )
    return 0


def challenger_research_track_cli(args: argparse.Namespace) -> int:
    result = write_challenger_research_track(
        sleeve_id=args.sleeve_id,
        sleeve_stability_report_id=args.sleeve_stability_report_id,
        store_root=_store_root(args),
        actor=args.actor,
    )
    track = result["track"]
    _print(
        {
            "challenger_track_id": track["challenger_track_id"],
            "incumbent_sleeve_id": track["incumbent_sleeve_id"],
            "trigger_report_ids": track["trigger_report_ids"],
            "hypothesis_count": len(track["challenger_hypothesis_set"]),
            "status": track["status"],
            "recommended_next_action": track["recommended_next_action"],
            "research_label_present": bool(track.get("research_label") == "RESEARCH_ONLY"),
            "immutable_registry_entry_present": bool(result.get("registry_row")),
            "json_path": result["json_path"],
            "markdown_path": result["markdown_path"],
        }
    )
    return 0


def challenger_evidence_cli(args: argparse.Namespace) -> int:
    result = write_challenger_evidence_batch(
        challenger_track_id=args.challenger_track_id,
        store_root=_store_root(args),
        actor=args.actor,
    )
    batch = result["batch"]
    _print(
        {
            "challenger_evidence_batch_id": batch["challenger_evidence_batch_id"],
            "challenger_track_id": batch["challenger_track_id"],
            "incumbent_sleeve_id": batch["incumbent_sleeve_id"],
            "total_hypotheses": len(batch["source_challenger_hypothesis_ids"]),
            "generated_item_count": sum(1 for item in batch["challenger_evidence_items"] if item["status"] == "generated"),
            "blocked_hypothesis_count": len(batch["blocked_hypotheses"]),
            "evidence_quality_summary": evidence_quality_summary(batch),
            "recommended_next_action": batch["recommended_next_action"],
            "research_label_present": bool(batch.get("research_label") == "RESEARCH_ONLY"),
            "immutable_registry_entry_present": bool(result.get("registry_row")),
            "json_path": result["json_path"],
        }
    )
    return 0


def challenger_comparison_cli(args: argparse.Namespace) -> int:
    result = write_challenger_comparison_report(
        challenger_evidence_batch_id=args.challenger_evidence_batch_id,
        challenger_track_id=args.challenger_track_id,
        store_root=_store_root(args),
        actor=args.actor,
    )
    report = result["report"]
    active = [row for row in report["comparison_table"] if row["entity_type"] == "challenger" and row["exclusion_status"] == "included_for_human_review"]
    excluded = [row for row in report["comparison_table"] if row["entity_type"] != "incumbent" and row["exclusion_status"] == "excluded"]
    _print(
        {
            "challenger_comparison_report_id": report["challenger_comparison_report_id"],
            "challenger_track_id": report["challenger_track_id"],
            "challenger_evidence_batch_id": report["challenger_evidence_batch_id"],
            "incumbent_sleeve_id": report["incumbent_sleeve_id"],
            "total_comparison_rows": len(report["comparison_table"]),
            "active_challenger_count": len(active),
            "excluded_challenger_count": len(excluded),
            "evidence_sufficiency": report["evidence_sufficiency"],
            "top_research_review_candidate_id": report["deterministic_rank_order"][0]["challenger_hypothesis_id"] if report["deterministic_rank_order"] else "",
            "recommended_next_action": report["recommended_next_action"],
            "research_label_present": bool(report.get("research_label") == "RESEARCH_ONLY"),
            "immutable_registry_entry_present": bool(result.get("registry_row")),
            "json_path": result["json_path"],
        }
    )
    return 0


def human_review_dossier_cli(args: argparse.Namespace) -> int:
    result = write_human_review_dossier(
        challenger_comparison_report_id=args.challenger_comparison_report_id,
        store_root=_store_root(args),
        actor=args.actor,
    )
    dossier = result["dossier"]
    _print(
        {
            "human_review_dossier_id": dossier["human_review_dossier_id"],
            "dossier_type": dossier["dossier_type"],
            "incumbent_sleeve_id": dossier["incumbent_sleeve_id"],
            "challenger_track_id": dossier["challenger_track_id"],
            "challenger_comparison_report_id": dossier["challenger_comparison_report_id"],
            "active_review_candidate_count": len(dossier["review_candidates"]),
            "blocked_or_excluded_count": len(dossier["blocked_or_excluded_items"]),
            "top_research_review_candidate_id": dossier["executive_summary"]["top_research_review_candidate_id"],
            "recommended_next_action": dossier["recommended_next_action"],
            "research_label_present": bool(dossier.get("research_label") == "RESEARCH_ONLY"),
            "immutable_registry_entry_present": bool(result.get("registry_row")),
            "json_path": result["json_path"],
        }
    )
    return 0


def create_paper_trial(args: argparse.Namespace) -> int:
    trial = build_paper_trial(
        sleeve_id=args.sleeve_id,
        sleeve_version_id=args.sleeve_version_id,
        hypothesis_id=args.hypothesis_id,
        dataset_snapshot_id=args.dataset_snapshot_id,
        regime_snapshot_id=args.regime_snapshot_id,
        cost_model_snapshot_id=args.cost_model_snapshot_id,
        source_evidence_package_ids=[item.strip() for item in args.source_evidence_package_ids.split(",") if item.strip()],
        threshold=float(args.threshold),
        observation_frequency=args.observation_frequency,
        outcome_windows=_parse_windows(args.outcome_windows),
        created_by=args.actor,
    )
    row = store_paper_trial(trial, store_root=_store_root(args), actor=args.actor)
    _print({"paper_trial": trial, "registry_row": row})
    return 0


def activate_paper_trial(args: argparse.Namespace) -> int:
    _print({"status_event": change_paper_trial_status(args.paper_trial_id, "active", store_root=_store_root(args), actor=args.actor)})
    return 0


def record_paper_observation(args: argparse.Namespace) -> int:
    _print({"paper_trial_observation": record_paper_observation_impl(paper_trial_id=args.paper_trial_id, as_of_date=args.as_of_date, operator_note=args.operator_note, store_root=_store_root(args), actor=args.actor)})
    return 0


def measure_paper_outcomes(args: argparse.Namespace) -> int:
    _print({"paper_trial_outcome": measure_paper_outcomes_impl(paper_trial_id=args.paper_trial_id, paper_trial_observation_id=args.paper_trial_observation_id, store_root=_store_root(args), actor=args.actor)})
    return 0


def review_paper_trial(args: argparse.Namespace) -> int:
    _print({"paper_trial_review": record_paper_trial_review(paper_trial_id=args.paper_trial_id, review_decision=args.decision, review_reason=args.reason, reviewed_by=args.reviewed_by or args.actor, store_root=_store_root(args))})
    return 0


def paper_trial_summary(args: argparse.Namespace) -> int:
    _print(build_paper_trial_summary(args.paper_trial_id, store_root=_store_root(args)))
    return 0


def record_next_paper_observation_cli(args: argparse.Namespace) -> int:
    _print(
        record_next_paper_observation_impl(
            paper_trial_id=args.paper_trial_id,
            as_of_date=args.as_of_date,
            operator_note=args.operator_note,
            store_root=_store_root(args),
            actor=args.actor,
        )
    )
    return 0


def list_paper_due_outcomes_cli(args: argparse.Namespace) -> int:
    _print(
        list_paper_due_outcomes_impl(
            paper_trial_id=args.paper_trial_id,
            as_of_date=args.as_of_date,
            store_root=_store_root(args),
            actor=args.actor,
        )
    )
    return 0


def measure_due_paper_outcomes_cli(args: argparse.Namespace) -> int:
    _print(
        measure_due_paper_outcomes_impl(
            paper_trial_id=args.paper_trial_id,
            as_of_date=args.as_of_date,
            store_root=_store_root(args),
            actor=args.actor,
        )
    )
    return 0


def paper_trial_ops_report_cli(args: argparse.Namespace) -> int:
    report = write_paper_trial_operations_report(
        args.paper_trial_id,
        as_of_date=args.as_of_date,
        store_root=_store_root(args),
        actor=args.actor,
    )
    _print(
        {
            "paper_trial_id": report["paper_trial_id"],
            "status": report["status"],
            "observation_count": report["observation_count"],
            "candidate_count_total": report["candidate_count_total"],
            "measured_candidate_count": report["measured_candidate_count"],
            "pending_due_outcomes": report["pending_due_outcomes"],
            "due_outcomes": report["due_outcomes"],
            "recommended_next_action": report["recommended_next_action"],
            "next_allowed_actions": report["next_allowed_actions"],
            "operations_report": report,
        }
    )
    return 0


def build_evidence_inventory_cli(args: argparse.Namespace) -> int:
    _print(write_evidence_inventory(store_root=_store_root(args), actor=args.actor))
    return 0


def build_paper_trial_inventory_cli(args: argparse.Namespace) -> int:
    _print(write_paper_trial_inventory(store_root=_store_root(args), actor=args.actor))
    return 0


def compare_sleeves_cli(args: argparse.Namespace) -> int:
    _print(write_sleeve_comparison_report(store_root=_store_root(args), actor=args.actor))
    return 0


def research_backlog_priority_cli(args: argparse.Namespace) -> int:
    _print(write_research_backlog_priority_report(store_root=_store_root(args), actor=args.actor))
    return 0


def build_portfolio_evidence_reports_cli(args: argparse.Namespace) -> int:
    evidence = write_evidence_inventory(store_root=_store_root(args), actor=args.actor)
    paper = write_paper_trial_inventory(store_root=_store_root(args), actor=args.actor)
    comparison = write_sleeve_comparison_report(store_root=_store_root(args), actor=args.actor)
    backlog = write_research_backlog_priority_report(store_root=_store_root(args), actor=args.actor)
    _print(
        {
            "evidence_inventory_id": evidence["evidence_inventory_id"],
            "paper_trial_inventory_id": paper["paper_trial_inventory_id"],
            "sleeve_comparison_report_id": comparison["sleeve_comparison_report_id"],
            "research_backlog_priority_report_id": backlog["research_backlog_priority_report_id"],
            "sleeve_count": evidence["sleeve_count"],
            "active_paper_trials": paper["active_trials"],
            "top_priority_sleeve_id": backlog["priority_rows"][0]["sleeve_id"] if backlog["priority_rows"] else "",
            "top_priority_bucket": backlog["priority_rows"][0]["priority_bucket"] if backlog["priority_rows"] else "",
            "top_recommended_next_action": backlog["priority_rows"][0]["recommended_next_action"] if backlog["priority_rows"] else "",
            "missing_artifacts_count": sum(len(row.get("missing_artifacts") or []) for row in evidence["sleeves"]),
            "research_label_present": bool(evidence.get("research_label") and paper.get("research_label") and comparison.get("research_label") and backlog.get("research_label")),
        }
    )
    return 0


def run_event_study(args: argparse.Namespace) -> int:
    result = run_event_study_runner(
        research_plan_id=args.research_plan_id,
        store_root=_store_root(args),
        actor=args.actor,
        regime_snapshot_id=getattr(args, "regime_snapshot_id", None),
        cost_model_snapshot_id=getattr(args, "cost_model_snapshot_id", None),
    )
    _print(result)
    return 0


def list_evidence(args: argparse.Namespace) -> int:
    _print({"evidence_packages": list_evidence_packages(store_root=_store_root(args))})
    return 0


def evidence_summary(args: argparse.Namespace) -> int:
    manifest = load_evidence_package_manifest(args.evidence_package_id, store_root=_store_root(args))
    summary_path = resolve_research_uri(manifest["summary_uri"], store_root=_store_root(args))
    _print({"evidence_manifest": manifest, "summary": read_json(summary_path)})
    return 0


def build_regime_snapshot(args: argparse.Namespace) -> int:
    result = build_regime_snapshot_impl(
        dataset_snapshot_id=args.dataset_snapshot_id,
        benchmark_symbol=args.benchmark_symbol,
        store_root=_store_root(args),
        created_by=args.actor,
    )
    _print(result)
    return 0


def create_default_cost_model(args: argparse.Namespace) -> int:
    result = create_default_cost_model_snapshot_impl(store_root=_store_root(args), actor=args.actor)
    _print(result)
    return 0


def compare_evidence(args: argparse.Namespace) -> int:
    package_ids = [item.strip() for item in args.evidence_package_ids.split(",") if item.strip()]
    comparison = compare_evidence_packages(package_ids, store_root=_store_root(args))
    if args.output:
        write_evidence_comparison(comparison, Path(args.output))
    _print(comparison)
    return 0


def create_csv_template(args: argparse.Namespace) -> int:
    result = create_csv_template_impl(symbol=args.symbol, output_path=Path(args.output))
    _print(result)
    return 0


def expected_csv_files(args: argparse.Namespace) -> int:
    report = expected_csv_files_report(
        universe_snapshot_id=args.universe_snapshot_id,
        csv_root=Path(args.csv_root),
        store_root=_store_root(args),
    )
    if args.output:
        Path(args.output).parent.mkdir(parents=True, exist_ok=True)
        Path(args.output).write_text(json.dumps(report, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    _print(report)
    return 0


def validate_local_csvs(args: argparse.Namespace) -> int:
    csv_root = Path(args.csv_root)
    report = build_csv_readiness_report(
        universe_snapshot_id=args.universe_snapshot_id,
        csv_root=csv_root,
        start=args.start,
        end=args.end,
        allow_missing_symbols=str(args.allow_missing_symbols).lower() == "true",
        store_root=_store_root(args),
    )
    output_path = Path(args.output) if args.output else csv_root / "csv_readiness_report.json"
    write_csv_readiness_report(report, output_path)
    _print({"csv_readiness_report": report, "output_path": str(output_path)})
    return 0 if report["ready_to_build"] else 4


def import_local_csvs(args: argparse.Namespace) -> int:
    report = import_local_csvs_impl(
        source_dir=Path(args.source_dir),
        csv_root=Path(args.csv_root),
        universe_snapshot_id=args.universe_snapshot_id,
        mode=args.mode,
        overwrite=str(args.overwrite).lower() == "true",
        store_root=_store_root(args),
    )
    _print(report)
    return 0


def first_dataset_status(args: argparse.Namespace) -> int:
    _print(first_dataset_status_impl(csv_root=Path(args.csv_root), store_root=_store_root(args), actor=args.actor))
    return 0


def build_from_present_csvs(args: argparse.Namespace) -> int:
    result = build_from_present_csvs_impl(
        universe=args.universe,
        csv_root=Path(args.csv_root),
        start=args.start,
        end=args.end,
        store_root=_store_root(args),
        actor=args.actor,
    )
    _print(result)
    return 0


def create_standard_event_study(args: argparse.Namespace) -> int:
    result = create_standard_event_study_impl(
        dataset_snapshot_id=args.dataset_snapshot_id,
        hypothesis_id=args.hypothesis_id,
        threshold=float(args.threshold),
        forward_windows=_parse_forward_windows(args.forward_windows),
        store_root=_store_root(args),
        actor=args.actor,
    )
    _print(result)
    return 0


def run_standard_event_study(args: argparse.Namespace) -> int:
    _print(run_standard_event_study_impl(research_plan_id=args.research_plan_id, store_root=_store_root(args), actor=args.actor))
    return 0


def csv_download_guide(args: argparse.Namespace) -> int:
    _print(
        csv_download_guide_impl(
            universe=args.universe,
            csv_root=Path(args.csv_root),
            store_root=_store_root(args),
            actor=args.actor,
        )
    )
    return 0


def verify_downloaded_csvs(args: argparse.Namespace) -> int:
    result = verify_downloaded_csvs_impl(
        universe=args.universe,
        csv_root=Path(args.csv_root),
        store_root=_store_root(args),
        actor=args.actor,
    )
    _print(result)
    return 0 if result["ready_for_dataset_build"] else 4


def first_api_dataset_status(args: argparse.Namespace) -> int:
    _apply_env_file(args)
    _print(
        first_api_dataset_status_impl(
            provider=args.provider,
            universe=args.universe,
            store_root=_store_root(args),
            actor=args.actor,
            env_file=_env_file(args),
        )
    )
    return 0


def build_first_api_evidence(args: argparse.Namespace) -> int:
    _apply_env_file(args)
    result = build_first_api_evidence_impl(
        provider=args.provider,
        universe=args.universe,
        start=args.start,
        end=args.end,
        threshold=float(args.threshold),
        forward_windows=_parse_forward_windows(args.forward_windows),
        store_root=_store_root(args),
        actor=args.actor,
        env_file=_env_file(args),
    )
    _print(result)
    return 0


def provider_config_status(args: argparse.Namespace) -> int:
    payload = provider_config_status_impl(env_file=_env_file(args))
    _print(payload)
    return 0 if any(row["ready"] for row in payload["providers"] if row["provider"] in {"alpha_vantage", "tiingo"}) else 4


def create_provider_env_template(args: argparse.Namespace) -> int:
    output = Path(args.output).resolve()
    if output.exists() and str(args.overwrite).lower() != "true":
        raise FileExistsError(f"Refusing to overwrite provider env file: {output}")
    template = research_lab_root() / "config" / "providers.example.env"
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(template.read_text(encoding="utf-8"), encoding="utf-8")
    gitignored = _is_gitignored(output)
    if not gitignored:
        output.unlink(missing_ok=True)
        raise RuntimeError(f"Provider env output is not gitignored: {output}")
    payload = {
        "output": str(output),
        "created": True,
        "gitignored": gitignored,
        "contains_api_keys": False,
        "masked_keys_only": True,
        "next_command": f"research_lab/.venv/bin/python -m research_lab.cli provider-config-status --env-file {output}",
    }
    _print(payload)
    return 0



def seed_event_families(args: argparse.Namespace) -> int:
    result = seed_event_families_impl(store_root=_store_root(args), actor=args.actor)
    _print(
        {
            "event_family_count": len(result["event_families"]),
            "event_family_ids": [row["event_family_id"] for row in result["event_families"]],
            "audit_event": result["audit_event"],
        }
    )
    return 0


def capture_event_observation(args: argparse.Namespace) -> int:
    result = capture_event_observation_impl(
        store_root=_store_root(args),
        actor=args.actor,
        event_family_id=args.event_family,
        title=args.title,
        description=args.description,
        source_type=args.source_type,
        source_ref=args.source_ref,
        symbols_mentioned=args.symbols,
        market_context={},
        suspected_mechanism=args.suspected_mechanism,
        confidence_level=args.confidence_level,
        research_priority=args.research_priority,
        notes=args.notes,
    )
    observation = result["event_observation"]
    _print(
        {
            "event_observation_id": observation["event_observation_id"],
            "event_family_id": observation["event_family_id"],
            "confidence_level": observation["confidence_level"],
            "research_priority": observation["research_priority"],
            "notes": observation["notes"],
            "registry_row": result["registry_row"],
            "audit_event": result["audit_event"],
        }
    )
    return 0


def create_event_cluster_cli(args: argparse.Namespace) -> int:
    result = create_event_cluster_impl(
        event_family_id=args.event_family,
        observation_ids=args.observation_ids,
        cluster_title=args.cluster_title,
        cluster_description=args.cluster_description,
        store_root=_store_root(args),
        actor=args.actor,
    )
    cluster = result["event_cluster"]
    _print(
        {
            "event_cluster_id": cluster["event_cluster_id"],
            "event_family_id": cluster["event_family_id"],
            "sample_observation_count": cluster["sample_observation_count"],
            "data_requirement_status": cluster["data_requirement_status"],
            "registry_row": result["registry_row"],
            "audit_event": result["audit_event"],
        }
    )
    return 0


def generate_intent_candidate_cli(args: argparse.Namespace) -> int:
    result = generate_intent_candidate_impl(event_cluster_id=args.event_cluster_id, store_root=_store_root(args), actor=args.actor)
    intent = result["intent_candidate"]
    _print(
        {
            "intent_candidate_id": intent["intent_candidate_id"],
            "event_cluster_id": intent["event_cluster_id"],
            "event_family_id": intent["event_family_id"],
            "candidate_edge_type": intent["candidate_edge_type"],
            "expected_fragility": intent["expected_fragility"],
            "registry_row": result["registry_row"],
            "audit_event": result["audit_event"],
        }
    )
    return 0


def generate_hypothesis_proposal_cli(args: argparse.Namespace) -> int:
    result = generate_hypothesis_proposal_impl(intent_candidate_id=args.intent_candidate_id, store_root=_store_root(args), actor=args.actor)
    proposal = result["hypothesis_proposal"]
    _print(
        {
            "hypothesis_proposal_id": proposal["hypothesis_proposal_id"],
            "intent_candidate_id": proposal["intent_candidate_id"],
            "event_family_id": proposal["event_family_id"],
            "proposal_status": proposal["proposal_status"],
            "governance_classification": proposal["governance_classification"],
            "data_requirement_status": proposal["data_requirement_status"],
            "research_label_present": proposal.get("research_label") == "RESEARCH_ONLY",
            "registry_row": result["registry_row"],
            "audit_event": result["audit_event"],
        }
    )
    return 0


def list_hypothesis_proposals_cli(args: argparse.Namespace) -> int:
    _print(hypothesis_proposal_queue_impl(status=args.status, store_root=_store_root(args), actor=args.actor, audit_view=False))
    return 0


def hypothesis_proposal_queue_cli(args: argparse.Namespace) -> int:
    _print(hypothesis_proposal_queue_impl(status=args.status, store_root=_store_root(args), actor=args.actor, audit_view=True))
    return 0


def assess_hypothesis_readiness_cli(args: argparse.Namespace) -> int:
    result = assess_hypothesis_readiness_impl(hypothesis_proposal_id=args.hypothesis_proposal_id, store_root=_store_root(args), actor=args.actor)
    assessment = result["readiness_assessment"]
    _print({
        "research_readiness_assessment_id": assessment["research_readiness_assessment_id"],
        "hypothesis_proposal_id": assessment["hypothesis_proposal_id"],
        "ready_for_research": assessment["ready_for_research"],
        "blocking_items": assessment["blocking_items"],
        "available_symbols": assessment.get("available_symbols", []),
        "missing_symbols": assessment["missing_symbols"],
        "breadth_snapshot_available": assessment.get("breadth_snapshot_available", False),
        "macro_event_calendar_available": assessment.get("macro_event_calendar_available", False),
        "next_allowed_actions": assessment["next_allowed_actions"],
        "registry_row": result["registry_row"],
        "audit_event": result["audit_event"],
    })
    return 0


def build_research_intake_dossier_cli(args: argparse.Namespace) -> int:
    result = build_research_intake_dossier_impl(hypothesis_proposal_id=args.hypothesis_proposal_id, store_root=_store_root(args), actor=args.actor)
    dossier = result["research_intake_dossier"]
    _print({
        "research_intake_dossier_id": dossier["research_intake_dossier_id"],
        "hypothesis_proposal_id": dossier["hypothesis_proposal_id"],
        "readiness_assessment_id": dossier["readiness_assessment_id"],
        "proposal_priority_score_id": dossier["proposal_priority_score_id"],
        "recommended_next_action": dossier["recommended_next_action"],
        "research_label_present": bool(dossier.get("research_label")),
        "json_path": result["json_path"],
        "markdown_path": result["markdown_path"],
    })
    return 0


def review_hypothesis_proposal_cli(args: argparse.Namespace) -> int:
    result = review_hypothesis_proposal_impl(
        hypothesis_proposal_id=args.hypothesis_proposal_id,
        decision=args.decision,
        reason=args.reason,
        reviewed_by=args.reviewed_by or args.actor,
        store_root=_store_root(args),
        actor=args.actor,
    )
    _print(result)
    return 0


def convert_hypothesis_proposal_to_research_plan_cli(args: argparse.Namespace) -> int:
    result = convert_hypothesis_proposal_to_research_plan_impl(
        hypothesis_proposal_id=args.hypothesis_proposal_id,
        approve=str(args.approve).lower() == "true",
        dataset_snapshot_id=args.dataset_snapshot_id,
        universe_snapshot_id=args.universe_snapshot_id,
        start=args.start,
        end=args.end,
        store_root=_store_root(args),
        actor=args.actor,
    )
    _print(result)
    return 0



def recover_hypotheses_cli(args: argparse.Namespace) -> int:
    result = recover_hypotheses_impl(
        dry_run=str(args.dry_run).lower() == "true",
        seed_defaults=str(args.seed_defaults).lower() == "true",
        store_root=_store_root(args),
        actor=args.actor,
    )
    report = result["report"]
    _print({
        "ok": result["ok"],
        "recovery_run_id": report["recovery_run_id"],
        "dry_run": report["dry_run"],
        "items_discovered": report["items_discovered"],
        "canonical_before_count": report["canonical_before_count"],
        "canonical_after_count": report["canonical_after_count"],
        "recovered_count": report["recovered_count"],
        "seeded_default_count": report["seeded_default_count"],
        "duplicate_count": report["duplicate_count"],
        "needs_operator_review_count": report["needs_operator_review_count"],
        "projection_build_id": report["projection_build_id"],
        "projection_integrity_status": report["projection_integrity_status"],
        "inventory_path": result["inventory_path"],
        "recovery_report_path": result["report_path"],
        "warnings": report["warnings"],
        "errors": report["errors"],
    })
    return 0 if result["ok"] else 4


def hypothesis_recovery_report_cli(args: argparse.Namespace) -> int:
    result = latest_hypothesis_recovery_report_impl(store_root=_store_root(args))
    _print(result)
    return 0 if result.get("ok") else 4

def rebuild_research_projections_cli(args: argparse.Namespace) -> int:
    result = rebuild_research_projections_impl(
        projection=args.projection,
        strict=str(args.strict).lower() == "true",
        store_root=_store_root(args),
        actor=args.actor,
    )
    hq = result["outputs"].get("hypothesis_queue", {})
    operator = result["outputs"].get("operator_home", {})
    projection = hq.get("projection", {})
    build = hq.get("build", {})
    operator_projection = operator.get("projection", {})
    operator_build = operator.get("build", {})
    primary_build = build or operator_build
    primary_projection = projection or operator_projection
    lane_counts = {lane: len(items or []) for lane, items in (projection.get("lanes") or {}).items()}
    operator_queue_counts = {name: len(items or []) for name, items in (operator_projection.get("queues") or {}).items()}
    _print({
        "ok": result["ok"],
        "projection_build_id": primary_build.get("projection_build_id"),
        "projection_type": primary_build.get("projection_type"),
        "status": primary_build.get("status"),
        "source_counts": (primary_projection.get("source_summary") or {}).get("registry_counts", {}),
        "source_proposal_count": (projection.get("source_summary") or {}).get("hypothesis_proposal_registry_row_count", 0),
        "artifact_proposal_count": (projection.get("source_summary") or {}).get("event_intake_proposal_artifact_count", 0),
        "projected_item_count": len(projection.get("items") or []),
        "lane_counts": lane_counts,
        "operator_projection_build_id": operator_build.get("projection_build_id"),
        "operator_projected_item_count": sum(operator_queue_counts.values()),
        "operator_queue_counts": operator_queue_counts,
        "integrity_status": primary_projection.get("integrity_status"),
        "warnings": primary_projection.get("integrity_warnings", []),
        "errors": primary_projection.get("integrity_errors", []),
        "output_uri": primary_build.get("output_uri"),
    })
    return 0 if result["ok"] else 4


def validate_research_projections_cli(args: argparse.Namespace) -> int:
    result = validate_research_projections_impl(
        projection=args.projection,
        strict=str(args.strict).lower() == "true",
        store_root=_store_root(args),
        actor=args.actor,
    )
    _print(result)
    return 0 if result["ok"] else 4

def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Aegis Research Lab immutable snapshot tools.")
    parser.add_argument("--store-root", default=None, help="Optional Research Store root override.")
    parser.add_argument("--actor", default="Aegis", help="Actor recorded in snapshot/audit metadata.")
    sub = parser.add_subparsers(dest="command", required=True)

    expanded_universes = sub.add_parser("create-expanded-universe-snapshots")
    expanded_universes.set_defaults(func=create_expanded_universe_snapshots)

    breadth_snapshot = sub.add_parser("build-breadth-snapshot")
    breadth_snapshot.add_argument("--dataset-snapshot-id", required=True)
    breadth_snapshot.add_argument("--universe-snapshot-id", required=True)
    breadth_snapshot.set_defaults(func=build_breadth_snapshot_cli)

    macro_calendar = sub.add_parser("import-macro-event-calendar")
    macro_calendar.add_argument("--input", required=True)
    macro_calendar.set_defaults(func=import_macro_event_calendar_cli)

    create_universe = sub.add_parser("create-universe-snapshot")
    create_universe.add_argument("--name", required=True)
    create_universe.add_argument("--version", required=True)
    create_universe.add_argument("--input", required=True)
    create_universe.set_defaults(func=create_universe_snapshot)

    list_universe = sub.add_parser("list-universe-snapshots")
    list_universe.set_defaults(func=list_universes)

    validate_uni = sub.add_parser("validate-universe-snapshot")
    validate_uni.add_argument("--universe-snapshot-id", required=True)
    validate_uni.set_defaults(func=validate_universe)

    create_dataset = sub.add_parser("create-empty-dataset-snapshot")
    create_dataset.add_argument("--dataset-type", required=True)
    create_dataset.add_argument("--provider", required=True)
    create_dataset.add_argument("--provider-version", default="placeholder_v1")
    create_dataset.add_argument("--interval", required=True)
    create_dataset.add_argument("--bar-policy", required=True)
    create_dataset.add_argument("--universe-snapshot-id", required=True)
    create_dataset.add_argument("--start", required=True)
    create_dataset.add_argument("--end", required=True)
    create_dataset.set_defaults(func=create_empty_dataset_snapshot)

    list_dataset = sub.add_parser("list-dataset-snapshots")
    list_dataset.set_defaults(func=list_datasets)

    validate_ds = sub.add_parser("validate-dataset-snapshot")
    validate_ds.add_argument("--dataset-snapshot-id", required=True)
    validate_ds.set_defaults(func=validate_dataset)

    build_ohlcv = sub.add_parser("build-ohlcv-dataset")
    build_ohlcv.add_argument("--dataset-type", default="ohlcv")
    build_ohlcv.add_argument("--provider", required=True)
    build_ohlcv.add_argument("--interval", required=True)
    build_ohlcv.add_argument("--bar-policy", required=True)
    build_ohlcv.add_argument("--universe-snapshot-id", required=True)
    build_ohlcv.add_argument("--start", required=True)
    build_ohlcv.add_argument("--end", required=True)
    build_ohlcv.add_argument("--allow-missing-symbols", choices=["true", "false"], default="false")
    build_ohlcv.add_argument("--require-readiness-report", choices=["true", "false"], default="false")
    build_ohlcv.add_argument("--readiness-report-path", default=None)
    build_ohlcv.add_argument("--env-file", default=None)
    build_ohlcv.set_defaults(func=build_ohlcv_dataset)

    head = sub.add_parser("dataset-head")
    head.add_argument("--dataset-snapshot-id", required=True)
    head.add_argument("--limit", type=int, default=20)
    head.set_defaults(func=dataset_head)

    summary = sub.add_parser("dataset-symbol-summary")
    summary.add_argument("--dataset-snapshot-id", required=True)
    summary.set_defaults(func=dataset_summary)

    coverage = sub.add_parser("dataset-coverage")
    coverage.add_argument("--dataset-snapshot-id", required=True)
    coverage.set_defaults(func=dataset_coverage)

    health = sub.add_parser("dependency-health")
    health.set_defaults(func=dependency_health)

    provider_config = sub.add_parser("provider-config-status")
    provider_config.add_argument("--env-file", default=None)
    provider_config.set_defaults(func=provider_config_status)

    env_template = sub.add_parser("create-provider-env-template")
    env_template.add_argument("--output", required=True)
    env_template.add_argument("--overwrite", choices=["true", "false"], default="false")
    env_template.set_defaults(func=create_provider_env_template)

    diagnostics = sub.add_parser("provider-diagnostics")
    diagnostics.add_argument("--provider", default="yfinance")
    diagnostics.add_argument("--env-file", default=None)
    diagnostics.set_defaults(func=provider_diagnostics)

    smoke = sub.add_parser("live-ohlcv-smoke-test")
    smoke.add_argument("--provider", default="yfinance")
    smoke.add_argument("--symbols", nargs="+", default=["SPY", "QQQ", "IWM"])
    smoke.add_argument("--start", default="2024-01-02")
    smoke.add_argument("--end", default="2024-02-01")
    smoke.add_argument("--env-file", default=None)
    smoke.set_defaults(func=live_ohlcv_smoke_test)

    create_plan = sub.add_parser("create-research-plan")
    create_plan.add_argument("--hypothesis-id", required=True)
    create_plan.add_argument("--title", required=True)
    create_plan.add_argument("--hypothesis", default=None)
    create_plan.add_argument("--dataset-snapshot-id", required=True)
    create_plan.add_argument("--universe-snapshot-id", required=True)
    create_plan.add_argument("--symbols", nargs="+", required=True)
    create_plan.add_argument("--start", required=True)
    create_plan.add_argument("--end", required=True)
    create_plan.add_argument("--event-type", required=True)
    create_plan.add_argument("--threshold", type=float, default=None)
    create_plan.add_argument("--percentile", type=float, default=None)
    create_plan.add_argument("--return-column", default="adj_close")
    create_plan.add_argument("--forward-windows", required=True)
    create_plan.set_defaults(func=create_research_plan)

    run_study = sub.add_parser("run-event-study")
    run_study.add_argument("--research-plan-id", required=True)
    run_study.add_argument("--regime-snapshot-id", default=None)
    run_study.add_argument("--cost-model-snapshot-id", default=None)
    run_study.set_defaults(func=run_event_study)

    create_bt = sub.add_parser("create-backtest-plan")
    create_bt.add_argument("--hypothesis-id", required=True)
    create_bt.add_argument("--title", required=True)
    create_bt.add_argument("--dataset-snapshot-id", required=True)
    create_bt.add_argument("--universe-snapshot-id", required=True)
    create_bt.add_argument("--regime-snapshot-id", required=True)
    create_bt.add_argument("--cost-model-snapshot-id", required=True)
    create_bt.add_argument("--symbols", nargs="+", required=True)
    create_bt.add_argument("--start", required=True)
    create_bt.add_argument("--end", required=True)
    create_bt.add_argument("--signal-type", required=True)
    create_bt.add_argument("--threshold", type=float, default=None)
    create_bt.add_argument("--percentile", type=float, default=None)
    create_bt.add_argument("--return-column", default="adj_close")
    create_bt.add_argument("--holding-period", type=int, default=5)
    create_bt.add_argument("--max-positions", type=int, default=5)
    create_bt.add_argument("--benchmark-symbol", default="SPY")
    create_bt.set_defaults(func=create_backtest_plan)

    run_bt = sub.add_parser("run-backtest")
    run_bt.add_argument("--backtest-plan-id", required=True)
    run_bt.set_defaults(func=run_backtest)

    bt_summary = sub.add_parser("backtest-summary")
    bt_summary.add_argument("--evidence-package-id", required=True)
    bt_summary.set_defaults(func=backtest_summary)

    first_bt = sub.add_parser("build-first-backtest-evidence")
    first_bt.add_argument("--dataset-snapshot-id", required=True)
    first_bt.add_argument("--regime-snapshot-id", required=True)
    first_bt.add_argument("--cost-model-snapshot-id", required=True)
    first_bt.set_defaults(func=build_first_backtest_evidence)

    gen_candidates = sub.add_parser("generate-candidates")
    gen_candidates.add_argument("--source-evidence-package-id", required=True)
    gen_candidates.add_argument("--dataset-snapshot-id", required=True)
    gen_candidates.add_argument("--regime-snapshot-id", required=True)
    gen_candidates.add_argument("--cost-model-snapshot-id", required=True)
    gen_candidates.add_argument("--as-of-date", required=True)
    gen_candidates.add_argument("--threshold", type=float, default=-0.02)
    gen_candidates.set_defaults(func=generate_candidates)

    list_cand = sub.add_parser("list-candidates")
    list_cand.add_argument("--candidate-batch-id", required=True)
    list_cand.set_defaults(func=list_candidates)

    decision = sub.add_parser("record-operator-decision")
    decision.add_argument("--candidate-id", required=True)
    decision.add_argument("--candidate-batch-id", default=None)
    decision.add_argument("--decision", required=True)
    decision.add_argument("--reason", required=True)
    decision.add_argument("--decided-by", required=True)
    decision.add_argument("--portfolio-context-snapshot-id", default="")
    decision.add_argument("--notes", default="")
    decision.set_defaults(func=record_operator_decision)

    ledger_summary = sub.add_parser("candidate-ledger-summary")
    ledger_summary.add_argument("--candidate-batch-id", required=True)
    ledger_summary.set_defaults(func=candidate_ledger_summary)

    measure_outcomes = sub.add_parser("measure-candidate-outcomes")
    measure_outcomes.add_argument("--candidate-batch-id", required=True)
    measure_outcomes.add_argument("--dataset-snapshot-id", required=True)
    measure_outcomes.add_argument("--cost-model-snapshot-id", required=True)
    measure_outcomes.add_argument("--benchmark-symbol", default="SPY")
    measure_outcomes.add_argument("--windows", default="1,2,5,10,20")
    measure_outcomes.set_defaults(func=measure_candidate_outcomes_cli)

    attribution_summary = sub.add_parser("candidate-attribution-summary")
    attribution_summary.add_argument("--candidate-batch-id", required=True)
    attribution_summary.set_defaults(func=candidate_attribution_summary)

    outcomes = sub.add_parser("list-outcomes")
    outcomes.add_argument("--candidate-batch-id", required=True)
    outcomes.set_defaults(func=list_outcomes)

    create_slv = sub.add_parser("create-sleeve")
    create_slv.add_argument("--sleeve-id", required=True)
    create_slv.add_argument("--name", required=True)
    create_slv.add_argument("--hypothesis-id", required=True)
    create_slv.add_argument("--sleeve-type", default="research_only")
    create_slv.add_argument("--description", default="")
    create_slv.set_defaults(func=create_sleeve)

    create_slvv = sub.add_parser("create-sleeve-version")
    create_slvv.add_argument("--sleeve-id", required=True)
    create_slvv.add_argument("--version", required=True)
    create_slvv.add_argument("--hypothesis-id", default="hyp_etf_drop_reversion_v1")
    create_slvv.add_argument("--event-study-evidence-package-id", required=True)
    create_slvv.add_argument("--backtest-evidence-package-id", required=True)
    create_slvv.add_argument("--candidate-batch-id", required=True)
    create_slvv.add_argument("--dataset-snapshot-id", required=True)
    create_slvv.add_argument("--regime-snapshot-id", required=True)
    create_slvv.add_argument("--cost-model-snapshot-id", required=True)
    create_slvv.set_defaults(func=create_sleeve_version_cli)

    eval_slv = sub.add_parser("evaluate-sleeve-health")
    eval_slv.add_argument("--sleeve-id", required=True)
    eval_slv.add_argument("--sleeve-version-id", required=True)
    eval_slv.add_argument("--as-of-date", required=True)
    eval_slv.set_defaults(func=evaluate_sleeve_health)

    chal_slv = sub.add_parser("challenge-sleeve")
    chal_slv.add_argument("--sleeve-id", required=True)
    chal_slv.add_argument("--sleeve-version-id", required=True)
    chal_slv.add_argument("--sleeve-health-snapshot-id", required=True)
    chal_slv.set_defaults(func=challenge_sleeve)

    review_slv = sub.add_parser("record-sleeve-review")
    review_slv.add_argument("--sleeve-id", required=True)
    review_slv.add_argument("--sleeve-version-id", required=True)
    review_slv.add_argument("--sleeve-challenge-id", required=True)
    review_slv.add_argument("--decision", required=True)
    review_slv.add_argument("--reason", required=True)
    review_slv.add_argument("--reviewed-by", required=True)
    review_slv.set_defaults(func=record_sleeve_review)

    slv_summary = sub.add_parser("sleeve-summary")
    slv_summary.add_argument("--sleeve-id", required=True)
    slv_summary.set_defaults(func=sleeve_summary_cli)

    long_run = sub.add_parser("run-longitudinal-candidate-study")
    long_run.add_argument("--sleeve-id", required=True)
    long_run.add_argument("--sleeve-version-id", required=True)
    long_run.add_argument("--source-evidence-package-id", required=True)
    long_run.add_argument("--dataset-snapshot-id", required=True)
    long_run.add_argument("--regime-snapshot-id", required=True)
    long_run.add_argument("--cost-model-snapshot-id", required=True)
    long_run.add_argument("--start", required=True)
    long_run.add_argument("--end", required=True)
    long_run.add_argument("--frequency", choices=["daily", "weekly", "monthly"], default="weekly")
    long_run.add_argument("--threshold", type=float, default=-0.02)
    long_run.add_argument("--outcome-windows", default="1,2,5,10,20")
    long_run.set_defaults(func=run_longitudinal_candidate_study_cli)

    ranking_report = sub.add_parser("ranking-quality-report")
    ranking_report.add_argument("--longitudinal-run-id", required=True)
    ranking_report.set_defaults(func=ranking_quality_report_cli)

    learning_report = sub.add_parser("sleeve-learning-report")
    learning_report.add_argument("--sleeve-id", required=True)
    learning_report.add_argument("--sleeve-version-id", required=True)
    learning_report.set_defaults(func=sleeve_learning_report_cli)

    drift_report = sub.add_parser("build-expectancy-drift-report")
    drift_report.add_argument("--sleeve-id", required=True)
    drift_report.add_argument("--sleeve-version-id", required=True)
    drift_report.add_argument("--longitudinal-run-id", required=True)
    drift_report.add_argument("--rolling-window-size", type=int, default=20)
    drift_report.set_defaults(func=expectancy_drift_report_cli)

    fragility_report = sub.add_parser("build-regime-fragility-report")
    fragility_report.add_argument("--sleeve-id", required=True)
    fragility_report.add_argument("--sleeve-version-id", required=True)
    fragility_report.add_argument("--longitudinal-run-id", required=True)
    fragility_report.set_defaults(func=regime_fragility_report_cli)

    stability_report = sub.add_parser("build-sleeve-stability-report")
    stability_report.add_argument("--sleeve-id", required=True)
    stability_report.add_argument("--sleeve-version-id", required=True)
    stability_report.add_argument("--expectancy-drift-report-id", required=True)
    stability_report.add_argument("--regime-fragility-report-id", required=True)
    stability_report.set_defaults(func=sleeve_stability_report_cli)

    stability_reports = sub.add_parser("build-stability-reports")
    stability_reports.add_argument("--sleeve-id", required=True)
    stability_reports.add_argument("--sleeve-version-id", required=True)
    stability_reports.add_argument("--longitudinal-run-id", required=True)
    stability_reports.add_argument("--rolling-window-size", type=int, default=20)
    stability_reports.set_defaults(func=stability_reports_cli)

    challenger_track = sub.add_parser("build-challenger-research-track")
    challenger_track.add_argument("--sleeve-id", required=True)
    challenger_track.add_argument("--sleeve-stability-report-id", required=True)
    challenger_track.set_defaults(func=challenger_research_track_cli)

    challenger_evidence = sub.add_parser("generate-challenger-evidence")
    challenger_evidence.add_argument("--challenger-track-id", required=True)
    challenger_evidence.set_defaults(func=challenger_evidence_cli)

    challenger_comparison = sub.add_parser("compare-challengers")
    challenger_comparison.add_argument("--challenger-evidence-batch-id", required=True)
    challenger_comparison.add_argument("--challenger-track-id", default=None)
    challenger_comparison.set_defaults(func=challenger_comparison_cli)

    human_review_dossier = sub.add_parser("prepare-human-review-dossier")
    human_review_dossier.add_argument("--challenger-comparison-report-id", required=True)
    human_review_dossier.set_defaults(func=human_review_dossier_cli)

    create_ptr = sub.add_parser("create-paper-trial")
    create_ptr.add_argument("--sleeve-id", required=True)
    create_ptr.add_argument("--sleeve-version-id", required=True)
    create_ptr.add_argument("--hypothesis-id", required=True)
    create_ptr.add_argument("--dataset-snapshot-id", required=True)
    create_ptr.add_argument("--regime-snapshot-id", required=True)
    create_ptr.add_argument("--cost-model-snapshot-id", required=True)
    create_ptr.add_argument("--source-evidence-package-ids", required=True)
    create_ptr.add_argument("--threshold", type=float, required=True)
    create_ptr.add_argument("--observation-frequency", choices=["manual", "daily", "weekly", "monthly"], default="manual")
    create_ptr.add_argument("--outcome-windows", default="1,2,5,10,20")
    create_ptr.set_defaults(func=create_paper_trial)

    activate_ptr = sub.add_parser("activate-paper-trial")
    activate_ptr.add_argument("--paper-trial-id", required=True)
    activate_ptr.set_defaults(func=activate_paper_trial)

    observe_ptr = sub.add_parser("record-paper-observation")
    observe_ptr.add_argument("--paper-trial-id", required=True)
    observe_ptr.add_argument("--as-of-date", required=True)
    observe_ptr.add_argument("--operator-note", default="")
    observe_ptr.set_defaults(func=record_paper_observation)

    measure_ptr = sub.add_parser("measure-paper-outcomes")
    measure_ptr.add_argument("--paper-trial-id", required=True)
    measure_ptr.add_argument("--paper-trial-observation-id", required=True)
    measure_ptr.set_defaults(func=measure_paper_outcomes)

    review_ptr = sub.add_parser("review-paper-trial")
    review_ptr.add_argument("--paper-trial-id", required=True)
    review_ptr.add_argument("--decision", required=True)
    review_ptr.add_argument("--reason", required=True)
    review_ptr.add_argument("--reviewed-by", required=True)
    review_ptr.set_defaults(func=review_paper_trial)

    ptr_summary = sub.add_parser("paper-trial-summary")
    ptr_summary.add_argument("--paper-trial-id", required=True)
    ptr_summary.set_defaults(func=paper_trial_summary)

    next_observe = sub.add_parser("record-next-paper-observation")
    next_observe.add_argument("--paper-trial-id", required=True)
    next_observe.add_argument("--as-of-date", required=True)
    next_observe.add_argument("--operator-note", default="")
    next_observe.set_defaults(func=record_next_paper_observation_cli)

    due_list = sub.add_parser("list-paper-due-outcomes")
    due_list.add_argument("--paper-trial-id", required=True)
    due_list.add_argument("--as-of-date", required=True)
    due_list.set_defaults(func=list_paper_due_outcomes_cli)

    measure_due = sub.add_parser("measure-due-paper-outcomes")
    measure_due.add_argument("--paper-trial-id", required=True)
    measure_due.add_argument("--as-of-date", required=True)
    measure_due.set_defaults(func=measure_due_paper_outcomes_cli)

    ops_report = sub.add_parser("paper-trial-ops-report")
    ops_report.add_argument("--paper-trial-id", required=True)
    ops_report.add_argument("--as-of-date", default=date.today().isoformat())
    ops_report.set_defaults(func=paper_trial_ops_report_cli)

    evidence_inventory = sub.add_parser("build-evidence-inventory")
    evidence_inventory.set_defaults(func=build_evidence_inventory_cli)

    paper_inventory = sub.add_parser("build-paper-trial-inventory")
    paper_inventory.set_defaults(func=build_paper_trial_inventory_cli)

    compare_slv = sub.add_parser("compare-sleeves")
    compare_slv.set_defaults(func=compare_sleeves_cli)

    backlog_priority = sub.add_parser("research-backlog-priority")
    backlog_priority.set_defaults(func=research_backlog_priority_cli)

    portfolio_reports = sub.add_parser("build-portfolio-evidence-reports")
    portfolio_reports.set_defaults(func=build_portfolio_evidence_reports_cli)

    list_ev = sub.add_parser("list-evidence-packages")
    list_ev.set_defaults(func=list_evidence)

    ev_summary = sub.add_parser("evidence-summary")
    ev_summary.add_argument("--evidence-package-id", required=True)
    ev_summary.add_argument("--include-regime", action="store_true")
    ev_summary.add_argument("--include-costs", action="store_true")
    ev_summary.set_defaults(func=evidence_summary)

    regime = sub.add_parser("build-regime-snapshot")
    regime.add_argument("--dataset-snapshot-id", required=True)
    regime.add_argument("--benchmark-symbol", default="SPY")
    regime.set_defaults(func=build_regime_snapshot)

    cost_model = sub.add_parser("create-default-cost-model")
    cost_model.set_defaults(func=create_default_cost_model)

    ev_compare = sub.add_parser("compare-evidence")
    ev_compare.add_argument("--evidence-package-ids", required=True)
    ev_compare.add_argument("--output", default=None)
    ev_compare.set_defaults(func=compare_evidence)

    template = sub.add_parser("create-csv-template")
    template.add_argument("--symbol", required=True)
    template.add_argument("--output", required=True)
    template.set_defaults(func=create_csv_template)

    expected = sub.add_parser("expected-csv-files")
    expected.add_argument("--universe-snapshot-id", required=True)
    expected.add_argument("--csv-root", required=True)
    expected.add_argument("--output", default=None)
    expected.set_defaults(func=expected_csv_files)

    readiness = sub.add_parser("validate-local-csvs")
    readiness.add_argument("--universe-snapshot-id", required=True)
    readiness.add_argument("--csv-root", required=True)
    readiness.add_argument("--start", required=True)
    readiness.add_argument("--end", required=True)
    readiness.add_argument("--allow-missing-symbols", choices=["true", "false"], default="false")
    readiness.add_argument("--output", default=None)
    readiness.set_defaults(func=validate_local_csvs)

    import_csv = sub.add_parser("import-local-csvs")
    import_csv.add_argument("--source-dir", required=True)
    import_csv.add_argument("--csv-root", required=True)
    import_csv.add_argument("--universe-snapshot-id", required=True)
    import_csv.add_argument("--mode", choices=["copy", "dry-run"], required=True)
    import_csv.add_argument("--overwrite", choices=["true", "false"], default="false")
    import_csv.set_defaults(func=import_local_csvs)

    first_status = sub.add_parser("first-dataset-status")
    first_status.add_argument("--csv-root", required=True)
    first_status.set_defaults(func=first_dataset_status)

    build_present = sub.add_parser("build-from-present-csvs")
    build_present.add_argument("--universe", required=True)
    build_present.add_argument("--csv-root", required=True)
    build_present.add_argument("--start", required=True)
    build_present.add_argument("--end", required=True)
    build_present.set_defaults(func=build_from_present_csvs)

    standard_plan = sub.add_parser("create-standard-event-study")
    standard_plan.add_argument("--dataset-snapshot-id", required=True)
    standard_plan.add_argument("--hypothesis-id", required=True)
    standard_plan.add_argument("--threshold", type=float, required=True)
    standard_plan.add_argument("--forward-windows", required=True)
    standard_plan.set_defaults(func=create_standard_event_study)

    standard_run = sub.add_parser("run-standard-event-study")
    standard_run.add_argument("--research-plan-id", required=True)
    standard_run.set_defaults(func=run_standard_event_study)

    guide = sub.add_parser("csv-download-guide")
    guide.add_argument("--universe", required=True)
    guide.add_argument("--csv-root", required=True)
    guide.set_defaults(func=csv_download_guide)

    verify_downloads = sub.add_parser("verify-downloaded-csvs")
    verify_downloads.add_argument("--csv-root", required=True)
    verify_downloads.add_argument("--universe", required=True)
    verify_downloads.set_defaults(func=verify_downloaded_csvs)

    api_status = sub.add_parser("first-api-dataset-status")
    api_status.add_argument("--provider", required=True)
    api_status.add_argument("--universe", required=True)
    api_status.add_argument("--env-file", default=None)
    api_status.set_defaults(func=first_api_dataset_status)

    api_evidence = sub.add_parser("build-first-api-evidence")
    api_evidence.add_argument("--provider", required=True)
    api_evidence.add_argument("--universe", required=True)
    api_evidence.add_argument("--start", required=True)
    api_evidence.add_argument("--end", required=True)
    api_evidence.add_argument("--threshold", type=float, required=True)
    api_evidence.add_argument("--forward-windows", required=True)
    api_evidence.add_argument("--env-file", default=None)
    api_evidence.set_defaults(func=build_first_api_evidence)


    seed_families = sub.add_parser("seed-event-families")
    seed_families.set_defaults(func=seed_event_families)

    capture_event = sub.add_parser("capture-event-observation")
    capture_event.add_argument("--event-family", required=True)
    capture_event.add_argument("--title", required=True)
    capture_event.add_argument("--description", required=True)
    capture_event.add_argument("--source-type", required=True)
    capture_event.add_argument("--source-ref", default="")
    capture_event.add_argument("--symbols", nargs="+", default=[])
    capture_event.add_argument("--suspected-mechanism", default="")
    capture_event.add_argument("--confidence-level", default=None)
    capture_event.add_argument("--research-priority", default="medium")
    capture_event.add_argument("--notes", default="")
    capture_event.set_defaults(func=capture_event_observation)

    create_cluster = sub.add_parser("create-event-cluster")
    create_cluster.add_argument("--event-family", required=True)
    create_cluster.add_argument("--observation-ids", nargs="+", required=True)
    create_cluster.add_argument("--cluster-title", required=True)
    create_cluster.add_argument("--cluster-description", default="")
    create_cluster.set_defaults(func=create_event_cluster_cli)

    gen_intent = sub.add_parser("generate-intent-candidate")
    gen_intent.add_argument("--event-cluster-id", required=True)
    gen_intent.set_defaults(func=generate_intent_candidate_cli)

    gen_proposal = sub.add_parser("generate-hypothesis-proposal")
    gen_proposal.add_argument("--intent-candidate-id", required=True)
    gen_proposal.set_defaults(func=generate_hypothesis_proposal_cli)

    list_proposals = sub.add_parser("list-hypothesis-proposals")
    list_proposals.add_argument("--status", default=None)
    list_proposals.set_defaults(func=list_hypothesis_proposals_cli)

    queue_proposals = sub.add_parser("hypothesis-proposal-queue")
    queue_proposals.add_argument("--status", default=None)
    queue_proposals.set_defaults(func=hypothesis_proposal_queue_cli)

    assess_proposal = sub.add_parser("assess-hypothesis-readiness")
    assess_proposal.add_argument("--hypothesis-proposal-id", required=True)
    assess_proposal.set_defaults(func=assess_hypothesis_readiness_cli)

    reassess_proposal = sub.add_parser("reassess-hypothesis-readiness")
    reassess_proposal.add_argument("--hypothesis-proposal-id", required=True)
    reassess_proposal.set_defaults(func=reassess_hypothesis_readiness_cli)

    dossier_proposal = sub.add_parser("build-research-intake-dossier")
    dossier_proposal.add_argument("--hypothesis-proposal-id", required=True)
    dossier_proposal.set_defaults(func=build_research_intake_dossier_cli)

    review_proposal = sub.add_parser("review-hypothesis-proposal")
    review_proposal.add_argument("--hypothesis-proposal-id", required=True)
    review_proposal.add_argument("--decision", required=True)
    review_proposal.add_argument("--reason", required=True)
    review_proposal.add_argument("--reviewed-by", default=None)
    review_proposal.set_defaults(func=review_hypothesis_proposal_cli)

    convert_proposal = sub.add_parser("convert-hypothesis-proposal-to-research-plan")
    convert_proposal.add_argument("--hypothesis-proposal-id", required=True)
    convert_proposal.add_argument("--approve", choices=["true", "false"], required=True)
    convert_proposal.add_argument("--dataset-snapshot-id", default=None)
    convert_proposal.add_argument("--universe-snapshot-id", default=None)
    convert_proposal.add_argument("--start", default=None)
    convert_proposal.add_argument("--end", default=None)
    convert_proposal.set_defaults(func=convert_hypothesis_proposal_to_research_plan_cli)

    recover = sub.add_parser("recover-hypotheses")
    recover.add_argument("--dry-run", choices=["true", "false"], required=True)
    recover.add_argument("--seed-defaults", choices=["true", "false"], default="false")
    recover.set_defaults(func=recover_hypotheses_cli)

    recovery_report = sub.add_parser("hypothesis-recovery-report")
    recovery_report.set_defaults(func=hypothesis_recovery_report_cli)

    rebuild_proj = sub.add_parser("rebuild-research-projections")
    rebuild_proj.add_argument("--projection", choices=["hypothesis_queue", "operator_home", "all"], default="all")
    rebuild_proj.add_argument("--strict", choices=["true", "false"], default="false")
    rebuild_proj.set_defaults(func=rebuild_research_projections_cli)

    validate_proj = sub.add_parser("validate-research-projections")
    validate_proj.add_argument("--projection", choices=["hypothesis_queue", "operator_home", "all"], default="all")
    validate_proj.add_argument("--strict", choices=["true", "false"], default="false")
    validate_proj.set_defaults(func=validate_research_projections_cli)

    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    try:
        return int(args.func(args))
    except FileExistsError as exc:
        print(json.dumps({"error": str(exc), "immutable_snapshot_violation": True}, sort_keys=True), file=sys.stderr)
        return 3
    except RuntimeError as exc:
        print(json.dumps({"error": str(exc), "failed_closed": True}, sort_keys=True), file=sys.stderr)
        return 4


if __name__ == "__main__":
    raise SystemExit(main())
