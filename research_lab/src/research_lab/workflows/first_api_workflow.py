from __future__ import annotations

from pathlib import Path
from typing import Any

from research_lab.config.provider_config import provider_config_status
from research_lab.datasets.ohlcv_builder import build_ohlcv_dataset_snapshot
from research_lab.providers.factory import provider_diagnostics
from research_lab.runtime.dependencies import research_dependency_health
from research_lab.workflows.first_dataset_workflow import (
    MINIMUM_VALID_SYMBOLS,
    create_or_reuse_minimum_viable_universe,
)
from research_lab.workflows.first_event_study_workflow import create_standard_event_study, run_standard_event_study


API_PROVIDER_CONFIGS = {
    "alpha_vantage": {
        "api_key_env": "ALPHA_VANTAGE_API_KEY",
        "bar_policy_version": "bp_daily_ohlcv_alpha_vantage_v1",
    },
    "tiingo": {
        "api_key_env": "TIINGO_API_KEY",
        "bar_policy_version": "bp_daily_ohlcv_tiingo_v1",
    },
}


def first_api_dataset_status(
    *,
    provider: str,
    universe: str,
    store_root: Path | None = None,
    actor: str = "Aegis",
    env_file: Path | None = None,
) -> dict[str, Any]:
    provider_name = str(provider or "").strip().lower()
    config = API_PROVIDER_CONFIGS.get(provider_name)
    if not config:
        raise ValueError("API dataset workflow supports --provider alpha_vantage or --provider tiingo")
    if universe != "local_etf_minimum_viable_v1":
        raise ValueError("API dataset workflow supports --universe local_etf_minimum_viable_v1")
    snapshot = create_or_reuse_minimum_viable_universe(store_root=store_root, actor=actor)
    diagnostics = provider_diagnostics(provider_name)
    config_status = provider_config_status()
    config_row = next((row for row in config_status["providers"] if row["provider"] == provider_name), None)
    blocking_reasons: list[str] = []
    if not diagnostics.get("api_key_present"):
        blocking_reasons.append(f"missing {config['api_key_env']}")
    if config_row and not config_row["ready"]:
        for reason in str(config_row["blocking_reason"] or "").split("; "):
            if reason and reason not in blocking_reasons:
                blocking_reasons.append(reason)
    ready = not blocking_reasons
    env_suffix = f" --env-file {env_file}" if env_file else ""
    smoke = (
        "research_lab/.venv/bin/python -m research_lab.cli live-ohlcv-smoke-test "
        f"--provider {provider_name} --symbols SPY QQQ IWM --start 2024-01-02 --end 2024-02-01{env_suffix}"
    )
    build = (
        "research_lab/.venv/bin/python -m research_lab.cli build-ohlcv-dataset "
        f"--provider {provider_name} --interval 1d --bar-policy {config['bar_policy_version']} "
        f"--universe-snapshot-id {snapshot['universe_snapshot_id']} --start 2015-01-01 --end 2025-12-31 --allow-missing-symbols true{env_suffix}"
    )
    plan = (
        "research_lab/.venv/bin/python -m research_lab.cli create-standard-event-study "
        "--dataset-snapshot-id <new_dataset_snapshot_id> --hypothesis-id hyp_etf_drop_reversion_v1 "
        "--threshold -0.02 --forward-windows 1,2,5,10"
    )
    run = "research_lab/.venv/bin/python -m research_lab.cli run-standard-event-study --research-plan-id <new_research_plan_id>"
    return {
        "provider": provider_name,
        "api_key_present": bool(diagnostics.get("api_key_present")),
        "dependency_status": research_dependency_health(),
        "universe": universe,
        "universe_snapshot_id": snapshot["universe_snapshot_id"],
        "required_min_symbols": MINIMUM_VALID_SYMBOLS,
        "provider_rate_limit_policy": diagnostics.get("rate_limit_policy"),
        "exact_smoke_test_command": smoke,
        "exact_build_command": build,
        "exact_standard_event_study_commands": {"create_plan": plan, "run_study": run},
        "ready_to_build": ready,
        "blocking_reasons": blocking_reasons,
        "provider_diagnostics": diagnostics,
        "provider_config": config_row,
        "bar_policy_version": config["bar_policy_version"],
        "no_fake_data_generated": True,
        "no_scraping_bypass": True,
        "schema_version": "first_api_dataset_status.v1",
    }


def build_first_api_evidence(
    *,
    provider: str,
    universe: str,
    start: str,
    end: str,
    threshold: float,
    forward_windows: list[int],
    store_root: Path | None = None,
    actor: str = "Aegis",
    env_file: Path | None = None,
) -> dict[str, Any]:
    status = first_api_dataset_status(provider=provider, universe=universe, store_root=store_root, actor=actor, env_file=env_file)
    if not status["ready_to_build"]:
        raise RuntimeError("API dataset workflow is blocked: " + ", ".join(status["blocking_reasons"]))
    dataset_result = build_ohlcv_dataset_snapshot(
        dataset_type="ohlcv",
        provider_name=status["provider"],
        interval="1d",
        bar_policy_version=status["bar_policy_version"],
        universe_snapshot_id=status["universe_snapshot_id"],
        start_date=start,
        end_date=end,
        store_root=store_root,
        created_by=actor,
        command="python -m research_lab.cli build-first-api-evidence",
        allow_missing_symbols=True,
    )
    dataset_id = dataset_result["dataset_snapshot"]["dataset_snapshot_id"]
    plan_result = create_standard_event_study(
        dataset_snapshot_id=dataset_id,
        hypothesis_id="hyp_etf_drop_reversion_v1",
        threshold=threshold,
        forward_windows=forward_windows,
        store_root=store_root,
        actor=actor,
    )
    study_result = run_standard_event_study(
        research_plan_id=plan_result["research_plan"]["research_plan_id"],
        store_root=store_root,
        actor=actor,
    )
    return {
        "status": "completed",
        "dataset_snapshot_id": dataset_id,
        "research_plan_id": plan_result["research_plan"]["research_plan_id"],
        "evidence_package_id": study_result["event_study"]["evidence_manifest"]["evidence_package_id"],
        "provider_status": status,
        "dataset_build": dataset_result,
        "research_plan": plan_result,
        "event_study": study_result,
    }
