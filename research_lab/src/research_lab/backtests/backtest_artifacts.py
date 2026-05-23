from __future__ import annotations

from pathlib import Path
import shutil
from typing import Any

from research_lab.backtests.performance_metrics import summary_markdown
from research_lab.contracts.schemas import validate_contract
from research_lab.research.research_plan import slugify
from research_lab.storage.hashing import content_hash, short_hash, utc_now_iso
from research_lab.storage.manifest_io import write_json
from research_lab.storage.parquet_io import file_sha256, write_parquet_records
from research_lab.storage.paths import ensure_store_layout, evidence_uri


def write_backtest_evidence_package(
    *,
    plan: dict[str, Any],
    dataset_snapshot: dict[str, Any],
    regime_snapshot: dict[str, Any],
    cost_model_snapshot: dict[str, Any],
    trades: list[dict[str, Any]],
    daily_positions: list[dict[str, Any]],
    equity_curve: list[dict[str, Any]],
    summary: dict[str, Any],
    created_by: str = "Aegis",
    store_root: Path | None = None,
    allow_json_fallback: bool = False,
) -> dict[str, Any]:
    store = ensure_store_layout(store_root)
    package_seed = content_hash(
        {
            "backtest_plan_id": plan["backtest_plan_id"],
            "dataset_snapshot_id": plan["dataset_snapshot_id"],
            "regime_snapshot_id": plan["regime_snapshot_id"],
            "cost_model_snapshot_id": plan["cost_model_snapshot_id"],
            "trade_count": len(trades),
            "plan_hash": plan["content_hash"],
        },
        sort_lists=True,
    )
    today = utc_now_iso()[:10].replace("-", "")
    evidence_package_id = f"ev_bt_{slugify(plan['hypothesis_id'])}_{today}_{short_hash(package_seed, 10)}"
    package_dir = store / "evidence_packages" / evidence_package_id
    if package_dir.exists():
        raise FileExistsError(f"Refusing to overwrite immutable evidence package: {package_dir}")
    stage_dir = store / "tmp" / f"{evidence_package_id}.staging"
    if stage_dir.exists():
        shutil.rmtree(stage_dir)
    stage_dir.mkdir(parents=True, exist_ok=False)

    write_json(stage_dir / "backtest_plan.json", plan, overwrite=False)
    trade_path = stage_dir / "trade_table.parquet"
    positions_path = stage_dir / "daily_position_table.parquet"
    equity_path = stage_dir / "equity_curve.parquet"
    write_parquet_records(trade_path, trades, allow_json_fallback=allow_json_fallback)
    write_parquet_records(positions_path, daily_positions, allow_json_fallback=allow_json_fallback)
    write_parquet_records(equity_path, equity_curve, allow_json_fallback=allow_json_fallback)
    summary_path = stage_dir / "performance_summary.json"
    validate_contract("backtest_result", summary)
    write_json(summary_path, summary, overwrite=False)
    (stage_dir / "performance_summary.md").write_text(summary_markdown(summary), encoding="utf-8")
    (stage_dir / "logs.txt").write_text(
        "Deterministic holding_period_backtest_v1 completed without broker execution, live trading, or sleeve mutation.\n",
        encoding="utf-8",
    )

    trade_hash = file_sha256(trade_path)
    positions_hash = file_sha256(positions_path)
    equity_hash = file_sha256(equity_path)
    summary_hash = file_sha256(summary_path)
    manifest = {
        "evidence_package_id": evidence_package_id,
        "evidence_type": "backtest",
        "hypothesis_id": plan["hypothesis_id"],
        "research_plan_id": plan["backtest_plan_id"],
        "backtest_plan_id": plan["backtest_plan_id"],
        "dataset_snapshot_id": plan["dataset_snapshot_id"],
        "universe_snapshot_id": plan["universe_snapshot_id"],
        "regime_snapshot_id": plan["regime_snapshot_id"],
        "regime_snapshot_hash": regime_snapshot["content_hash"],
        "cost_model_snapshot_id": plan["cost_model_snapshot_id"],
        "cost_model_snapshot_hash": cost_model_snapshot["content_hash"],
        "runner_name": plan["runner_name"],
        "runner_version": plan["runner_version"],
        "runner_input_hash": content_hash(
            {
                "backtest_plan_hash": plan["content_hash"],
                "dataset_snapshot_hash": dataset_snapshot["content_hash"],
                "regime_snapshot_hash": regime_snapshot["content_hash"],
                "cost_model_snapshot_hash": cost_model_snapshot["content_hash"],
            },
            sort_lists=True,
        ),
        "runner_output_hash": content_hash(
            {"trade_table_hash": trade_hash, "equity_curve_hash": equity_hash, "performance_summary_hash": summary_hash},
            sort_lists=True,
        ),
        "trade_count": len(trades),
        "event_count": len(trades),
        "evidence_quality": "research_simulation",
        "trade_table_hash": trade_hash,
        "daily_position_table_hash": positions_hash,
        "equity_curve_hash": equity_hash,
        "performance_summary_hash": summary_hash,
        "summary_hash": summary_hash,
        "artifact_uris": [
            f"{evidence_uri(evidence_package_id)}/trade_table.parquet",
            f"{evidence_uri(evidence_package_id)}/daily_position_table.parquet",
            f"{evidence_uri(evidence_package_id)}/equity_curve.parquet",
            f"{evidence_uri(evidence_package_id)}/performance_summary.json",
            f"{evidence_uri(evidence_package_id)}/performance_summary.md",
        ],
        "summary_uri": f"{evidence_uri(evidence_package_id)}/performance_summary.json",
        "created_at": utc_now_iso(),
        "created_by": created_by,
        "schema_version": "evidence_package.v1",
    }
    manifest["manifest_hash"] = content_hash(
        manifest,
        exclude={"created_at", "manifest_hash", "evidence_package_id", "artifact_uris", "summary_uri"},
        sort_lists=True,
    )
    validate_contract("evidence_package", manifest)
    write_json(stage_dir / "evidence_manifest.json", manifest, overwrite=False)
    stage_dir.rename(package_dir)
    return manifest

