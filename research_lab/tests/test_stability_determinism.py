from __future__ import annotations

from pathlib import Path

from research_lab.stability.expectancy_drift import build_expectancy_drift_report
from research_lab.stability.regime_fragility import build_regime_fragility_report
from research_lab.storage.manifest_io import write_json
from research_lab.storage.parquet_io import write_parquet_records


SLEEVE_ID = "slv_fixture"
SLEEVE_VERSION_ID = "slvv_fixture"


def _write_run(store: Path) -> str:
    run_id = "lcr_fixture"
    root = store / "longitudinal_runs" / run_id
    rows = []
    for idx in range(50):
        rows.append(
            {
                "candidate_id": f"cand_{idx:03d}",
                "as_of_date": f"2024-04-{(idx % 28) + 1:02d}",
                "outcome_window": "5d",
                "post_cost_return": 0.01 if idx % 2 == 0 else -0.005,
                "excess_return": 0.002,
                "outcome_status": "measured",
                "risk_regime": "risk_on" if idx < 25 else "risk_off",
            }
        )
    write_json(
        root / "longitudinal_run.json",
        {
            "longitudinal_run_id": run_id,
            "sleeve_id": SLEEVE_ID,
            "sleeve_version_id": SLEEVE_VERSION_ID,
            "schema_version": "longitudinal_candidate_run.v1",
        },
        overwrite=False,
    )
    write_parquet_records(root / "candidate_batch_index.parquet", [{"candidate_count": len(rows)}], allow_json_fallback=True)
    write_parquet_records(root / "outcome_index.parquet", rows, allow_json_fallback=True)
    return run_id


def test_stability_reports_are_deterministic(tmp_path: Path) -> None:
    store = tmp_path / "store"
    run_id = _write_run(store)

    drift_a = build_expectancy_drift_report(sleeve_id=SLEEVE_ID, sleeve_version_id=SLEEVE_VERSION_ID, longitudinal_run_id=run_id, store_root=store)
    drift_b = build_expectancy_drift_report(sleeve_id=SLEEVE_ID, sleeve_version_id=SLEEVE_VERSION_ID, longitudinal_run_id=run_id, store_root=store)
    fragility_a = build_regime_fragility_report(sleeve_id=SLEEVE_ID, sleeve_version_id=SLEEVE_VERSION_ID, longitudinal_run_id=run_id, store_root=store)
    fragility_b = build_regime_fragility_report(sleeve_id=SLEEVE_ID, sleeve_version_id=SLEEVE_VERSION_ID, longitudinal_run_id=run_id, store_root=store)

    assert drift_a == drift_b
    assert fragility_a == fragility_b
    assert list(drift_a["rolling_metrics"]) == sorted(drift_a["rolling_metrics"], key=lambda value: int(value.rstrip("d")))
    assert list(fragility_a["regime_metrics"]) == sorted(fragility_a["regime_metrics"])
