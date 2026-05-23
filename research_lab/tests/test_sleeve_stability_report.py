from __future__ import annotations

from pathlib import Path

from ops.aegis.research_lab.research_lab_routes import research_lab_sleeve_stability_v1
from research_lab.contracts.schemas import validate_contract
from research_lab.stability.expectancy_drift import write_expectancy_drift_report
from research_lab.stability.regime_fragility import write_regime_fragility_report
from research_lab.stability.sleeve_stability import build_sleeve_stability_report, write_sleeve_stability_report
from research_lab.storage.manifest_io import read_jsonl, write_json
from research_lab.storage.parquet_io import write_parquet_records


SLEEVE_ID = "slv_fixture"
SLEEVE_VERSION_ID = "slvv_fixture"


def _write_run(store: Path, values: list[float], run_id: str = "lcr_fixture") -> str:
    root = store / "longitudinal_runs" / run_id
    rows = [
        {
            "candidate_id": f"cand_{idx:03d}",
            "as_of_date": f"2024-03-{(idx % 28) + 1:02d}",
            "outcome_window": "5d",
            "post_cost_return": value,
            "excess_return": value / 2,
            "outcome_status": "measured",
            "risk_regime": "risk_on",
        }
        for idx, value in enumerate(values)
    ]
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


def _write_inputs(store: Path, values: list[float]) -> tuple[dict, dict]:
    run_id = _write_run(store, values)
    drift = write_expectancy_drift_report(
        sleeve_id=SLEEVE_ID,
        sleeve_version_id=SLEEVE_VERSION_ID,
        longitudinal_run_id=run_id,
        store_root=store,
    )
    fragility = write_regime_fragility_report(
        sleeve_id=SLEEVE_ID,
        sleeve_version_id=SLEEVE_VERSION_ID,
        longitudinal_run_id=run_id,
        store_root=store,
    )
    return drift, fragility


def test_sleeve_stability_report_schema_validates_and_combines_statuses(tmp_path: Path) -> None:
    store = tmp_path / "store"
    drift, fragility = _write_inputs(store, [0.01] * 45)

    report = build_sleeve_stability_report(
        sleeve_id=SLEEVE_ID,
        sleeve_version_id=SLEEVE_VERSION_ID,
        expectancy_drift_report_id=drift["expectancy_drift_report_id"],
        regime_fragility_report_id=fragility["regime_fragility_report_id"],
        store_root=store,
    )

    validate_contract("sleeve_stability_report", report)
    assert report["overall_stability_status"] in {"stable", "watch"}
    assert "No live trading" in report["research_label"]


def test_sleeve_stability_degrading_takes_precedence(tmp_path: Path) -> None:
    store = tmp_path / "store"
    drift, fragility = _write_inputs(store, [0.02] * 25 + [-0.02] * 25)

    report = build_sleeve_stability_report(
        sleeve_id=SLEEVE_ID,
        sleeve_version_id=SLEEVE_VERSION_ID,
        expectancy_drift_report_id=drift["expectancy_drift_report_id"],
        regime_fragility_report_id=fragility["regime_fragility_report_id"],
        store_root=store,
    )

    assert report["overall_stability_status"] == "degrading"
    assert report["recommended_action"] == "challenge_sleeve"


def test_sleeve_stability_registry_appends_and_route_is_read_only(tmp_path: Path) -> None:
    store = tmp_path / "store"
    drift, fragility = _write_inputs(store, [0.01] * 45)
    before = sorted(path.relative_to(store).as_posix() for path in store.rglob("*") if path.is_file())

    report = write_sleeve_stability_report(
        sleeve_id=SLEEVE_ID,
        sleeve_version_id=SLEEVE_VERSION_ID,
        expectancy_drift_report_id=drift["expectancy_drift_report_id"],
        regime_fragility_report_id=fragility["regime_fragility_report_id"],
        store_root=store,
    )
    route_payload = research_lab_sleeve_stability_v1(sleeve_id=SLEEVE_ID, store_root=store)
    after_route = sorted(path.relative_to(store).as_posix() for path in store.rglob("*") if path.is_file())

    rows = read_jsonl(store / "registries" / "sleeve_stability_reports.jsonl")
    assert rows[-1]["sleeve_stability_report_id"] == report["sleeve_stability_report_id"]
    assert route_payload["read_only"] is True
    assert route_payload["sleeve_stability"]["sleeve_stability_report_id"] == report["sleeve_stability_report_id"]
    assert after_route != before
    assert after_route == sorted(path.relative_to(store).as_posix() for path in store.rglob("*") if path.is_file())


def test_no_execution_or_approval_fields_appear(tmp_path: Path) -> None:
    store = tmp_path / "store"
    drift, fragility = _write_inputs(store, [0.01] * 45)
    report = build_sleeve_stability_report(
        sleeve_id=SLEEVE_ID,
        sleeve_version_id=SLEEVE_VERSION_ID,
        expectancy_drift_report_id=drift["expectancy_drift_report_id"],
        regime_fragility_report_id=fragility["regime_fragility_report_id"],
        store_root=store,
    )

    text = str(report).lower()
    assert "broker execution" in report["research_label"]
    assert "order_management" not in text
    assert "capital_allocation" not in text
    assert "automatic_promotion" not in text
