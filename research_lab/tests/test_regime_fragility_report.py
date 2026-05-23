from __future__ import annotations

from pathlib import Path

from research_lab.contracts.schemas import validate_contract
from research_lab.stability.regime_fragility import build_regime_fragility_report, write_regime_fragility_report
from research_lab.storage.manifest_io import read_jsonl, write_json
from research_lab.storage.parquet_io import write_parquet_records


SLEEVE_ID = "slv_fixture"
SLEEVE_VERSION_ID = "slvv_fixture"


def _write_run(store: Path, rows: list[dict], run_id: str = "lcr_fixture") -> str:
    root = store / "longitudinal_runs" / run_id
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
    write_parquet_records(root / "candidate_batch_index.parquet", [{"candidate_count": len({row["candidate_id"] for row in rows})}], allow_json_fallback=True)
    write_parquet_records(root / "outcome_index.parquet", rows, allow_json_fallback=True)
    return run_id


def _row(idx: int, value: float, risk_regime: str) -> dict:
    return {
        "candidate_id": f"cand_{idx:03d}",
        "as_of_date": f"2024-02-{(idx % 28) + 1:02d}",
        "outcome_window": "5d",
        "post_cost_return": value,
        "excess_return": value / 2,
        "outcome_status": "measured",
        "risk_regime": risk_regime,
    }


def test_regime_fragility_report_schema_validates(tmp_path: Path) -> None:
    rows = [_row(idx, 0.01, "risk_on") for idx in range(35)]
    run_id = _write_run(tmp_path / "store", rows)

    report = build_regime_fragility_report(
        sleeve_id=SLEEVE_ID,
        sleeve_version_id=SLEEVE_VERSION_ID,
        longitudinal_run_id=run_id,
        store_root=tmp_path / "store",
    )

    validate_contract("regime_fragility_report", report)
    assert report["schema_version"] == "regime_fragility_report.v1"
    assert "hypothetical research evidence" in report["research_label"]


def test_fragility_report_detects_fragile_regime_spread(tmp_path: Path) -> None:
    rows = [_row(idx, 0.02, "risk_on") for idx in range(20)]
    rows += [_row(idx + 20, -0.02, "risk_off") for idx in range(20)]
    run_id = _write_run(tmp_path / "store", rows)

    report = build_regime_fragility_report(
        sleeve_id=SLEEVE_ID,
        sleeve_version_id=SLEEVE_VERSION_ID,
        longitudinal_run_id=run_id,
        store_root=tmp_path / "store",
    )

    assert report["fragility_status"] == "fragile"
    assert report["recommended_action"] == "challenge_sleeve"


def test_fragility_report_detects_thin_regime_sample_as_watch(tmp_path: Path) -> None:
    rows = [_row(idx, 0.01, "risk_on") for idx in range(15)]
    rows += [_row(idx + 15, 0.011, "risk_off") for idx in range(15)]
    run_id = _write_run(tmp_path / "store", rows)

    report = build_regime_fragility_report(
        sleeve_id=SLEEVE_ID,
        sleeve_version_id=SLEEVE_VERSION_ID,
        longitudinal_run_id=run_id,
        store_root=tmp_path / "store",
    )

    assert report["fragility_status"] == "watch"
    assert "regime_sample_sizes_are_thin" in report["fragility_reasons"]


def test_regime_fragility_registry_appends(tmp_path: Path) -> None:
    store = tmp_path / "store"
    run_id = _write_run(store, [_row(idx, 0.01, "risk_on") for idx in range(35)])

    report = write_regime_fragility_report(
        sleeve_id=SLEEVE_ID,
        sleeve_version_id=SLEEVE_VERSION_ID,
        longitudinal_run_id=run_id,
        store_root=store,
    )

    rows = read_jsonl(store / "registries" / "regime_fragility_reports.jsonl")
    assert rows[-1]["regime_fragility_report_id"] == report["regime_fragility_report_id"]
