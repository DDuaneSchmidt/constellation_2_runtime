from __future__ import annotations

import copy
from pathlib import Path

import pytest

from constellation_2.common.outcome_state_kernel_v1 import (
    OutcomeStateError,
    list_outcome_states_v1,
    materialize_outcome_state_v1,
)
from constellation_2.common.paper_session_fact_plane_v1 import atomic_write_idempotent_validated_json_v1
from constellation_2.common.product_summary_kernel_v1 import materialize_product_summary_v1
from constellation_2.common.tests.test_opportunity_state_kernel_v1 import ACCOUNT, DAY, ENV, SLEEVE
from constellation_2.common.tests.test_product_summary_kernel_v1 import _seed_product_runtime


def _strip_volatile(obj: object) -> object:
    if isinstance(obj, dict):
        return {
            key: _strip_volatile(value)
            for key, value in obj.items()
            if key not in {"generated_at_utc", "artifact_sha256"}
        }
    if isinstance(obj, list):
        return [_strip_volatile(value) for value in obj]
    return copy.deepcopy(obj)


def _materialize_product_summary(monkeypatch: pytest.MonkeyPatch, canonical_truth: Path, sleeve_root: Path) -> None:
    materialize_product_summary_v1(
        day_utc=DAY,
        sleeve_id=SLEEVE,
        environment=ENV,
        ib_account=ACCOUNT,
        canonical_truth_root=canonical_truth,
        truth_sleeves_root=sleeve_root,
        emit_artifacts=True,
    )


def _write_reconciliation_report(canonical_truth: Path, *, submissions_total: int, reason_codes: list[str]) -> Path:
    payload = {
        "schema_id": "reconciliation_report",
        "schema_version": "v3",
        "day_utc": DAY,
        "produced_utc": f"{DAY}T18:00:00Z",
        "producer": {
            "repo": "constellation",
            "module": "test_outcome_state_kernel_v1",
            "git_sha": "a" * 40,
        },
        "status": "OK",
        "reason_codes": reason_codes,
        "notes": ["fixture"],
        "input_manifest": [
            {
                "type": "fixture",
                "path": "/tmp/fixture",
                "sha256": "1" * 64,
            }
        ],
        "broker_side": {
            "broker_event_log_path": "/tmp/broker.log",
            "broker_event_log_sha256": "2" * 64,
            "broker_event_manifest_path": "/tmp/broker.manifest.json",
            "counts": {
                "broker_events_total": submissions_total,
                "execDetails_total": submissions_total,
            },
        },
        "truth_side": {
            "exec_evidence_day_dir": str((canonical_truth / "execution_evidence_v1" / "submissions" / DAY).resolve()),
            "submission_ids": (["a" * 64] if submissions_total else []),
            "counts": {
                "submissions_total": submissions_total,
            },
        },
        "comparisons": {
            "truth_submissions_vs_broker_execdetails": {
                "status": "OK" if submissions_total else "SKIPPED_SAFE_IDLE",
                "reason": "fixture",
            },
            "cash": {"status": "OK", "reason": "fixture"},
            "positions": {"status": "OK", "reason": "fixture"},
        },
    }
    path = canonical_truth / "reports" / "reconciliation_report_v3" / DAY / "reconciliation_report.v3.json"
    return atomic_write_idempotent_validated_json_v1(
        path=path,
        payload=payload,
        schema_relpath="governance/04_DATA/SCHEMAS/C2/REPORTS/reconciliation_report.v3.schema.json",
        volatile_field_names=("produced_utc",),
    ).path


def _write_fill_ledger(canonical_truth: Path, *, submission_id: str = "a" * 64, filled_qty: int = 10) -> Path:
    payload = {
        "schema_id": "C2_FILL_LEDGER_V1",
        "schema_version": 1,
        "produced_utc": f"{DAY}T18:00:00Z",
        "day_utc": DAY,
        "producer": {
            "repo": "constellation",
            "git_sha": "a" * 40,
            "module": "test_outcome_state_kernel_v1",
        },
        "status": "OK",
        "reason_codes": ["FILL_LEDGER_OK"],
        "submission_id": submission_id,
        "binding_hash": "b" * 64,
        "engine_id": "ENGINE",
        "source_intent_id": "fixture_source_intent_1234",
        "intent_sha256": "c" * 64,
        "order_qty": 10,
        "filled_qty": filled_qty,
        "remaining_qty": 10 - filled_qty,
        "avg_fill_price_weighted": "100",
        "lifecycle_status": "FILLED" if filled_qty else "OPEN",
        "event_hashes": ["d" * 64],
        "canonical_json_hash": "e" * 64,
    }
    path = canonical_truth / "fill_ledger_v1" / DAY / f"{submission_id}.fill_ledger.v1.json"
    return atomic_write_idempotent_validated_json_v1(
        path=path,
        payload=payload,
        schema_relpath="governance/04_DATA/SCHEMAS/C2/EXECUTION_EVIDENCE/fill_ledger.v1.schema.json",
        volatile_field_names=("produced_utc",),
    ).path


def test_outcome_state_is_deterministic_for_same_governed_truth(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    canonical_truth, sleeve_root, *_ = _seed_product_runtime(monkeypatch, tmp_path)
    _materialize_product_summary(monkeypatch, canonical_truth, sleeve_root)

    first = materialize_outcome_state_v1(
        day_utc=DAY,
        sleeve_id=SLEEVE,
        environment=ENV,
        ib_account=ACCOUNT,
        canonical_truth_root=canonical_truth,
        truth_sleeves_root=sleeve_root,
        emit_artifacts=False,
    )
    second = materialize_outcome_state_v1(
        day_utc=DAY,
        sleeve_id=SLEEVE,
        environment=ENV,
        ib_account=ACCOUNT,
        canonical_truth_root=canonical_truth,
        truth_sleeves_root=sleeve_root,
        emit_artifacts=False,
    )
    assert _strip_volatile(first["rows"]) == _strip_volatile(second["rows"])


def test_outcome_claim_strength_withholds_when_realized_basis_missing(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    canonical_truth, sleeve_root, *_ = _seed_product_runtime(monkeypatch, tmp_path)
    _materialize_product_summary(monkeypatch, canonical_truth, sleeve_root)
    report = materialize_outcome_state_v1(
        day_utc=DAY,
        sleeve_id=SLEEVE,
        environment=ENV,
        ib_account=ACCOUNT,
        canonical_truth_root=canonical_truth,
        truth_sleeves_root=sleeve_root,
        emit_artifacts=False,
    )
    assert all(row["claim_strength"] in {"not_yet_observable", "insufficient_evidence"} for row in report["rows"])
    assert all(row["attribution_state"] == "unsupported" for row in report["rows"])


def test_outcome_effectiveness_and_bounded_attribution_are_conservative(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    canonical_truth, sleeve_root, *_ = _seed_product_runtime(monkeypatch, tmp_path, blocked_readiness=True)
    _materialize_product_summary(monkeypatch, canonical_truth, sleeve_root)
    _write_reconciliation_report(canonical_truth, submissions_total=0, reason_codes=["NO_SUBMISSIONS_FOUND"])
    report = materialize_outcome_state_v1(
        day_utc=DAY,
        sleeve_id=SLEEVE,
        environment=ENV,
        ib_account=ACCOUNT,
        canonical_truth_root=canonical_truth,
        truth_sleeves_root=sleeve_root,
        comparison_type="blocked_vs_allowed",
        emit_artifacts=False,
    )
    blocked = [row for row in report["rows"] if row["opportunity_type"] == "platform_blocker_review"][0]
    assert blocked["effectiveness_state"] == "protective"
    assert blocked["attribution_state"] == "bounded"
    assert blocked["claim_strength"] == "bounded_association"
    assert blocked["comparison_state"]["comparison_status"] == "applied"


def test_outcome_no_speculative_attribution_on_realized_activity(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    canonical_truth, sleeve_root, *_ = _seed_product_runtime(monkeypatch, tmp_path)
    _materialize_product_summary(monkeypatch, canonical_truth, sleeve_root)
    _write_fill_ledger(canonical_truth, filled_qty=10)
    _write_reconciliation_report(canonical_truth, submissions_total=1, reason_codes=["RECONCILIATION_OK"])
    report = materialize_outcome_state_v1(
        day_utc=DAY,
        sleeve_id=SLEEVE,
        environment=ENV,
        ib_account=ACCOUNT,
        canonical_truth_root=canonical_truth,
        truth_sleeves_root=sleeve_root,
        emit_artifacts=False,
    )
    observed_rows = [row for row in report["rows"] if row["realized_state"] == "realized_activity_observed"]
    assert observed_rows
    assert all(row["attribution_state"] == "unsupported" for row in observed_rows)
    assert all(row["claim_strength"] == "observed_fact" for row in observed_rows)


def test_outcome_comparison_rejects_unsupported_type(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    canonical_truth, sleeve_root, *_ = _seed_product_runtime(monkeypatch, tmp_path)
    _materialize_product_summary(monkeypatch, canonical_truth, sleeve_root)
    report = materialize_outcome_state_v1(
        day_utc=DAY,
        sleeve_id=SLEEVE,
        environment=ENV,
        ib_account=ACCOUNT,
        canonical_truth_root=canonical_truth,
        truth_sleeves_root=sleeve_root,
        comparison_type="tax_aware_vs_naive",
        emit_artifacts=False,
    )
    assert all(row["comparison_state"]["comparison_status"] == "rejected" for row in report["rows"])
    assert all(row["comparison_state"]["reason_id"] == "OUTCOME_COMPARISON_UNSUPPORTED_TYPE" for row in report["rows"])


def test_outcome_explanation_and_lineage_are_reconstructable(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    canonical_truth, sleeve_root, *_ = _seed_product_runtime(monkeypatch, tmp_path, blocked_readiness=True)
    _materialize_product_summary(monkeypatch, canonical_truth, sleeve_root)
    first = materialize_outcome_state_v1(
        day_utc=DAY,
        sleeve_id=SLEEVE,
        environment=ENV,
        ib_account=ACCOUNT,
        canonical_truth_root=canonical_truth,
        truth_sleeves_root=sleeve_root,
        emit_artifacts=True,
    )
    _write_reconciliation_report(canonical_truth, submissions_total=0, reason_codes=["NO_SUBMISSIONS_FOUND"])
    second = materialize_outcome_state_v1(
        day_utc=DAY,
        sleeve_id=SLEEVE,
        environment=ENV,
        ib_account=ACCOUNT,
        canonical_truth_root=canonical_truth,
        truth_sleeves_root=sleeve_root,
        comparison_type="blocked_vs_allowed",
        emit_artifacts=True,
    )
    rows = list_outcome_states_v1(canonical_truth_root=canonical_truth, day_utc=DAY)
    assert rows
    assert any("primary_explanation" in row.payload for row in rows)
    assert any("supersedes_ref" in row.payload for row in rows if row.payload["claim_strength"] == "bounded_association")
    assert first["artifact_refs"]
    assert second["artifact_refs"]


def test_outcome_fails_closed_without_product_summary(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    canonical_truth, sleeve_root, *_ = _seed_product_runtime(monkeypatch, tmp_path)
    with pytest.raises(OutcomeStateError, match="OUTCOME_STATE_REQUIRES_PRODUCT_SUMMARY"):
        materialize_outcome_state_v1(
            day_utc=DAY,
            sleeve_id=SLEEVE,
            environment=ENV,
            ib_account=ACCOUNT,
            canonical_truth_root=canonical_truth,
            truth_sleeves_root=sleeve_root,
            emit_artifacts=False,
        )


def test_outcome_rejects_forbidden_repo_local_truth_root(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    canonical_truth, sleeve_root, *_ = _seed_product_runtime(monkeypatch, tmp_path)
    _materialize_product_summary(monkeypatch, canonical_truth, sleeve_root)
    with pytest.raises(OutcomeStateError, match="OUTCOME_STATE_FORBIDDEN_TRUTH_ROOT"):
        materialize_outcome_state_v1(
            day_utc=DAY,
            sleeve_id=SLEEVE,
            environment=ENV,
            ib_account=ACCOUNT,
            canonical_truth_root=Path("/home/node/constellation/constellation_2/runtime/truth"),
            truth_sleeves_root=sleeve_root,
            emit_artifacts=False,
        )
