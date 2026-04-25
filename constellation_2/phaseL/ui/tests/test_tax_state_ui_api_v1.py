from __future__ import annotations

import json
from pathlib import Path

import pytest

from constellation_2.common.tax.storage_v1 import append_validated_jsonl_v1
from constellation_2.common.tax.truth_v1 import (
    accept_tax_fact_candidate_v1,
    build_tax_fact_candidate_v1,
    build_tax_observed_event_v1,
)
from constellation_2.common.tax_state_kernel_v1 import materialize_tax_state_v1
from constellation_2.phaseL.ui_api.tax_state_read_model import build_tax_state_view


DAY = "2026-04-17"
SLEEVE = "PRIMARY"
ENV = "PAPER"
ACCOUNT = "DUO847203"
GIT_SHA = "a" * 40
TAX_SCOPE_ID = "filing:joint"


def _write_json(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")


def _accepted_fact(event_family: str, payload: dict[str, str], recorded_at: str) -> dict[str, object]:
    observed = build_tax_observed_event_v1(
        event_family=event_family,
        scope_ids=(TAX_SCOPE_ID,),
        payload=payload,
        source_event_ref=f"{event_family}:{recorded_at}",
        observed_at=recorded_at,
        recorded_at=recorded_at,
    )
    candidate = build_tax_fact_candidate_v1(observed)
    _, accepted = accept_tax_fact_candidate_v1(candidate)
    assert accepted is not None
    return accepted


def _seed_tax_runtime(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> tuple[Path, Path]:
    global_truth = tmp_path / "runtime" / "truth"
    repo_root = tmp_path / "repo"

    monkeypatch.setattr("constellation_2.phaseL.ui_api.tax_state_read_model.GLOBAL_TRUTH_ROOT", global_truth)

    _write_json(
        global_truth / "accounting_v2" / "nav" / DAY / "nav.v2.json",
        {
            "schema_id": "C2_ACCOUNTING_NAV_V2",
            "schema_version": 2,
            "produced_utc": f"{DAY}T16:00:00Z",
            "day_utc": DAY,
            "producer": {"repo": "constellation", "git_sha": GIT_SHA, "module": "test_tax_state_ui_api_v1.py"},
            "status": "ACTIVE",
            "reason_codes": ["BROKER_MARKS_SOURCE_V1"],
            "input_manifest": [{"type": "cash_ledger_snapshot_v1", "path": "/tmp/cash.json", "sha256": "1" * 64}],
            "nav": {
                "currency": "USD",
                "nav_total": "100000.00",
                "cash_total": "50000.00",
                "gross_positions_value": "50000.00",
                "realized_pnl_to_date": "123.45",
                "unrealized_pnl": "456.78",
                "components": [{"symbol": "AAPL", "qty": "10", "mv": "1000.00"}],
                "notes": [],
            },
            "history": {},
        },
    )
    _write_json(
        global_truth / "cash_ledger_v1" / "snapshots" / DAY / "cash_ledger_snapshot.v1.json",
        {
            "schema_id": "C2_CASH_LEDGER_SNAPSHOT_V1",
            "schema_version": 1,
            "produced_utc": f"{DAY}T16:05:00Z",
            "day_utc": DAY,
            "authority_basis": "broker_account_values",
            "producer": {"repo": "constellation", "git_sha": GIT_SHA, "module": "test_tax_state_ui_api_v1.py"},
            "status": "OK",
            "reason_codes": ["BROKER_ACCOUNT_VALUES"],
            "input_manifest": [
                {
                    "type": "broker_account_values",
                    "path": "/tmp/broker_account_values.json",
                    "sha256": "2" * 64,
                    "day_utc": DAY,
                    "producer": "fixture",
                }
            ],
            "snapshot": {
                "observed_at_utc": f"{DAY}T16:05:00Z",
                "currency": "USD",
                "cash_total_cents": 5000000,
                "nlv_total_cents": 10000000,
                "available_funds_cents": 5000000,
                "excess_liquidity_cents": 5000000,
                "account_id": "DUO847203",
                "notes": [],
            },
        },
    )
    _write_json(
        global_truth / "positions_v1" / "snapshots" / DAY / "positions_snapshot.v5.json",
        {
            "schema_id": "C2_POSITIONS_SNAPSHOT_V5",
            "schema_version": 5,
            "day_utc": DAY,
            "produced_utc": f"{DAY}T16:10:00Z",
            "producer": {"repo": "constellation", "git_sha": GIT_SHA, "module": "test_tax_state_ui_api_v1.py"},
            "status": "OK",
            "reason_codes": ["POSITIONS_SNAPSHOT_OK"],
            "input_manifest": [
                {
                    "type": "execution_positions_snapshot_v5",
                    "path": "/tmp/positions_input.json",
                    "sha256": "3" * 64,
                    "day_utc": DAY,
                    "producer": "fixture",
                }
            ],
            "accounts": [
                {
                    "account_id": "DUO847203",
                    "cash_total_cents": 5000000,
                    "broker_cash_cents": 5000000,
                    "currency": "USD",
                    "cash_source": "CASH_LEDGER_ONLY",
                    "reason_codes": ["CASH_OK"],
                }
            ],
            "items": [
                {
                    "position_id": "position:AAPL",
                    "account_id": "DUO847203",
                    "origin": "NATIVE",
                    "engine_id": "paper_engine_v1",
                    "source_intent_id": "intent:AAPL",
                    "intent_sha256": "4" * 64,
                    "instrument": {"kind": "equity", "symbol": "AAPL"},
                    "qty": 10,
                    "avg_cost_cents": 15000,
                    "opened_day_utc": "2025-01-01",
                    "last_transition_utc": f"{DAY}T16:10:00Z",
                    "last_transition_type": "OPEN",
                    "lifecycle_state": "OPEN",
                    "lifecycle_reason_code": "POSITION_OPEN",
                    "status": "OPEN",
                    "lots": [
                        {
                            "lot_id": "LOT-AAPL-1",
                            "direction": "LONG",
                            "opened_day_utc": "2025-01-01",
                            "remaining_qty_abs": 10,
                            "cost_basis_cents": 150000,
                            "source_kind": "NATIVE_FILL",
                            "source_ref": "fixture-lot",
                        }
                    ],
                    "reconciliation": {
                        "broker_position_present": True,
                        "broker_qty": "10",
                        "status": "MATCH",
                        "reason_codes": ["MATCH"],
                    },
                }
            ],
            "reconciliation": {
                "broker_statement_present": False,
                "broker_statement_path": None,
                "cash_status": "MATCH",
                "cash_delta_cents": 0,
                "positions_status": "MATCH",
                "reason_codes": ["MATCH"],
                "position_mismatches": [],
            },
            "canonical_json_hash": "5" * 64,
        },
    )
    _write_json(
        repo_root / "governance" / "02_REGISTRIES" / "C2_IB_ACCOUNT_REGISTRY_V1.json",
        {
            "schema_id": "c2_ib_account_registry",
            "accounts": [
                {
                    "account_id": "DUO847203",
                    "environment": "PAPER",
                    "enabled_for_submission": True,
                }
            ],
        },
    )
    monkeypatch.setattr(
        "constellation_2.common.tax_state_kernel_v1.ACCOUNT_REGISTRY_PATH",
        repo_root / "governance" / "02_REGISTRIES" / "C2_IB_ACCOUNT_REGISTRY_V1.json",
    )
    return global_truth, global_truth.parent / "truth_sleeves" / SLEEVE / ENV


def test_tax_state_view_stays_missing_without_tax_artifact_even_when_raw_runtime_truth_exists(
    monkeypatch, tmp_path: Path
) -> None:
    _seed_tax_runtime(monkeypatch, tmp_path)

    payload = build_tax_state_view(DAY)

    assert payload["view_name"] == "tax_state"
    assert payload["tax_status"] == "MISSING"
    assert payload["tax_warnings"] == ["TAX_STATE_ARTIFACT_MISSING"]
    assert payload["lot_level_entries"]["status"] == "unavailable"
    assert payload["account_tax_profiles"] == []


def test_tax_state_view_renders_governed_tax_artifact_and_bound_advisory_only(
    monkeypatch, tmp_path: Path
) -> None:
    global_truth, sleeve_truth = _seed_tax_runtime(monkeypatch, tmp_path)
    journal_path = (
        global_truth
        / "journals"
        / "accepted_tax_fact_v1"
        / "scopes"
        / TAX_SCOPE_ID
        / "accepted_tax_fact.v1.jsonl"
    )
    append_validated_jsonl_v1(
        path=journal_path,
        payloads=(
            _accepted_fact(
                "account_classification_tax_regime",
                {"account_id": ACCOUNT, "tax_regime": "taxable_brokerage"},
                f"{DAY}T10:00:00Z",
            ),
        ),
        schema_relpath="governance/04_DATA/SCHEMAS/C2/TAX/accepted_tax_fact.v1.schema.json",
    )
    tax_report = materialize_tax_state_v1(
        day_utc=DAY,
        sleeve_id=SLEEVE,
        environment=ENV,
        ib_account=ACCOUNT,
        tax_scope_id=TAX_SCOPE_ID,
        canonical_truth_root=global_truth,
        truth_sleeves_root=sleeve_truth,
        emit_artifact=True,
        evaluated_at_utc=f"{DAY}T16:30:00Z",
    )

    payload = build_tax_state_view(DAY)

    assert payload["tax_status"] == "OK"
    assert payload["completeness_state"] == "complete"
    assert payload["opportunity_states"] == ["harvest_candidate_available"]
    assert payload["lot_level_entries"]["status"] == "available"
    assert payload["harvesting_candidates"]["status"] == "available"
    assert payload["tax_state_id"] == tax_report["tax_state"]["tax_state_id"]
    assert payload["source_refs"][0]["artifact_type"] == "tax_state_v1"


def test_tax_page_reads_tax_projection_not_blocked_domain() -> None:
    root = Path(__file__).resolve().parents[4]
    domain_client = (
        root / "constellation_2" / "phaseL" / "ui" / "static" / "operator_shell" / "domain_client" / "index.js"
    ).read_text(encoding="utf-8")
    pages = (
        root / "constellation_2" / "phaseL" / "ui" / "static" / "operator_shell" / "pages" / "index.js"
    ).read_text(encoding="utf-8")

    assert 'query("/api/tax")' in domain_client
    assert "async function renderTaxPage" in pages
    switch_section = pages.split("export async function loadRouteView", 1)[1]
    assert 'case "tax":' in switch_section
    assert "return renderTaxPage(state);" in switch_section
