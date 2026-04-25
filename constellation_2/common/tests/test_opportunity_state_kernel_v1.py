from __future__ import annotations

import copy
import json
from pathlib import Path

import pytest

from constellation_2.common.advisory_decision_state_kernel_v1 import (
    find_latest_advisory_decision_state_v1,
    materialize_advisory_decision_state_v1,
)
from constellation_2.common.control_plane_validation_kernel_v1 import _build_scope
from constellation_2.common.opportunity_explanation_mapping_v1 import map_opportunity_explanation_v1
from constellation_2.common.opportunity_review_snapshot_v1 import (
    find_latest_opportunity_review_snapshot_v1,
)
from constellation_2.common.opportunity_scenario_significance_v1 import (
    evaluate_opportunity_scenario_significance_v1,
)
from constellation_2.common.opportunity_state_kernel_v1 import (
    find_latest_opportunity_state_v1,
    list_opportunity_states_v1,
    materialize_opportunity_state_v1,
)
from constellation_2.common.opportunity_state_precedence_v1 import (
    evaluate_opportunity_state_precedence_v1,
)
from constellation_2.common.paper_session_fact_plane_v1 import (
    atomic_write_idempotent_validated_json_v1,
)
from constellation_2.common.tax.storage_v1 import append_validated_jsonl_v1
from constellation_2.common.tax.truth_v1 import (
    accept_tax_fact_candidate_v1,
    build_tax_fact_candidate_v1,
    build_tax_observed_event_v1,
)
from constellation_2.common.tax_state_kernel_v1 import materialize_tax_state_v1
from constellation_2.common.tests.test_advisory_decision_state_v1 import (
    _bundle5_session_chain,
    _seed_deployment_state,
)
from constellation_2.phaseD.lib.canon_json_v1 import canonical_hash_for_c2_artifact_v1


DAY = "2026-04-17"
PREV_DAY = "2026-04-16"
SLEEVE = "PRIMARY"
ENV = "PAPER"
ACCOUNT = "DUO847203"
GIT_SHA = "a" * 40
TAX_SCOPE_ID = "filing:joint"
REPO_ROOT = Path(__file__).resolve().parents[3]


def _write_json(path: Path, payload: object) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(payload, sort_keys=True, separators=(",", ":"), ensure_ascii=False) + "\n",
        encoding="utf-8",
    )


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


def _scope_id(canonical_truth_root: Path, truth_sleeves_root: Path) -> str:
    return _build_scope(
        repo_root=REPO_ROOT,
        day_utc=DAY,
        sleeve_id=SLEEVE,
        environment=ENV,
        ib_account=ACCOUNT,
        operation_type="fresh_paper_entry_v1",
        candidate_path=None,
        canonical_truth_root=canonical_truth_root,
        truth_sleeves_root=truth_sleeves_root,
    ).scope_id


def _seed_tax_runtime_inputs(
    canonical_truth: Path,
    truth_sleeves: Path,
    monkeypatch: pytest.MonkeyPatch,
    *,
    positions_status: str = "OK",
    cost_basis_cents: int = 150000,
    quantity: int = 10,
    market_value: str = "1000.00",
) -> Path:
    registry_path = canonical_truth.parent / "repo" / "governance" / "02_REGISTRIES" / "C2_IB_ACCOUNT_REGISTRY_V1.json"
    monkeypatch.setattr(
        "constellation_2.common.tax_state_kernel_v1.ACCOUNT_REGISTRY_PATH",
        registry_path,
    )
    _write_json(
        registry_path,
        {
            "schema_id": "c2_ib_account_registry",
            "accounts": [
                {
                    "account_id": ACCOUNT,
                    "environment": ENV,
                    "enabled_for_submission": True,
                }
            ],
        },
    )
    _write_json(
        canonical_truth / "accounting_v2" / "nav" / DAY / "nav.v2.json",
        {
            "schema_id": "C2_ACCOUNTING_NAV_V2",
            "schema_version": 2,
            "produced_utc": f"{DAY}T16:00:00Z",
            "day_utc": DAY,
            "producer": {
                "repo": "constellation",
                "git_sha": GIT_SHA,
                "module": "test_opportunity_state_kernel_v1.py",
            },
            "status": "ACTIVE",
            "reason_codes": ["BROKER_MARKS_SOURCE_V1"],
            "input_manifest": [
                {
                    "type": "cash_ledger_snapshot_v1",
                    "path": "/tmp/cash.json",
                    "sha256": "1" * 64,
                }
            ],
            "nav": {
                "currency": "USD",
                "nav_total": "110000.00",
                "cash_total": "10000.00",
                "gross_positions_value": "100000.00",
                "realized_pnl_to_date": "0.00",
                "unrealized_pnl": "-500.00",
                "components": [{"symbol": "AAPL", "qty": str(quantity), "mv": market_value}],
                "notes": [],
            },
            "history": {},
        },
    )
    _write_json(
        canonical_truth / "cash_ledger_v1" / "snapshots" / DAY / "cash_ledger_snapshot.v1.json",
        {
            "schema_id": "C2_CASH_LEDGER_SNAPSHOT_V1",
            "schema_version": 1,
            "produced_utc": f"{DAY}T16:05:00Z",
            "day_utc": DAY,
            "authority_basis": "broker_account_values",
            "producer": {
                "repo": "constellation",
                "git_sha": GIT_SHA,
                "module": "test_opportunity_state_kernel_v1.py",
            },
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
                "cash_total_cents": 1000000,
                "nlv_total_cents": 11000000,
                "available_funds_cents": 1000000,
                "excess_liquidity_cents": 1000000,
                "account_id": ACCOUNT,
                "notes": [],
            },
        },
    )
    _write_json(
        canonical_truth / "positions_v1" / "snapshots" / DAY / "positions_snapshot.v5.json",
        {
            "schema_id": "C2_POSITIONS_SNAPSHOT_V5",
            "schema_version": 5,
            "day_utc": DAY,
            "produced_utc": f"{DAY}T16:10:00Z",
            "producer": {
                "repo": "constellation",
                "git_sha": GIT_SHA,
                "module": "test_opportunity_state_kernel_v1.py",
            },
            "status": positions_status,
            "reason_codes": ["POSITIONS_SNAPSHOT_OK"] if positions_status == "OK" else ["POSITIONS_SNAPSHOT_DEGRADED"],
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
                    "account_id": ACCOUNT,
                    "currency": "USD",
                    "cash_total_cents": 1000000,
                    "broker_cash_cents": 1000000,
                    "cash_source": "CASH_LEDGER_ONLY",
                    "reason_codes": ["CASH_OK"],
                }
            ],
            "items": [
                {
                    "position_id": "position:AAPL",
                    "account_id": ACCOUNT,
                    "origin": "NATIVE",
                    "engine_id": "paper_engine_v1",
                    "source_intent_id": "intent:AAPL",
                    "intent_sha256": "4" * 64,
                    "instrument": {"kind": "equity", "symbol": "AAPL"},
                    "qty": quantity,
                    "avg_cost_cents": int(cost_basis_cents / quantity) if quantity else 0,
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
                            "remaining_qty_abs": quantity,
                            "cost_basis_cents": cost_basis_cents,
                            "source_kind": "NATIVE_FILL",
                            "source_ref": "fixture-lot",
                        }
                    ],
                    "reconciliation": {
                        "broker_position_present": True,
                        "broker_qty": str(quantity),
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
    return registry_path


def _write_tax_journal(canonical_truth: Path, facts: list[dict[str, object]]) -> Path:
    path = (
        canonical_truth
        / "journals"
        / "accepted_tax_fact_v1"
        / "scopes"
        / TAX_SCOPE_ID
        / "accepted_tax_fact.v1.jsonl"
    )
    if path.exists():
        path.unlink()
    append_validated_jsonl_v1(
        path=path,
        payloads=facts,
        schema_relpath="governance/04_DATA/SCHEMAS/C2/TAX/accepted_tax_fact.v1.schema.json",
    )
    return path


def _materialize_tax_state(
    canonical_truth: Path,
    truth_sleeves: Path,
    *,
    emit_artifact: bool = True,
    evaluated_at_utc: str = f"{DAY}T16:30:00Z",
) -> dict:
    return materialize_tax_state_v1(
        day_utc=DAY,
        sleeve_id=SLEEVE,
        environment=ENV,
        ib_account=ACCOUNT,
        canonical_truth_root=canonical_truth,
        truth_sleeves_root=truth_sleeves,
        tax_scope_id=TAX_SCOPE_ID,
        emit_artifact=emit_artifact,
        evaluated_at_utc=evaluated_at_utc,
    )


def _materialize_advisory(
    *,
    canonical_truth: Path,
    truth_sleeves: Path,
    stage_result: dict,
    chain_path: Path,
    deployment_path: Path,
    tax_state_path: Path | None = None,
    opportunity_state_path: Path | None = None,
    advisory_authority_class: str = "recommendation",
    advisory_item_id: str = "primary_advisory_surface",
    emit_artifact: bool = True,
) -> dict:
    return materialize_advisory_decision_state_v1(
        day_utc=DAY,
        sleeve_id=SLEEVE,
        environment=ENV,
        ib_account=ACCOUNT,
        stage_path=stage_result["stage_ref"]["artifact_path"],
        transition_record_path=stage_result["transition_record_ref"]["artifact_path"],
        certification_path=stage_result["stage_certification_ref"]["artifact_path"],
        startup_chain_certification_path=str(chain_path),
        deployment_state_path=str(deployment_path),
        tax_state_path=(str(tax_state_path) if tax_state_path is not None else None),
        opportunity_state_path=(str(opportunity_state_path) if opportunity_state_path is not None else None),
        advisory_item_id=advisory_item_id,
        advisory_surface_label="operator_shell_advisory",
        advisory_authority_class=advisory_authority_class,
        canonical_truth_root=canonical_truth,
        truth_sleeves_root=truth_sleeves,
        emit_artifact=emit_artifact,
    )


def _scenario_path(canonical_truth: Path, run_id: str = "stress-run-1") -> Path:
    return (
        canonical_truth
        / "reports"
        / "stress_case_view_v1"
        / DAY
        / run_id
        / "stress_case_view.v1.json"
    ).resolve()


def _write_scenario(path: Path, *, support_status: str, scenario_ids: list[str], outcomes: list[str], summary: str) -> Path:
    _write_json(
        path,
        {
            "schema_id": "stress_case_view",
            "schema_version": "v1",
            "authority_class": "view_authority",
            "support_status": support_status,
            "produced_utc": f"{DAY}T17:00:00Z",
            "run_id": path.parent.name,
            "planning_snapshot_id": "planning-snapshot-1",
            "scenario_ids": scenario_ids,
            "outcomes": outcomes,
            "summary": summary,
        },
    )
    return path


def _strip_volatile(obj: object) -> object:
    if isinstance(obj, dict):
        return {key: _strip_volatile(value) for key, value in obj.items() if key != "generated_at_utc"}
    if isinstance(obj, list):
        return [_strip_volatile(value) for value in obj]
    return copy.deepcopy(obj)


def _write_prior_snapshot(canonical_truth: Path, scope_id: str) -> Path:
    snapshot_id = "review-snapshot-prior"
    path = (
        canonical_truth
        / "reports"
        / "opportunity_review_snapshot_v1"
        / PREV_DAY
        / scope_id
        / snapshot_id
        / "opportunity_review_snapshot.v1.json"
    )
    payload = {
        "schema_id": "opportunity_review_snapshot",
        "schema_version": "v1",
        "artifact_id": "opportunity_review_snapshot_v1",
        "surface_kind": "projection",
        "review_snapshot_id": snapshot_id,
        "kernel_version": "fixture.kernel",
        "generated_at_utc": f"{PREV_DAY}T20:00:00Z",
        "authority_label": "governed_certified_opportunity_review",
        "target_day": PREV_DAY,
        "scope_id": scope_id,
        "opportunity_rows": [
            {
                "opportunity_id": canonical_hash_for_c2_artifact_v1(
                    {"type": "advisory_action_review", "advisory_item_id": "primary_advisory_surface"}
                ),
                "opportunity_type": "advisory_action_review",
                "opportunity_state": "actionable",
                "review_priority": "review_now",
                "delta_state": "unchanged",
                "summary_fingerprint": "prior-fingerprint",
            },
            {
                "opportunity_id": "resolved-opportunity-id",
                "opportunity_type": "tax_harvest_review",
                "opportunity_state": "actionable",
                "review_priority": "review_now",
                "delta_state": "unchanged",
                "summary_fingerprint": "resolved-fingerprint",
            },
        ],
        "resolved_rows": [],
        "summary": {
            "current_count": 2,
            "new_count": 0,
            "changed_count": 0,
            "resolved_count": 0,
            "review_now_count": 2,
        },
    }
    atomic_write_idempotent_validated_json_v1(
        path=path,
        payload=payload,
        schema_relpath="governance/04_DATA/SCHEMAS/C2/RUNTIME/opportunity_review_snapshot.v1.schema.json",
        volatile_field_names=("generated_at_utc",),
    )
    return path


def _build_truth_with_tax_and_advisory(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    *,
    positions_status: str = "OK",
    with_tax_facts: bool = True,
    materialize_advisory: bool = True,
) -> tuple[Path, Path, dict, Path, Path, dict | None]:
    canonical_truth, sleeve_root, stage_result, chain_path = _bundle5_session_chain(tmp_path, monkeypatch)
    _seed_tax_runtime_inputs(canonical_truth, sleeve_root, monkeypatch, positions_status=positions_status)
    if with_tax_facts:
        _write_tax_journal(
            canonical_truth,
            [
                _accepted_fact(
                    "account_classification_tax_regime",
                    {"account_id": ACCOUNT, "tax_regime": "taxable_brokerage"},
                    f"{DAY}T10:00:00Z",
                )
            ],
        )
    tax_report = _materialize_tax_state(canonical_truth, sleeve_root, emit_artifact=True)
    deployment_path = _seed_deployment_state(canonical_truth)
    advisory_report = None
    if materialize_advisory:
            advisory_report = _materialize_advisory(
                canonical_truth=canonical_truth,
                truth_sleeves=sleeve_root,
                stage_result=stage_result,
                chain_path=chain_path,
                deployment_path=deployment_path,
                tax_state_path=Path(tax_report["artifact_ref"]["artifact_path"]).resolve(),
                emit_artifact=True,
            )
    return canonical_truth, sleeve_root, stage_result, chain_path, deployment_path, advisory_report


def test_opportunity_state_is_deterministic_for_same_certified_truth(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    canonical_truth, sleeve_root, _, _, _, _ = _build_truth_with_tax_and_advisory(tmp_path, monkeypatch)

    first = materialize_opportunity_state_v1(
        day_utc=DAY,
        sleeve_id=SLEEVE,
        environment=ENV,
        ib_account=ACCOUNT,
        canonical_truth_root=canonical_truth,
        truth_sleeves_root=sleeve_root,
    )
    second = materialize_opportunity_state_v1(
        day_utc=DAY,
        sleeve_id=SLEEVE,
        environment=ENV,
        ib_account=ACCOUNT,
        canonical_truth_root=canonical_truth,
        truth_sleeves_root=sleeve_root,
    )

    assert _strip_volatile(first["opportunities"]) == _strip_volatile(second["opportunities"])
    assert _strip_volatile(first["review_snapshot"]) == _strip_volatile(second["review_snapshot"])


def test_opportunity_precedence_and_explanations_are_table_driven() -> None:
    degraded = evaluate_opportunity_state_precedence_v1(
        freshness_state="fresh",
        degraded_upstream_truth=True,
        release_blocked=False,
        tax_effect_state="clear",
        advisory_blocked=False,
        base_review_priority="review_now",
        blocker_states=["basis_missing"],
        delta_state="new",
        scenario_significance_state="review_now",
    )
    assert degraded["primary_rule_id"] == "DEGRADED_UPSTREAM_TRUTH"
    assert degraded["opportunity_state"] == "blocked"
    assert degraded["review_priority"] == "review_now"
    assert degraded["visibility_state"] == "current_downgraded"
    assert "degraded_upstream_truth" in degraded["effective_blocker_states"]

    stale = evaluate_opportunity_state_precedence_v1(
        freshness_state="stale",
        degraded_upstream_truth=False,
        release_blocked=False,
        tax_effect_state="clear",
        advisory_blocked=False,
        base_review_priority="review_now",
        blocker_states=[],
        delta_state="unchanged",
        scenario_significance_state="not_evaluated",
    )
    assert stale["primary_rule_id"] == "STALE_OPPORTUNITY"
    assert stale["visibility_state"] == "historical_only"

    explanation = map_opportunity_explanation_v1(
        opportunity_type="scenario_review",
        opportunity_state="blocked",
        actionability_state="blocked",
        review_priority="review_now",
        blocker_states=["degraded_upstream_truth"],
        delta_state="blocked_but_still_important",
        scenario_significance_state="basis_unavailable",
        freshness_state="fresh",
        visibility_state="current_downgraded",
        reason_id="DEGRADED_UPSTREAM_TRUTH",
        evidence_refs=[{"artifact_id": "fixture", "artifact_path": "/tmp/f", "artifact_sha256": "a" * 64}],
        authority_label="governed_certified_opportunity",
        detail_fields={"test": True},
    )
    assert explanation["explanation_id"] == "opportunity.degraded_upstream_truth"
    assert explanation["delta_state"] == "blocked_but_still_important"


def test_opportunity_taxonomy_and_review_snapshot_delta_are_deterministic(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    canonical_truth, sleeve_root, _, _, _, _ = _build_truth_with_tax_and_advisory(tmp_path, monkeypatch)
    scope_id = _scope_id(canonical_truth, sleeve_root)
    _write_prior_snapshot(canonical_truth, scope_id)
    scenario_path = _write_scenario(
        _scenario_path(canonical_truth, "stress-run-delta"),
        support_status="fully_supported",
        scenario_ids=["base", "stress"],
        outcomes=["drawdown", "recovery"],
        summary="fixture comparison",
    )

    report = materialize_opportunity_state_v1(
        day_utc=DAY,
        sleeve_id=SLEEVE,
        environment=ENV,
        ib_account=ACCOUNT,
        canonical_truth_root=canonical_truth,
        truth_sleeves_root=sleeve_root,
        stress_case_view_path=scenario_path,
        emit_artifacts=True,
    )

    by_type = {row["opportunity_type"]: row for row in report["opportunities"]}
    assert {"tax_harvest_review", "advisory_action_review", "scenario_review"}.issubset(by_type)
    assert by_type["scenario_review"]["scenario_significance_state"] == "review_now"
    assert by_type["scenario_review"]["review_priority"] == "review_now"
    assert by_type["advisory_action_review"]["delta_state"] == "changed"
    assert report["review_snapshot"]["summary"]["changed_count"] >= 1
    assert report["review_snapshot"]["summary"]["resolved_count"] == 1
    assert report["review_snapshot"]["prior_review_snapshot_ref"]["artifact_id"] == "opportunity_review_snapshot_v1"

    latest_snapshot = find_latest_opportunity_review_snapshot_v1(
        canonical_truth_root=canonical_truth,
        scope_id=scope_id,
        day_utc=DAY,
    )
    assert latest_snapshot is not None
    assert latest_snapshot.payload["summary"]["resolved_count"] == 1


def test_scenario_significance_binding_is_bounded_and_deterministic() -> None:
    assert evaluate_opportunity_scenario_significance_v1(scenario_payload=None)["state"] == "not_evaluated"
    assert evaluate_opportunity_scenario_significance_v1(
        scenario_payload={
            "support_status": "degraded",
            "summary": "stub_only_view",
            "scenario_ids": ["base"],
            "outcomes": [],
        }
    )["state"] == "basis_unavailable"
    assert evaluate_opportunity_scenario_significance_v1(
        scenario_payload={
            "support_status": "fully_supported",
            "summary": "fixture comparison",
            "scenario_ids": ["base", "stress"],
            "outcomes": ["material_drawdown"],
        }
    )["state"] == "review_now"


def test_opportunity_aware_advisory_binding_uses_opportunity_refs_and_downgrades_or_blocks(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    canonical_truth, sleeve_root, stage_result, chain_path, deployment_path, _ = _build_truth_with_tax_and_advisory(
        tmp_path, monkeypatch
    )

    downgrade_scenario_path = _write_scenario(
        _scenario_path(canonical_truth, "stress-run-monitor"),
        support_status="fully_supported",
        scenario_ids=["base"],
        outcomes=["small_change"],
        summary="fixture monitor",
    )
    downgrade_report = materialize_opportunity_state_v1(
        day_utc=DAY,
        sleeve_id=SLEEVE,
        environment=ENV,
        ib_account=ACCOUNT,
        canonical_truth_root=canonical_truth,
        truth_sleeves_root=sleeve_root,
        stress_case_view_path=downgrade_scenario_path,
        emit_artifacts=True,
    )
    scenario_ref = next(
        Path(ref["artifact_path"]).resolve()
        for ref, payload in zip(downgrade_report["artifact_refs"], downgrade_report["opportunities"])
        if payload["opportunity_type"] == "scenario_review"
    )
    downgraded = _materialize_advisory(
        canonical_truth=canonical_truth,
        truth_sleeves=sleeve_root,
        stage_result=stage_result,
        chain_path=chain_path,
        deployment_path=deployment_path,
        opportunity_state_path=scenario_ref,
        emit_artifact=True,
    )["decision"]
    assert downgraded["invalidation_rule_id"] == "OPPORTUNITY_BINDING_DOWNGRADED"
    assert downgraded["decision_state"] == "stale"
    assert downgraded["opportunity_binding_state"]["effect_state"] == "downgrade"
    assert downgraded["governing_opportunity_refs"][0]["artifact_id"] == "opportunity_state_v1"

    blocked_scenario_path = _write_scenario(
        _scenario_path(canonical_truth, "stress-run-blocked"),
        support_status="degraded",
        scenario_ids=["base"],
        outcomes=["no_financial_math"],
        summary="stub_only_view",
    )
    blocked_report = materialize_opportunity_state_v1(
        day_utc=DAY,
        sleeve_id=SLEEVE,
        environment=ENV,
        ib_account=ACCOUNT,
        canonical_truth_root=canonical_truth,
        truth_sleeves_root=sleeve_root,
        stress_case_view_path=blocked_scenario_path,
        emit_artifacts=True,
    )
    blocked_ref = next(
        Path(ref["artifact_path"]).resolve()
        for ref, payload in zip(blocked_report["artifact_refs"], blocked_report["opportunities"])
        if payload["opportunity_type"] == "scenario_review"
    )
    blocked = _materialize_advisory(
        canonical_truth=canonical_truth,
        truth_sleeves=sleeve_root,
        stage_result=stage_result,
        chain_path=chain_path,
        deployment_path=deployment_path,
        opportunity_state_path=blocked_ref,
        emit_artifact=True,
    )["decision"]
    assert blocked["invalidation_rule_id"] == "OPPORTUNITY_BINDING_BLOCKED"
    assert blocked["decision_state"] == "blocked"
    assert blocked["opportunity_binding_state"]["effect_state"] == "block"


def test_opportunity_lineage_is_reconstructable_from_supersession(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    canonical_truth, sleeve_root, _, _, _, _ = _build_truth_with_tax_and_advisory(tmp_path, monkeypatch)
    scenario_path = _write_scenario(
        _scenario_path(canonical_truth, "stress-run-lineage"),
        support_status="fully_supported",
        scenario_ids=["base", "stress"],
        outcomes=["drawdown"],
        summary="fixture comparison",
    )
    first = materialize_opportunity_state_v1(
        day_utc=DAY,
        sleeve_id=SLEEVE,
        environment=ENV,
        ib_account=ACCOUNT,
        canonical_truth_root=canonical_truth,
        truth_sleeves_root=sleeve_root,
        stress_case_view_path=scenario_path,
        emit_artifacts=True,
    )
    _write_scenario(
        scenario_path,
        support_status="degraded",
        scenario_ids=["base"],
        outcomes=["no_financial_math"],
        summary="stub_only_view",
    )
    second = materialize_opportunity_state_v1(
        day_utc=DAY,
        sleeve_id=SLEEVE,
        environment=ENV,
        ib_account=ACCOUNT,
        canonical_truth_root=canonical_truth,
        truth_sleeves_root=sleeve_root,
        stress_case_view_path=scenario_path,
        emit_artifacts=True,
    )

    first_ref = next(
        Path(ref["artifact_path"]).resolve()
        for ref, payload in zip(first["artifact_refs"], first["opportunities"])
        if payload["opportunity_type"] == "scenario_review"
    )
    latest = find_latest_opportunity_state_v1(canonical_truth_root=canonical_truth, day_utc=DAY)
    assert latest is not None
    scenario_rows = [
        ref for ref in list_opportunity_states_v1(canonical_truth_root=canonical_truth, day_utc=DAY)
        if str(ref.payload.get("opportunity_type") or "") == "scenario_review"
    ]
    assert len(scenario_rows) >= 2
    newest = max(scenario_rows, key=lambda ref: str(ref.payload.get("generated_at_utc") or ""))
    assert newest.payload["supersedes_ref"]["artifact_path"] == str(first_ref)
    assert newest.payload["primary_explanation"]["explanation_id"] == "opportunity.degraded_upstream_truth"
    assert newest.payload["semantic_events"][-1]["event_type"] == "advisory_downgraded_due_to_opportunity"


def test_opportunity_path_rejects_forbidden_truth_and_second_truth_system(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    canonical_truth, sleeve_root, _, _, _, _ = _build_truth_with_tax_and_advisory(tmp_path, monkeypatch)
    forbidden_tax = tmp_path / "forbidden" / "tax_state.v1.json"
    _write_json(forbidden_tax, {"schema_id": "tax_state"})

    with pytest.raises(Exception):
        materialize_opportunity_state_v1(
            day_utc=DAY,
            sleeve_id=SLEEVE,
            environment=ENV,
            ib_account=ACCOUNT,
            canonical_truth_root=canonical_truth,
            truth_sleeves_root=sleeve_root,
            tax_state_path=forbidden_tax,
        )
