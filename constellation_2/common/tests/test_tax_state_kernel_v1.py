from __future__ import annotations

import copy
import json
from pathlib import Path

import pytest

from constellation_2.common.advisory_decision_state_kernel_v1 import (
    materialize_advisory_decision_state_v1,
)
from constellation_2.common.control_plane_validation_kernel_v1 import _build_scope
from constellation_2.common.tax.storage_v1 import (
    append_validated_jsonl_v1,
)
from constellation_2.common.tax.truth_v1 import (
    accept_tax_fact_candidate_v1,
    build_tax_fact_candidate_v1,
    build_tax_observed_event_v1,
)
from constellation_2.common.tax_state_explanation_mapping_v1 import (
    map_tax_state_explanation_v1,
)
from constellation_2.common.tax_state_kernel_v1 import (
    ACCOUNT_REGISTRY_PATH,
    find_latest_tax_state_v1,
    list_tax_states_v1,
    materialize_tax_state_v1,
)
from constellation_2.common.tax_state_precedence_v1 import (
    evaluate_tax_state_precedence_v1,
)
from constellation_2.common.tests.test_advisory_decision_state_v1 import (
    _bundle5_session_chain,
    _seed_deployment_state,
)


DAY = "2026-04-17"
SLEEVE = "PRIMARY"
ENV = "PAPER"
ACCOUNT = "DUO847203"
REPO_ROOT = Path(__file__).resolve().parents[3]
GIT_SHA = "a" * 40
TAX_SCOPE_ID = "filing:joint"


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


def _seed_runtime_inputs(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    *,
    positions_status: str = "OK",
    origin: str = "NATIVE",
    source_kind: str = "NATIVE_FILL",
    cost_basis_cents: int = 150000,
    quantity: int = 10,
    market_value: str = "1000.00",
) -> tuple[Path, Path, Path]:
    canonical_truth = tmp_path / "runtime" / "truth"
    truth_sleeves = tmp_path / "runtime" / "truth_sleeves" / SLEEVE / ENV
    registry_path = tmp_path / "repo" / "governance" / "02_REGISTRIES" / "C2_IB_ACCOUNT_REGISTRY_V1.json"
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
                "module": "test_tax_state_kernel_v1.py",
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
                "module": "test_tax_state_kernel_v1.py",
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
                "module": "test_tax_state_kernel_v1.py",
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
                    "origin": origin,
                    "engine_id": "paper_engine_v1",
                    "source_intent_id": "intent:AAPL",
                    "intent_sha256": "4" * 64,
                    "instrument": {"kind": "equity", "symbol": "AAPL"},
                    "qty": quantity,
                    "avg_cost_cents": int(cost_basis_cents / quantity) if quantity else 0,
                    "opened_day_utc": "2025-01-01",
                    "last_transition_utc": f"{DAY}T16:10:00Z",
                    "last_transition_type": "IMPORTED_SNAPSHOT" if origin == "IMPORTED" else "OPEN",
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
                            "source_kind": source_kind,
                            "source_ref": "fixture-position-lot",
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
    return canonical_truth, truth_sleeves, registry_path


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
    emit_artifact: bool = False,
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


def _strip_volatile(payload: dict) -> dict:
    out = copy.deepcopy(payload)
    out.pop("generated_at_utc", None)
    return out


def test_tax_state_is_deterministic_for_same_governed_truth(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    canonical_truth, truth_sleeves, _ = _seed_runtime_inputs(tmp_path, monkeypatch)
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

    first = _materialize_tax_state(canonical_truth, truth_sleeves)
    second = _materialize_tax_state(canonical_truth, truth_sleeves)

    assert _strip_volatile(first["tax_state"]) == _strip_volatile(second["tax_state"])


def test_tax_state_precedence_and_opportunity_visibility_are_table_driven() -> None:
    blocked = evaluate_tax_state_precedence_v1(
        completeness_state="incomplete_basis",
        freshness_state="fresh",
        blocker_states=["basis_unknown"],
        opportunity_states=["harvest_candidate_available"],
    )
    assert blocked["primary_rule_id"] == "INCOMPLETE_TAX_BASIS"
    assert blocked["visibility_state"] == "current_downgraded"
    assert blocked["effective_opportunity_states"] == []
    assert blocked["advisory_effect_state"] == "block"

    visible = evaluate_tax_state_precedence_v1(
        completeness_state="complete",
        freshness_state="fresh",
        blocker_states=[],
        opportunity_states=["harvest_candidate_available"],
    )
    assert visible["primary_rule_id"] == "TAX_OPPORTUNITY_VISIBLE"
    assert visible["visibility_state"] == "current"
    assert visible["effective_opportunity_states"] == ["harvest_candidate_available"]


def test_tax_state_completeness_and_opportunities_follow_deterministic_truth(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    incomplete_truth, incomplete_sleeves, _ = _seed_runtime_inputs(tmp_path / "incomplete", monkeypatch)
    incomplete = _materialize_tax_state(incomplete_truth, incomplete_sleeves)["tax_state"]
    assert incomplete["completeness_state"] == "incomplete_basis"
    assert incomplete["primary_rule_id"] == "INCOMPLETE_TAX_BASIS"
    assert "account_regime_missing" in incomplete["blocker_states"]
    assert incomplete["harvesting_candidates"]["status"] == "unavailable"
    assert incomplete["advisory_binding_state"]["effect_state"] == "block"

    complete_truth, complete_sleeves, _ = _seed_runtime_inputs(tmp_path / "complete", monkeypatch)
    _write_tax_journal(
        complete_truth,
        [
            _accepted_fact(
                "account_classification_tax_regime",
                {"account_id": ACCOUNT, "tax_regime": "taxable_brokerage"},
                f"{DAY}T10:00:00Z",
            )
        ],
    )
    complete = _materialize_tax_state(complete_truth, complete_sleeves)["tax_state"]
    assert complete["completeness_state"] == "complete"
    assert complete["primary_rule_id"] == "TAX_OPPORTUNITY_VISIBLE"
    assert complete["blocker_states"] == []
    assert complete["opportunity_states"] == ["harvest_candidate_available"]
    assert complete["harvesting_candidates"]["status"] == "available"
    assert complete["harvesting_candidates"]["candidate_count"] == 1
    assert complete["advisory_binding_state"]["effect_state"] == "clear"


def test_tax_state_blocker_matrix_covers_wash_sale_and_degraded_runtime(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    wash_truth, wash_sleeves, _ = _seed_runtime_inputs(tmp_path / "wash", monkeypatch)
    _write_tax_journal(
        wash_truth,
        [
            _accepted_fact(
                "account_classification_tax_regime",
                {"account_id": ACCOUNT, "tax_regime": "taxable_brokerage"},
                f"{DAY}T10:00:00Z",
            ),
            _accepted_fact(
                "wash_sale_detected",
                {
                    "security_id": "AAPL",
                    "loss_sale_at": f"{DAY}T09:00:00Z",
                    "window_end_at": "2026-05-17T09:00:00Z",
                },
                f"{DAY}T10:05:00Z",
            ),
        ],
    )
    wash_state = _materialize_tax_state(wash_truth, wash_sleeves)["tax_state"]
    assert wash_state["primary_rule_id"] == "WASH_SALE_CONFLICT_BLOCK"
    assert "wash_sale_conflict" in wash_state["blocker_states"]
    assert wash_state["wash_sale_state"] == "blocked"
    assert wash_state["harvesting_candidates"]["status"] == "unavailable"

    degraded_truth, degraded_sleeves, _ = _seed_runtime_inputs(tmp_path / "degraded", monkeypatch)
    _write_tax_journal(
        degraded_truth,
        [
            _accepted_fact(
                "account_classification_tax_regime",
                {"account_id": ACCOUNT, "tax_regime": "taxable_brokerage"},
                f"{DAY}T10:00:00Z",
            ),
            _accepted_fact(
                "tax_data_gap_detected",
                {"gap_code": "MISSING_HOLDING_PERIOD_HISTORY"},
                f"{DAY}T10:05:00Z",
            ),
        ],
    )
    degraded_state = _materialize_tax_state(degraded_truth, degraded_sleeves)["tax_state"]
    assert degraded_state["completeness_state"] == "degraded_runtime"
    assert degraded_state["primary_rule_id"] == "DEGRADED_TAX_RUNTIME"
    assert degraded_state["advisory_binding_state"]["effect_state"] == "downgrade"


def test_tax_aware_advisory_binding_uses_tax_state_refs_and_blocks_when_required(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    canonical_truth, truth_sleeves, session_result, chain_path = _bundle5_session_chain(tmp_path, monkeypatch)
    _seed_runtime_inputs(tmp_path / "tax_inputs", monkeypatch)
    tax_truth = canonical_truth
    _write_json(
        tax_truth / "accounting_v2" / "nav" / DAY / "nav.v2.json",
        json.loads((tmp_path / "tax_inputs" / "runtime" / "truth" / "accounting_v2" / "nav" / DAY / "nav.v2.json").read_text(encoding="utf-8")),
    )
    _write_json(
        tax_truth / "cash_ledger_v1" / "snapshots" / DAY / "cash_ledger_snapshot.v1.json",
        json.loads((tmp_path / "tax_inputs" / "runtime" / "truth" / "cash_ledger_v1" / "snapshots" / DAY / "cash_ledger_snapshot.v1.json").read_text(encoding="utf-8")),
    )
    _write_json(
        tax_truth / "positions_v1" / "snapshots" / DAY / "positions_snapshot.v5.json",
        json.loads((tmp_path / "tax_inputs" / "runtime" / "truth" / "positions_v1" / "snapshots" / DAY / "positions_snapshot.v5.json").read_text(encoding="utf-8")),
    )
    registry_path = tmp_path / "tax_inputs" / "repo" / "governance" / "02_REGISTRIES" / "C2_IB_ACCOUNT_REGISTRY_V1.json"
    monkeypatch.setattr(
        "constellation_2.common.tax_state_kernel_v1.ACCOUNT_REGISTRY_PATH",
        registry_path,
    )
    tax_report = materialize_tax_state_v1(
        day_utc=DAY,
        sleeve_id=SLEEVE,
        environment=ENV,
        ib_account=ACCOUNT,
        canonical_truth_root=canonical_truth,
        truth_sleeves_root=truth_sleeves,
        tax_scope_id=TAX_SCOPE_ID,
        emit_artifact=True,
        evaluated_at_utc=f"{DAY}T16:30:00Z",
    )
    tax_ref = tax_report["artifact_ref"]
    assert tax_ref is not None

    advisory_report = materialize_advisory_decision_state_v1(
        day_utc=DAY,
        sleeve_id=SLEEVE,
        environment=ENV,
        ib_account=ACCOUNT,
        stage_path=session_result["stage_ref"]["artifact_path"],
        transition_record_path=session_result["transition_record_ref"]["artifact_path"],
        advisory_item_id="primary_tax_sensitive_advisory",
        advisory_surface_label="operator_shell_advisory",
        advisory_authority_class="recommendation",
        certification_path=session_result["stage_certification_ref"]["artifact_path"],
        startup_chain_certification_path=str(chain_path),
        deployment_state_path=str(_seed_deployment_state(canonical_truth)),
        tax_state_path=tax_ref["artifact_path"],
        canonical_truth_root=canonical_truth,
        truth_sleeves_root=truth_sleeves,
        emit_artifact=False,
    )["decision"]

    assert advisory_report["decision_state"] == "blocked"
    assert advisory_report["invalidation_rule_id"] == "TAX_BINDING_BLOCKED"
    assert advisory_report["governing_tax_refs"][0]["artifact_path"] == tax_ref["artifact_path"]
    assert advisory_report["tax_binding_state"]["binding_reason_id"] == "INCOMPLETE_TAX_BASIS"


def test_tax_state_explanation_and_lineage_are_deterministic_and_reconstructable(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    canonical_truth, truth_sleeves, _ = _seed_runtime_inputs(tmp_path, monkeypatch)
    _write_tax_journal(
        canonical_truth,
        [
            _accepted_fact(
                "account_classification_tax_regime",
                {"account_id": ACCOUNT, "tax_regime": "taxable_brokerage"},
                f"{DAY}T10:00:00Z",
            ),
            _accepted_fact(
                "tax_data_gap_detected",
                {"gap_code": "MISSING_HOLDING_PERIOD_HISTORY"},
                f"{DAY}T10:05:00Z",
            ),
        ],
    )
    first = _materialize_tax_state(
        canonical_truth,
        truth_sleeves,
        emit_artifact=True,
        evaluated_at_utc=f"{DAY}T16:30:00Z",
    )
    first_ref = first["artifact_ref"]
    assert first_ref is not None
    explanation = map_tax_state_explanation_v1(
        completeness_state="degraded_runtime",
        freshness_state="fresh",
        blocker_states=[],
        opportunity_states=[],
        degraded_reason_id="DEGRADED_TAX_RUNTIME",
        evidence_refs=first["tax_state"]["evidence_refs"],
        authority_label=first["tax_state"]["authority_label"],
        detail_fields={"fixture": True},
    )
    assert explanation["evidence_refs"] == first["tax_state"]["evidence_refs"]
    assert explanation["authority_label"] == first["tax_state"]["authority_label"]
    assert "confidence" not in json.dumps(explanation, sort_keys=True).lower()

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
    second = _materialize_tax_state(
        canonical_truth,
        truth_sleeves,
        emit_artifact=True,
        evaluated_at_utc=f"{DAY}T17:30:00Z",
    )
    second_ref = second["artifact_ref"]
    assert second_ref is not None
    assert second["tax_state"]["supersedes_ref"]["artifact_path"] == first_ref["artifact_path"]
    refs = list_tax_states_v1(
        canonical_truth_root=canonical_truth,
        day_utc=DAY,
        scope_id=second["tax_state"]["scope_id"],
    )
    assert len(refs) == 2
    latest = find_latest_tax_state_v1(
        canonical_truth_root=canonical_truth,
        day_utc=DAY,
        scope_id=second["tax_state"]["scope_id"],
    )
    assert latest is not None
    assert latest.payload["tax_state_id"] == second["tax_state"]["tax_state_id"]


def test_tax_path_rejects_forbidden_truth_and_never_accepts_second_tax_truth_system(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    canonical_truth, truth_sleeves, session_result, chain_path = _bundle5_session_chain(tmp_path, monkeypatch)
    forbidden_tax_path = (tmp_path / "forbidden" / "tax_state.v1.json").resolve()
    forbidden_tax_path.parent.mkdir(parents=True, exist_ok=True)
    forbidden_tax_path.write_text("{}", encoding="utf-8")

    with pytest.raises(RuntimeError, match="ADVISORY_DECISION_INPUT_OUTSIDE_CANONICAL_TRUTH_ROOT"):
        materialize_advisory_decision_state_v1(
            day_utc=DAY,
            sleeve_id=SLEEVE,
            environment=ENV,
            ib_account=ACCOUNT,
            stage_path=session_result["stage_ref"]["artifact_path"],
            transition_record_path=session_result["transition_record_ref"]["artifact_path"],
            advisory_item_id="forbidden_tax_binding",
            advisory_surface_label="operator_shell_advisory",
            advisory_authority_class="recommendation",
            certification_path=session_result["stage_certification_ref"]["artifact_path"],
            startup_chain_certification_path=str(chain_path),
            deployment_state_path=str(_seed_deployment_state(canonical_truth)),
            tax_state_path=str(forbidden_tax_path),
            canonical_truth_root=canonical_truth,
            truth_sleeves_root=truth_sleeves,
            emit_artifact=False,
        )
