from __future__ import annotations

import json
import sys
from pathlib import Path

SOURCE_ROOT = Path("/home/node/constellation")
if str(SOURCE_ROOT) not in sys.path:
    sys.path.insert(0, str(SOURCE_ROOT))

from constellation_2.common.paper_second_attempt_clearance_v1 import (  # noqa: E402
    OPERATOR_SCHEMA_ID,
    plan_hash_v1,
    sha256_file_v1,
)
from constellation_2.phaseD.lib.submit_boundary_paper_v4 import _ib_payload_hash  # noqa: E402
from ops.tools.run_first_ib_accepted_paper_order_checklist_v1 import (  # noqa: E402
    build_first_ib_accepted_paper_order_checklist_v1,
)


DAY = "2026-04-29"
NOW = "2026-04-29T14:31:00Z"
INTENT = "c" * 64
SUBMISSION = "new-submission"
PRIOR = "prior-submission"


def _write_json(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, sort_keys=True, separators=(",", ":")) + "\n", encoding="utf-8")


def _snapshot(truth: Path, execution: Path) -> tuple[Path, Path]:
    snapshot = execution / "options_chain_snapshot_v1" / DAY / "capture" / "options_chain_snapshot.v1.json"
    cert = snapshot.parent / "freshness_certificate.v1.json"
    _write_json(
        snapshot,
        {
            "schema_id": "options_chain_snapshot",
            "schema_version": "v1",
            "day_utc": DAY,
            "as_of_utc": "2026-04-29T14:30:00Z",
            "underlying": {"symbol": "SPY", "spot_price": "110.00", "spot_as_of_utc": "2026-04-29T14:30:00Z"},
            "contracts": [
                {
                    "contract_key": "SPY|2026-05-01T00:00:00Z|PUT|100.00",
                    "right": "PUT",
                    "strike": "100.00",
                    "expiry_utc": "2026-05-01T00:00:00Z",
                    "bid": "1.00",
                    "ask": "1.05",
                    "ib": {"conId": 1001, "localSymbol": "SPY P100", "exchange": "SMART"},
                },
                {
                    "contract_key": "SPY|2026-05-01T00:00:00Z|PUT|95.00",
                    "right": "PUT",
                    "strike": "95.00",
                    "expiry_utc": "2026-05-01T00:00:00Z",
                    "bid": "0.40",
                    "ask": "0.45",
                    "ib": {"conId": 1002, "localSymbol": "SPY P95", "exchange": "SMART"},
                },
            ],
        },
    )
    _write_json(
        cert,
        {
            "schema_id": "freshness_certificate",
            "schema_version": "v1",
            "day_utc": DAY,
            "valid_from_utc": "2026-04-29T14:30:00Z",
            "valid_until_utc": "2026-04-29T14:35:00Z",
        },
    )
    _write_json(
        truth / "reports" / "market_open_data_gate_v1" / DAY / "market_open_data_gate.v1.json",
        {"status": "PASS", "snapshot_path": str(snapshot), "freshness_certificate_path": str(cert)},
    )
    return snapshot, cert


def _order_plan(plan_hash_seed: str = "new") -> dict:
    return {
        "schema_id": "order_plan",
        "schema_version": "v1",
        "intent_hash": INTENT,
        "structure": "VERTICAL_SPREAD",
        "underlying": {"symbol": "SPY", "currency": "USD"},
        "legs": [
            {
                "action": "SELL",
                "right": "PUT",
                "strike": "100.00",
                "expiry_utc": "2026-05-01T00:00:00Z",
                "ib_conId": 1001,
                "ratio": 1,
                "contract_key": "SPY|2026-05-01T00:00:00Z|PUT|100.00",
            },
            {
                "action": "BUY",
                "right": "PUT",
                "strike": "95.00",
                "expiry_utc": "2026-05-01T00:00:00Z",
                "ib_conId": 1002,
                "ratio": 1,
                "contract_key": "SPY|2026-05-01T00:00:00Z|PUT|95.00",
            },
        ],
        "order_terms": {
            "order_type": "LIMIT",
            "limit_price": "0.60",
            "time_in_force": "DAY",
            "is_credit": True,
            "plan_hash_seed": plan_hash_seed,
        },
        "risk_proof": {"defined_risk_proven": True, "width_points": "5.00", "max_loss_usd": "440.00", "contracts": 1},
    }


def _structure(snapshot: Path, cert: Path) -> dict:
    return {
        "status": "PASS",
        "market_open_data": {"snapshot_path": str(snapshot), "freshness_certificate_path": str(cert)},
        "structure_decisions": [
            {
                "sleeve_id": "C2_VOL_INCOME_DEFINED_RISK_V1",
                "pricing_inputs": {"snapshot_path": str(snapshot), "freshness_certificate_path": str(cert)},
                "option_structure": {
                    "legs": _order_plan()["legs"],
                    "width_points": "5.00",
                    "net_credit": "0.60",
                    "max_loss_cents": 44000,
                },
            }
        ],
    }


def _seed_all_pass(tmp_path: Path) -> tuple[Path, Path]:
    truth = tmp_path / "truth"
    execution = tmp_path / "truth_sleeves" / "PRIMARY" / "PAPER"
    snapshot, cert = _snapshot(truth, execution)
    structure = _structure(snapshot, cert)
    _write_json(truth / "reports" / "structure_decision_supply_v1" / DAY / "structure_decision_supply.v1.json", structure)

    phasec = execution / "phaseC_preflight_v1" / DAY / "attempt_A0001" / INTENT
    plan = _order_plan()
    order_path = phasec / "order_plan.v1.json"
    _write_json(order_path, plan)
    _write_json(phasec / "mapping_ledger_record.v1.json", {"chain_snapshot_hash": sha256_file_v1(snapshot), "freshness_cert_hash": sha256_file_v1(cert)})
    _write_json(phasec / "binding_record.v1.json", {"freshness_cert_hash": sha256_file_v1(cert)})
    _write_json(phasec / "structure_decision_supply.v1.json", structure)
    _write_json(phasec / "submit_preflight_decision.v1.json", {"decision": "ALLOW"})
    _write_json(
        phasec / "execution_identity_record.v1.json",
        {
            "submission_id": SUBMISSION,
            "source_refs": [
                {"ref_type": "order_plan_ref", "path": str(order_path), "sha256": sha256_file_v1(order_path)}
            ],
        },
    )

    prior_dir = execution / "execution_evidence_v1" / "submissions" / DAY / PRIOR
    prior_plan = _order_plan(plan_hash_seed="prior")
    prior_plan["order_terms"]["limit_price"] = "0.50"
    _write_json(prior_dir / "order_plan.v1.json", prior_plan)
    _write_json(prior_dir / "broker_submission_record.v2.json", {"submission_id": PRIOR, "status": "CANCELLED", "broker_ids": {"order_id": 94, "perm_id": 0}})
    clearance = truth / "reports" / "paper_second_attempt_clearance_v1" / DAY / PRIOR / "paper_second_attempt_clearance.v1.json"
    _write_json(
        clearance,
        {
            "schema_id": "paper_second_attempt_clearance",
            "schema_version": "v1",
            "day_utc": DAY,
            "environment": "PAPER",
            "status": "CLEARED",
            "canonical_blocker": "",
            "_path": str(clearance),
            "prior_submission": {
                "submission_id": PRIOR,
                "account": "DUO847203",
                "order_plan_path": str(prior_dir / "order_plan.v1.json"),
                "order_plan_sha256": sha256_file_v1(prior_dir / "order_plan.v1.json"),
            },
            "operator_clearance": {"schema_id": OPERATOR_SCHEMA_ID},
        },
    )

    submission = execution / "execution_evidence_v1" / "submissions" / DAY / SUBMISSION
    ib_payload = {
        "bag": {
            "secType": "BAG",
            "legs": [{"conId": 1001, "ratio": 1, "action": "SELL"}, {"conId": 1002, "ratio": 1, "action": "BUY"}],
        },
        "order": {"action": "BUY"},
        "routing": {"smart_combo_routing_params": [{"tag": "NonGuaranteed", "value": "1"}]},
    }
    ib_payload_hash = _ib_payload_hash(ib_payload)
    _write_json(
        submission / "ib_order_payload.v1.json",
        {
            "schema_id": "ib_order_payload",
            "day_utc": DAY,
            "environment": "PAPER",
            "ib_account": "DUO847203",
            "payload": ib_payload,
            "payload_sha256": ib_payload_hash,
            "submit_payload_sha256": ib_payload_hash,
        },
    )
    _write_json(
        submission / "ib_combo_preview.v1.json",
        {
            "schema_id": "ib_combo_preview",
            "day_utc": DAY,
            "environment": "PAPER",
            "ib_account": "DUO847203",
            "status": "PASS",
            "canonical_blocker": "",
            "whatif_ok": True,
            "detail": "WHATIF_OK",
            "preview_payload_sha256": ib_payload_hash,
            "submit_payload_sha256": ib_payload_hash,
            "preview_payload_hash_matches_submit_payload_hash": True,
            "raw": {"payload": ib_payload},
        },
    )
    _write_json(
        truth / "reports" / "submit_boundary_status_v1" / DAY / "submit_boundary_status.v1.json",
        {"boundary_status": "AUTHORIZED", "submission_authorized": True},
    )
    return truth, execution


def _run(truth: Path, execution: Path, *, account: str = "DUO847203") -> dict:
    return build_first_ib_accepted_paper_order_checklist_v1(
        truth_root=truth,
        execution_root=execution,
        day_utc=DAY,
        environment="PAPER",
        ib_account=account,
        now_utc=NOW,
    )


def _refresh_snapshot_hash_refs(execution: Path, snapshot_path: Path) -> None:
    phasec = execution / "phaseC_preflight_v1" / DAY / "attempt_A0001" / INTENT
    mapping_path = phasec / "mapping_ledger_record.v1.json"
    mapping = json.loads(mapping_path.read_text(encoding="utf-8"))
    mapping["chain_snapshot_hash"] = sha256_file_v1(snapshot_path)
    _write_json(mapping_path, mapping)


def test_all_pass_status_pass(tmp_path: Path) -> None:
    truth, execution = _seed_all_pass(tmp_path)
    payload = _run(truth, execution)
    assert payload["status"] == "PASS"
    assert payload["overall_status"] == payload["status"]
    assert payload["canonical_blocker"] == ""
    assert payload["first_failed_group"] == ""
    assert payload["first_failed_check"] == ""
    assert payload["grouped_check_summary"]["MARKET_DATA"]["status"] == "PASS"
    assert payload["grouped_check_summary"]["IB_PREVIEW"]["status"] == "PASS"
    assert payload["trade_summary"]["symbol"] == "SPY"
    assert payload["trade_summary"]["short_leg"]["action"] == "SELL"
    assert payload["trade_summary"]["long_leg"]["action"] == "BUY"
    assert payload["trade_summary"]["account"] == "DUO847203"
    assert payload["plain_english_next_action"].startswith("All pre-submit checklist checks passed")
    assert all(row["status"] == "PASS" for row in payload["checks"])


def test_missing_preview_blocks(tmp_path: Path) -> None:
    truth, execution = _seed_all_pass(tmp_path)
    (execution / "execution_evidence_v1" / "submissions" / DAY / SUBMISSION / "ib_combo_preview.v1.json").unlink()
    payload = _run(truth, execution)
    assert payload["status"] == "BLOCKED"
    assert payload["overall_status"] == payload["status"]
    assert payload["canonical_blocker"] == "IB_PREVIEW_MISSING"
    assert payload["first_failed_group"] == "IB_PREVIEW"
    assert payload["first_failed_check"] == "19_IB_PREVIEW_PASS"
    assert payload["grouped_check_summary"]["IB_PREVIEW"]["status"] == "NOT_CHECKED"
    assert payload["trade_summary"]["preview_status"] == ""
    assert payload["plain_english_next_action"] == "Run IB what-if preview for the exact payload and do not submit until preview passes."


def test_operator_summary_does_not_change_readiness_decision(tmp_path: Path) -> None:
    truth, execution = _seed_all_pass(tmp_path)

    passing = _run(truth, execution)
    assert passing["status"] == "PASS"
    assert passing["overall_status"] == "PASS"

    preview_path = execution / "execution_evidence_v1" / "submissions" / DAY / SUBMISSION / "ib_combo_preview.v1.json"
    preview = json.loads(preview_path.read_text(encoding="utf-8"))
    preview["status"] = "BLOCKED"
    preview["whatif_ok"] = False
    _write_json(preview_path, preview)

    blocked = _run(truth, execution)
    assert blocked["status"] == "BLOCKED"
    assert blocked["overall_status"] == "BLOCKED"
    assert blocked["canonical_blocker"] == "IB_PREVIEW_NOT_PASS"
    assert blocked["grouped_check_summary"]["IB_PREVIEW"]["canonical_blocker"] == "IB_PREVIEW_NOT_PASS"


def test_error_201_preview_blocks(tmp_path: Path) -> None:
    truth, execution = _seed_all_pass(tmp_path)
    _write_json(
        execution / "execution_evidence_v1" / "submissions" / DAY / SUBMISSION / "ib_combo_preview.v1.json",
        {"status": "BLOCKED", "canonical_blocker": "IB_ERROR_201_RISKLESS_COMBINATION", "whatif_ok": False, "detail": "Error 201 Riskless combination orders are not allowed.", "ib_account": "DUO847203"},
    )
    payload = _run(truth, execution)
    assert payload["status"] == "BLOCKED"
    assert any(row["blocker"] == "IB_ERROR_201_RISKLESS_COMBINATION" for row in payload["checks"])


def test_stale_snapshot_lineage_blocks(tmp_path: Path) -> None:
    truth, execution = _seed_all_pass(tmp_path)
    structure_path = truth / "reports" / "structure_decision_supply_v1" / DAY / "structure_decision_supply.v1.json"
    structure = json.loads(structure_path.read_text(encoding="utf-8"))
    structure["market_open_data"]["snapshot_path"] = str(execution / "options_chain_snapshot_v1" / DAY / "old" / "options_chain_snapshot.v1.json")
    _write_json(structure_path, structure)
    payload = _run(truth, execution)
    assert payload["status"] == "BLOCKED"
    assert payload["canonical_blocker"] == "SNAPSHOT_LINEAGE_MISMATCH"


def test_near_itm_structure_blocks(tmp_path: Path) -> None:
    truth, execution = _seed_all_pass(tmp_path)
    snapshot_path = execution / "options_chain_snapshot_v1" / DAY / "capture" / "options_chain_snapshot.v1.json"
    snapshot = json.loads(snapshot_path.read_text(encoding="utf-8"))
    snapshot["underlying"]["spot_price"] = "101.00"
    _write_json(snapshot_path, snapshot)
    _refresh_snapshot_hash_refs(execution, snapshot_path)

    payload = _run(truth, execution)

    assert payload["status"] == "BLOCKED"
    assert payload["canonical_blocker"] == "NEAR_ITM_NOT_APPROVED"


def test_illiquid_structure_blocks(tmp_path: Path) -> None:
    truth, execution = _seed_all_pass(tmp_path)
    snapshot_path = execution / "options_chain_snapshot_v1" / DAY / "capture" / "options_chain_snapshot.v1.json"
    snapshot = json.loads(snapshot_path.read_text(encoding="utf-8"))
    snapshot["contracts"][0]["ask"] = "1.30"
    _write_json(snapshot_path, snapshot)
    _refresh_snapshot_hash_refs(execution, snapshot_path)

    payload = _run(truth, execution)

    assert payload["status"] == "BLOCKED"
    assert payload["canonical_blocker"] == "LIQUIDITY_INVALID"


def test_identical_plan_hash_blocks(tmp_path: Path) -> None:
    truth, execution = _seed_all_pass(tmp_path)
    current = execution / "phaseC_preflight_v1" / DAY / "attempt_A0001" / INTENT / "order_plan.v1.json"
    prior = execution / "execution_evidence_v1" / "submissions" / DAY / PRIOR / "order_plan.v1.json"
    plan = json.loads(current.read_text(encoding="utf-8"))
    _write_json(prior, plan)
    clearance = truth / "reports" / "paper_second_attempt_clearance_v1" / DAY / PRIOR / "paper_second_attempt_clearance.v1.json"
    obj = json.loads(clearance.read_text(encoding="utf-8"))
    obj["prior_submission"]["order_plan_sha256"] = sha256_file_v1(prior)
    _write_json(clearance, obj)
    payload = _run(truth, execution)
    assert payload["status"] == "BLOCKED"
    assert payload["canonical_blocker"] == "IDENTICAL_PLAN_HASH"


def test_identical_structure_pricing_blocks(tmp_path: Path) -> None:
    truth, execution = _seed_all_pass(tmp_path)
    prior = execution / "execution_evidence_v1" / "submissions" / DAY / PRIOR / "order_plan.v1.json"
    current = json.loads((execution / "phaseC_preflight_v1" / DAY / "attempt_A0001" / INTENT / "order_plan.v1.json").read_text(encoding="utf-8"))
    prior_plan = dict(current)
    prior_plan["prior_only_metadata"] = "changes-plan-hash-without-changing-structure-pricing-signature"
    _write_json(prior, prior_plan)
    clearance = truth / "reports" / "paper_second_attempt_clearance_v1" / DAY / PRIOR / "paper_second_attempt_clearance.v1.json"
    obj = json.loads(clearance.read_text(encoding="utf-8"))
    obj["prior_submission"]["order_plan_sha256"] = sha256_file_v1(prior)
    _write_json(clearance, obj)

    payload = _run(truth, execution)

    assert payload["status"] == "BLOCKED"
    assert payload["canonical_blocker"] == "IDENTICAL_STRUCTURE_PRICING"


def test_wrong_account_blocks(tmp_path: Path) -> None:
    truth, execution = _seed_all_pass(tmp_path)
    payload = _run(truth, execution, account="DU999999")
    assert payload["status"] == "BLOCKED"
    assert payload["canonical_blocker"] == "PAPER_ACCOUNT_SCOPE_INVALID"


def test_submit_boundary_not_authorized_blocks(tmp_path: Path) -> None:
    truth, execution = _seed_all_pass(tmp_path)
    _write_json(
        truth / "reports" / "submit_boundary_status_v1" / DAY / "submit_boundary_status.v1.json",
        {"boundary_status": "BLOCKED", "submission_authorized": False},
    )
    payload = _run(truth, execution)
    assert payload["status"] == "BLOCKED"
    assert payload["canonical_blocker"] == "SUBMIT_BOUNDARY_NOT_AUTHORIZED"
