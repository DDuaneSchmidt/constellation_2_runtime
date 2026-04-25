from __future__ import annotations

import json
import sys
from pathlib import Path

import pytest


def _repo_root() -> Path:
    return Path(__file__).resolve().parents[3]


REPO_ROOT = _repo_root()
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from constellation_2.phaseD.lib.validate_against_schema_v1 import validate_against_repo_schema_v1  # noqa: E402
from constellation_2.common.canonical_sleeve_identity_v1 import (  # noqa: E402
    canonical_sleeve_id_from_engine_id,
    canonical_sleeve_id_from_execution_scope,
)
import ops.tools.run_portfolio_governance_snapshot_day_v1 as portfolio_snapshot_writer  # noqa: E402
import ops.tools.run_sleeve_allocation_epoch_day_v1 as allocation_epoch_writer  # noqa: E402
import ops.tools.run_intent_authorization_ledger_day_v1 as authorization_ledger_writer  # noqa: E402
import ops.tools.run_exposure_ledger_day_v1 as exposure_ledger_writer  # noqa: E402


DAY = "2026-04-14"
DAY_EXPOSURE = "2026-04-13"
INTENT_HASH = "c5d143b73ce2abed1650df5ef637994a924dba64e8df7f64c29c964850654d8f"
INTENT_HASH_EXPOSURE = "00091fc97c0d1e953e652e0f2461c1b2b95492b8bb6891c04fbd0471adcf8ce0"
BINDING_HASH = "b13bd8662bfec5e2f072e28fc6720163bb010daf694eddd2e3bdeb7ee81ae678"
SUBMISSION_ID = "d50ff800edd387bdb8ef31f8d9355e8d6c4b8d957c81035d246963fcba5db0da"


def _write_json(path: Path, obj: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(obj, sort_keys=True, separators=(",", ":")) + "\n", encoding="utf-8")


def _write_jsonl(path: Path, objects: list[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        "".join(json.dumps(obj, sort_keys=True, separators=(",", ":")) + "\n" for obj in objects),
        encoding="utf-8",
    )


def _sha(seed: str) -> str:
    import hashlib

    return hashlib.sha256(seed.encode("utf-8")).hexdigest()


def _seed_capital_risk_envelope(truth_root: Path, *, day_utc: str) -> None:
    _write_json(
        truth_root / "reports" / "capital_risk_envelope_v2" / day_utc / "capital_risk_envelope.v2.json",
        {
            "schema_id": "capital_risk_envelope",
            "schema_version": "v2",
            "day_utc": day_utc,
            "produced_utc": f"{day_utc}T00:00:00Z",
            "producer": {"repo": "constellation", "module": "test", "git_sha": "7d64db4"},
            "status": "PASS",
            "reason_codes": [],
            "input_manifest": [
                {"type": "allocation_summary", "path": "/tmp/alloc.json", "sha256": "a" * 64},
            ],
            "checks": {
                "allocation_summary_present": True,
                "nav_present": True,
                "positions_present": True,
                "drawdown_present": True,
                "positions_all_have_max_loss": True,
                "portfolio_within_envelope": True,
            },
            "envelope": {
                "contracts": {
                    "drawdown_contract": {"path": "/tmp/drawdown.md", "sha256": "b" * 64},
                    "capital_risk_envelope_contract": {"path": "/tmp/envelope.md", "sha256": "c" * 64},
                },
                "drawdown_multiplier_table": [
                    {"threshold_drawdown_pct": "0.000000", "multiplier": "1.00"},
                ],
                "base_envelope_pct": "0.020000",
                "nav_total": 5000000,
                "nav_total_cents": 500000000,
                "peak_nav": 5000000,
                "drawdown_abs": 0,
                "drawdown_pct": "0.000000",
                "multiplier": "1.00",
                "allowed_capital_at_risk_cents": 200000,
                "portfolio_capital_at_risk_cents": 100000,
                "headroom_cents": 100000,
                "positions": [],
            },
        },
    )


def _seed_capital_allocation(truth_root: Path, *, day_utc: str, intent_hash: str) -> None:
    _write_json(
        truth_root / "allocation_v1" / "capital_authority_allocation_v1" / day_utc / "capital_authority_allocation.v1.json",
        {
            "schema_id": "C2_CAPITAL_AUTHORITY_ALLOCATION_V1",
            "schema_version": 1,
            "day_utc": day_utc,
            "produced_utc": f"{day_utc}T00:00:00Z",
            "producer": {"repo": "constellation", "module": "test", "git_sha": "7d64db4"},
            "status": "OK",
            "reason_codes": [],
            "portfolio": {
                "allowed_capital_at_risk_cents": 200000,
                "used_capital_at_risk_cents": 100000,
                "headroom_cents": 100000,
            },
            "per_sleeve": [
                {
                    "sleeve_id": "C2_TREND_EQ_PRIMARY",
                    "engine_ids": ["C2_TREND_EQ_PRIMARY_V1"],
                    "allowed_capital_at_risk_cents": 100000,
                    "used_capital_at_risk_cents": 100000,
                    "headroom_cents": 0,
                },
            ],
            "per_intent": [
                {
                    "engine_id": "C2_TREND_EQ_PRIMARY_V1",
                    "intent_id": f"intent_{day_utc}",
                    "intent_hash": intent_hash,
                    "sleeve_id": "C2_TREND_EQ_PRIMARY",
                    "decision": "AUTHORIZED",
                    "authorized_quantity": 1,
                    "reason_codes": ["CAPAUTH_AUTHORIZED"],
                }
            ],
            "correlation_gate_binding": {
                "applied": True,
                "binding_mode": "ALLOCATION_CAP_CONSTRAINT",
                "gate_artifact_path": "/tmp/correlation_gate.json",
                "gate_artifact_sha256": "d" * 64,
                "policy_id": "C2_CORRELATION_ENVELOPE_POLICY_V1",
                "policy_sha256": "e" * 64,
            },
            "input_manifest": [
                {
                    "type": "policy_manifest",
                    "path": str(REPO_ROOT / "governance/02_REGISTRIES/C2_CAPITAL_AUTHORITY_POLICY_V1.json"),
                    "sha256": _sha("policy"),
                    "day_utc": None,
                    "producer": "governance",
                }
            ],
        },
    )


def _seed_authorization_artifact(truth_root: Path, *, day_utc: str, intent_hash: str, status: str = "AUTHORIZED") -> Path:
    path = truth_root / "engine_activity_v1" / "authorization_v1" / day_utc / f"{intent_hash}.authorization.v1.json"
    _write_json(
        path,
        {
            "schema_id": "C2_AUTHORIZATION_V1",
            "schema_version": 1,
            "day_utc": day_utc,
            "produced_utc": f"{day_utc}T00:00:00Z",
            "engine_id": "C2_TREND_EQ_PRIMARY_V1",
            "intent_id": f"intent_{day_utc}",
            "intent_hash": intent_hash,
            "status": status,
            "reason_codes": ["CAPAUTH_AUTHORIZED"] if status == "AUTHORIZED" else ["CAPAUTH_REJECTED"],
            "authorization": {
                "decision": status,
                "authorized_quantity": 1 if status == "AUTHORIZED" else 0,
                "constraints": [],
                "decision_hash": _sha(f"decision:{intent_hash}:{status}"),
            },
            "input_manifest": [],
        },
    )
    return path


def _seed_equity_order_plan(truth_root: Path, *, day_utc: str, intent_hash: str, source_intent_id: str | None = None) -> Path:
    normalized_source_intent_id = source_intent_id or f"seed_intent_{day_utc}_v1"
    path = truth_root / "phaseC_preflight_v1" / day_utc / "attempt_A0001" / intent_hash / "equity_order_plan.v2.json"
    _write_json(
        path,
        {
            "schema_id": "equity_order_plan",
            "schema_version": "v2",
            "plan_id": f"{normalized_source_intent_id}_plan",
            "created_at_utc": f"{day_utc}T00:00:00Z",
            "intent_hash": _sha(f"intent:{intent_hash}"),
            "structure": "EQUITY_SPOT",
            "symbol": "SPY",
            "currency": "USD",
            "action": "BUY",
            "qty_shares": 1,
            "order_terms": {"order_type": "LIMIT", "limit_price": "100.00", "time_in_force": "DAY"},
            "risk_proof": None,
            "engine_id": "C2_TREND_EQ_PRIMARY_V1",
            "source_intent_id": normalized_source_intent_id,
            "intent_sha256": intent_hash,
            "lineage_envelope_ref": {"path": "lineage_envelope.v1.json", "sha256": _sha(f"lineage:{intent_hash}")},
            "canonical_json_hash": _sha(f"plan:{intent_hash}"),
        },
    )
    return path


def _seed_positions_snapshot(truth_root: Path, *, day_utc: str, binding_hash: str) -> Path:
    path = truth_root / "positions_v1" / "snapshots" / day_utc / "positions_snapshot.v4.json"
    _write_json(
        path,
        {
            "schema_id": "C2_POSITIONS_SNAPSHOT_V4",
            "schema_version": 4,
            "produced_utc": f"{day_utc}T00:00:00Z",
            "day_utc": day_utc,
            "producer": {"repo": "constellation", "git_sha": "7d64db4", "module": "test"},
            "status": "OK",
            "reason_codes": [],
            "input_manifest": [
                {
                    "type": "execution_evidence",
                    "path": "/tmp/execution_evidence",
                    "sha256": "1" * 64,
                    "day_utc": day_utc,
                    "producer": "execution_evidence_v1",
                }
            ],
            "positions": {
                "currency": "USD",
                "asof_utc": f"{day_utc}T00:00:00Z",
                "notes": [],
                "items": [
                    {
                        "position_id": binding_hash,
                        "engine_id": "C2_TREND_EQ_PRIMARY_V1",
                        "instrument": {"kind": "EQUITY", "symbol": "SPY", "currency": "USD", "ib_conId": None, "ib_localSymbol": None},
                        "qty": 0,
                        "avg_cost_cents": 0,
                        "market_exposure_type": "UNDEFINED_RISK",
                        "max_loss_cents": None,
                        "opened_day_utc": day_utc,
                        "status": "OPEN",
                    }
                ],
            },
        },
    )
    return path


def _seed_fill_ledger(truth_root: Path, *, day_utc: str, binding_hash: str, intent_hash: str) -> Path:
    path = truth_root / "fill_ledger_v1" / day_utc / f"{SUBMISSION_ID}.fill_ledger.v1.json"
    _write_json(
        path,
        {
            "schema_id": "C2_FILL_LEDGER_V1",
            "schema_version": 1,
            "produced_utc": f"{day_utc}T00:00:00Z",
            "day_utc": day_utc,
            "producer": {"repo": "constellation", "git_sha": "7d64db4", "module": "test"},
            "status": "OK",
            "reason_codes": [],
            "submission_id": SUBMISSION_ID,
            "binding_hash": binding_hash,
            "engine_id": "C2_TREND_EQ_PRIMARY_V1",
            "source_intent_id": f"intent_{day_utc}",
            "intent_sha256": intent_hash,
            "order_qty": 1,
            "filled_qty": 0,
            "remaining_qty": 1,
            "avg_fill_price_weighted": "0",
            "lifecycle_status": "OPEN",
            "event_hashes": [_sha("event")],
            "canonical_json_hash": _sha("fill"),
        },
    )
    return path


def test_canonical_sleeve_mapping_is_engine_based_and_primary_needs_engine() -> None:
    assert canonical_sleeve_id_from_engine_id("C2_TREND_EQ_PRIMARY_V1", repo_root=REPO_ROOT) == "C2_TREND_EQ_PRIMARY"
    assert (
        canonical_sleeve_id_from_execution_scope("PRIMARY", "C2_TREND_EQ_PRIMARY_V1", repo_root=REPO_ROOT)
        == "C2_TREND_EQ_PRIMARY"
    )
    with pytest.raises(ValueError, match="ENGINE_ID_MISSING"):
        canonical_sleeve_id_from_execution_scope("PRIMARY", "", repo_root=REPO_ROOT)


def test_portfolio_snapshot_writer_reads_authoritative_inputs_and_writes_schema_valid_output(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    truth_root = tmp_path / "truth"
    _seed_capital_risk_envelope(truth_root, day_utc=DAY)
    monkeypatch.setattr(sys, "argv", ["run_portfolio_governance_snapshot_day_v1.py", "--day", DAY, "--truth_root", str(truth_root)])
    assert portfolio_snapshot_writer.main() == 0
    out_path = truth_root / "risk_v1" / "portfolio_governance_snapshot_v1" / DAY / "portfolio_governance_snapshot.v1.json"
    out_obj = json.loads(out_path.read_text(encoding="utf-8"))
    validate_against_repo_schema_v1(
        out_obj,
        REPO_ROOT,
        "governance/04_DATA/SCHEMAS/C2/RISK/portfolio_governance_snapshot.v1.schema.json",
    )
    assert out_obj["policy_hash"]
    assert out_obj["sleeve_registry_hash"]


def test_allocation_epoch_writer_produces_schema_valid_additive_epoch_artifact(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    truth_root = tmp_path / "truth"
    _seed_capital_risk_envelope(truth_root, day_utc=DAY)
    _seed_capital_allocation(truth_root, day_utc=DAY, intent_hash=INTENT_HASH)
    monkeypatch.setattr(sys, "argv", ["run_portfolio_governance_snapshot_day_v1.py", "--day", DAY, "--truth_root", str(truth_root)])
    assert portfolio_snapshot_writer.main() == 0
    monkeypatch.setattr(sys, "argv", ["run_sleeve_allocation_epoch_day_v1.py", "--day", DAY, "--truth_root", str(truth_root)])
    assert allocation_epoch_writer.main() == 0
    epoch_files = sorted((truth_root / "allocation_v1" / "sleeve_allocation_epoch_v1" / DAY).glob("*.json"))
    assert len(epoch_files) == 1
    epoch_obj = json.loads(epoch_files[0].read_text(encoding="utf-8"))
    validate_against_repo_schema_v1(
        epoch_obj,
        REPO_ROOT,
        "governance/04_DATA/SCHEMAS/C2/ALLOCATION/sleeve_allocation_epoch.v1.schema.json",
    )
    assert epoch_obj["policy_hash"]
    assert epoch_obj["sleeve_registry_hash"]


def test_authorization_ledger_writer_emits_requested_authorized_reserved_with_epoch(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    truth_root = tmp_path / "truth"
    _seed_capital_risk_envelope(truth_root, day_utc=DAY)
    _seed_capital_allocation(truth_root, day_utc=DAY, intent_hash=INTENT_HASH)
    _seed_authorization_artifact(truth_root, day_utc=DAY, intent_hash=INTENT_HASH, status="AUTHORIZED")
    _seed_equity_order_plan(truth_root, day_utc=DAY, intent_hash=INTENT_HASH)
    monkeypatch.setattr(sys, "argv", ["run_portfolio_governance_snapshot_day_v1.py", "--day", DAY, "--truth_root", str(truth_root)])
    assert portfolio_snapshot_writer.main() == 0
    monkeypatch.setattr(sys, "argv", ["run_sleeve_allocation_epoch_day_v1.py", "--day", DAY, "--truth_root", str(truth_root)])
    assert allocation_epoch_writer.main() == 0
    monkeypatch.setattr(sys, "argv", ["run_intent_authorization_ledger_day_v1.py", "--day", DAY, "--truth_root", str(truth_root)])
    assert authorization_ledger_writer.main() == 0
    ledger_path = truth_root / "engine_activity_v1" / "intent_authorization_ledger_v1" / DAY / f"{INTENT_HASH}.intent_authorization_ledger.v1.jsonl"
    lines = [json.loads(line) for line in ledger_path.read_text(encoding="utf-8").splitlines() if line.strip()]
    assert [line["state"] for line in lines] == ["REQUESTED", "AUTHORIZED", "RESERVED"]
    assert lines[-1]["reserved_quantity"] == lines[-1]["authorized_quantity"] == 1
    assert lines[-1]["committed_quantity"] == 0
    assert lines[-1]["allocation_epoch_id"]
    for line in lines:
        validate_against_repo_schema_v1(
            line,
            REPO_ROOT,
            "governance/04_DATA/SCHEMAS/C2/ENGINE_ACTIVITY/intent_authorization_ledger.v1.schema.json",
        )


def test_exposure_ledger_writer_joins_position_fill_authorization_and_fails_closed_without_fill_bridge(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    truth_root = tmp_path / "truth"
    _seed_capital_risk_envelope(truth_root, day_utc=DAY_EXPOSURE)
    _seed_capital_allocation(truth_root, day_utc=DAY_EXPOSURE, intent_hash=INTENT_HASH_EXPOSURE)
    _seed_authorization_artifact(truth_root, day_utc=DAY_EXPOSURE, intent_hash=INTENT_HASH_EXPOSURE, status="AUTHORIZED")
    _seed_equity_order_plan(truth_root, day_utc=DAY_EXPOSURE, intent_hash=INTENT_HASH_EXPOSURE)
    _seed_positions_snapshot(truth_root, day_utc=DAY_EXPOSURE, binding_hash=BINDING_HASH)
    _seed_fill_ledger(truth_root, day_utc=DAY_EXPOSURE, binding_hash=BINDING_HASH, intent_hash=INTENT_HASH_EXPOSURE)
    monkeypatch.setattr(sys, "argv", ["run_portfolio_governance_snapshot_day_v1.py", "--day", DAY_EXPOSURE, "--truth_root", str(truth_root)])
    assert portfolio_snapshot_writer.main() == 0
    monkeypatch.setattr(sys, "argv", ["run_sleeve_allocation_epoch_day_v1.py", "--day", DAY_EXPOSURE, "--truth_root", str(truth_root)])
    assert allocation_epoch_writer.main() == 0
    monkeypatch.setattr(sys, "argv", ["run_intent_authorization_ledger_day_v1.py", "--day", DAY_EXPOSURE, "--truth_root", str(truth_root)])
    assert authorization_ledger_writer.main() == 0
    monkeypatch.setattr(sys, "argv", ["run_exposure_ledger_day_v1.py", "--day", DAY_EXPOSURE, "--truth_root", str(truth_root)])
    assert exposure_ledger_writer.main() == 0
    out_path = truth_root / "exposure_v1" / "exposure_ledger_v1" / DAY_EXPOSURE / f"{BINDING_HASH}.exposure_ledger.v1.json"
    out_obj = json.loads(out_path.read_text(encoding="utf-8"))
    validate_against_repo_schema_v1(
        out_obj,
        REPO_ROOT,
        "governance/04_DATA/SCHEMAS/C2/EXPOSURE/exposure_ledger.v1.schema.json",
    )
    assert out_obj["position_id"] == BINDING_HASH
    assert out_obj["binding_hash"] == BINDING_HASH
    assert out_obj["intent_hash"] == INTENT_HASH_EXPOSURE

    broken_truth_root = tmp_path / "broken_truth"
    _seed_capital_risk_envelope(broken_truth_root, day_utc=DAY_EXPOSURE)
    _seed_capital_allocation(broken_truth_root, day_utc=DAY_EXPOSURE, intent_hash=INTENT_HASH_EXPOSURE)
    _seed_authorization_artifact(broken_truth_root, day_utc=DAY_EXPOSURE, intent_hash=INTENT_HASH_EXPOSURE, status="AUTHORIZED")
    _seed_equity_order_plan(broken_truth_root, day_utc=DAY_EXPOSURE, intent_hash=INTENT_HASH_EXPOSURE)
    _seed_positions_snapshot(broken_truth_root, day_utc=DAY_EXPOSURE, binding_hash=BINDING_HASH)
    monkeypatch.setattr(sys, "argv", ["run_portfolio_governance_snapshot_day_v1.py", "--day", DAY_EXPOSURE, "--truth_root", str(broken_truth_root)])
    assert portfolio_snapshot_writer.main() == 0
    monkeypatch.setattr(sys, "argv", ["run_sleeve_allocation_epoch_day_v1.py", "--day", DAY_EXPOSURE, "--truth_root", str(broken_truth_root)])
    assert allocation_epoch_writer.main() == 0
    monkeypatch.setattr(sys, "argv", ["run_intent_authorization_ledger_day_v1.py", "--day", DAY_EXPOSURE, "--truth_root", str(broken_truth_root)])
    assert authorization_ledger_writer.main() == 0
    monkeypatch.setattr(sys, "argv", ["run_exposure_ledger_day_v1.py", "--day", DAY_EXPOSURE, "--truth_root", str(broken_truth_root)])
    with pytest.raises(SystemExit, match="FILL_LEDGER_DIR_MISSING|POSITION_FILL_LINK_MISSING"):
        exposure_ledger_writer.main()
