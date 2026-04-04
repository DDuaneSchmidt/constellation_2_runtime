from __future__ import annotations

import json
import sys
from pathlib import Path

REPO_ROOT = Path("/home/node/constellation_2_clean").resolve()
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from constellation_2.common import gate_authority_foundation_v1 as gaf  # noqa: E402
from constellation_2.phaseD.lib.canon_json_v1 import canonical_json_bytes_v1  # noqa: E402
from ops.tools import run_capital_authority_allocation_day_v1 as capalloc  # noqa: E402


def _write_json(path: Path, obj: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(obj, sort_keys=True, separators=(",", ":")) + "\n", encoding="utf-8")


def _gate_row(gate_id: str, status: str, reason_codes: list[str] | None = None) -> dict:
    artifact = f"/tmp/fake/{gate_id}.json"
    return {
        "gate_id": gate_id,
        "gate_class": "CLASS1_SYSTEM_HARD_STOP",
        "required": True,
        "blocking": True,
        "status": status,
        "artifact_path": artifact,
        "artifact_sha256": "a" * 64,
        "reason_codes": reason_codes or ([] if status in ("PASS", "OK") else ["TEST_FAIL"]),
    }


def _write_gate_stack(
    truth_root: Path,
    day: str,
    rows: list[dict],
    *,
    status: str = "FAIL",
    blocking_class: str = "CLASS1_SYSTEM_HARD_STOP",
) -> None:
    _write_json(
        truth_root / "reports" / "gate_stack_verdict_v1" / day / "gate_stack_verdict.v1.json",
        {
            "schema_id": "gate_stack_verdict",
            "schema_version": "v1",
            "produced_utc": f"{day}T00:00:00Z",
            "day_utc": day,
            "producer": {"repo": "fixture", "module": "fixture", "git_sha": "abcdef1"},
            "status": status,
            "blocking_class": blocking_class,
            "reason_codes": [],
            "input_manifest": [],
            "gates": rows,
        },
    )


def _write_lifecycle_support(truth_root: Path, day: str) -> None:
    _write_json(
        truth_root / "reports" / "lifecycle_progression_status_v1" / day / "lifecycle_progression_status.v1.json",
        {
            "schema_id": "lifecycle_progression_status_v1",
            "schema_version": 1,
            "day_utc": day,
            "produced_utc": f"{day}T00:00:00Z",
            "execution_completion_ruleset_id": "EXECUTION_COMPLETION_RULESET_V1",
            "execution_completion_ruleset_version": 1,
            "lifecycle_completeness_policy_id": "C2_LIFECYCLE_COMPLETENESS_POLICY_V1",
            "lifecycle_completeness_policy_version": 1,
            "run_scope": {"day_utc": day, "mode": "PAPER", "truth_partition": str(truth_root)},
            "stages": [],
            "first_break_stage": "AUTHORIZATION_COMPLETE",
            "execution_truth_status": "EXECUTION_TRUTH_INCOMPLETE",
            "integrity_status": "PAYLOAD_INCOMPLETE",
            "evidence_refs": [],
        },
    )


def _prepare_pretrade_truth(truth_root: Path, day: str = "2026-04-02") -> None:
    _write_lifecycle_support(truth_root, day)
    _write_json(
        truth_root / "intents_v1" / "snapshots" / day / "intent.exposure_intent.v1.json",
        {
            "schema_id": "exposure_intent",
            "schema_version": "v1",
            "intent_id": "intent-1",
            "created_at_utc": f"{day}T00:00:00Z",
            "engine": {"engine_id": "C2_TREND_EQ_PRIMARY_V1", "mode": "PAPER"},
            "underlying": {"symbol": "SPY"},
        },
    )
    _write_gate_stack(
        truth_root,
        day,
        [
            _gate_row("feed_attestation_gate_v1", "PASS"),
            _gate_row("heartbeat_gate_v1", "FAIL", ["HB_MISSING"]),
            _gate_row("correlation_envelope_gate_v1", "MISSING", ["MISSING_GATE_ARTIFACT"]),
            _gate_row("replay_certification_gate_v1", "PASS", ["REPLAY_CERT_FIRST_RUN"]),
            _gate_row("capital_risk_envelope_v2", "MISSING", ["MISSING_GATE_ARTIFACT"]),
            _gate_row("liquidity_slippage_gate_v1", "MISSING", ["MISSING_GATE_ARTIFACT"]),
            _gate_row("operator_daily_gate_v3", "FAIL", ["MISSING_POSITIONS_SNAPSHOT"]),
        ],
    )


def _prepare_posttrade_truth(truth_root: Path, day: str = "2026-04-02") -> None:
    _prepare_pretrade_truth(truth_root, day)
    _write_json(truth_root / "positions_v1" / "snapshots" / day / "positions_snapshot.v2.json", {"schema_id": "positions_snapshot", "schema_version": "v2"})
    _write_gate_stack(
        truth_root,
        day,
        [
            _gate_row("feed_attestation_gate_v1", "PASS"),
            _gate_row("heartbeat_gate_v1", "PASS"),
            _gate_row("correlation_envelope_gate_v1", "PASS"),
            _gate_row("replay_certification_gate_v1", "PASS"),
            _gate_row("capital_risk_envelope_v2", "FAIL", ["NAV_MISSING"]),
            _gate_row("liquidity_slippage_gate_v1", "FAIL", ["LIQPOL_MISSING_NAV"]),
            _gate_row("operator_daily_gate_v3", "FAIL", ["MISSING_RECONCILIATION_REPORT_V3"]),
        ],
    )


def test_lifecycle_state_derivation_for_no_economic_truth_day(tmp_path: Path) -> None:
    truth_root = tmp_path / "truth_sleeves" / "PRIMARY" / "PAPER"
    _prepare_pretrade_truth(truth_root)
    facts = gaf.collect_gate_authority_facts(REPO_ROOT, truth_root, "2026-04-02")
    doc = gaf.derive_lifecycle_state_authority_doc(REPO_ROOT, truth_root, "2026-04-02", "2026-04-02T00:00:00Z", "PAPER", facts)
    assert doc["lifecycle_state"] == "PRE_TRADE"


def test_gate_classification_registry_validation() -> None:
    rows = gaf._classification_map(REPO_ROOT)
    assert rows["feed_attestation_gate_v1"]["required_in_authorization_verdict"] is True
    assert rows["operator_daily_gate_v3"]["required_in_economic_health_verdict"] is True


def test_authorization_verdict_includes_only_applicable_gates(tmp_path: Path) -> None:
    truth_root = tmp_path / "truth_sleeves" / "PRIMARY" / "PAPER"
    _prepare_pretrade_truth(truth_root)
    docs = gaf.build_gate_authority_docs(REPO_ROOT, truth_root, "2026-04-02", "2026-04-02T00:00:00Z", "PAPER")
    included = {row["gate_id"] for row in docs["authorization_verdict"]["included_gates"]}
    excluded = {row["gate_id"] for row in docs["authorization_verdict"]["excluded_gates"]}
    assert included == {
        "feed_attestation_gate_v1",
        "heartbeat_gate_v1",
        "correlation_envelope_gate_v1",
        "replay_certification_gate_v1",
    }
    assert {"capital_risk_envelope_v2", "liquidity_slippage_gate_v1", "operator_daily_gate_v3"} <= excluded


def test_economic_health_verdict_includes_only_applicable_gates(tmp_path: Path) -> None:
    truth_root = tmp_path / "truth_sleeves" / "PRIMARY" / "PAPER"
    _prepare_posttrade_truth(truth_root)
    docs = gaf.build_gate_authority_docs(REPO_ROOT, truth_root, "2026-04-02", "2026-04-02T00:00:00Z", "PAPER")
    included = {row["gate_id"] for row in docs["economic_verdict"]["included_gates"]}
    assert included == {
        "capital_risk_envelope_v2",
        "liquidity_slippage_gate_v1",
        "operator_daily_gate_v3",
    }


def test_gate_decision_ledger_records_exclusions_and_reasons(tmp_path: Path) -> None:
    truth_root = tmp_path / "truth_sleeves" / "PRIMARY" / "PAPER"
    _prepare_pretrade_truth(truth_root)
    docs = gaf.build_gate_authority_docs(REPO_ROOT, truth_root, "2026-04-02", "2026-04-02T00:00:00Z", "PAPER")
    by_gate = {row["gate_id"]: row for row in docs["ledger"]["inclusion_exclusion_decisions"]}
    assert by_gate["capital_risk_envelope_v2"]["included_in_authorization_verdict"] is False
    assert "LIFECYCLE_STATE_BEFORE_POST_TRADE_ECONOMIC" in by_gate["capital_risk_envelope_v2"]["exclusion_reasons"]


def test_authorization_and_economic_verdicts_are_independent(tmp_path: Path) -> None:
    truth_root = tmp_path / "truth_sleeves" / "PRIMARY" / "PAPER"
    _prepare_posttrade_truth(truth_root)
    docs = gaf.build_gate_authority_docs(REPO_ROOT, truth_root, "2026-04-02", "2026-04-02T00:00:00Z", "PAPER")
    assert docs["authorization_verdict"]["status"] == "PASS"
    assert docs["economic_verdict"]["status"] == "FAIL"


def test_pointer_and_allocation_consume_authorization_verdict_only(tmp_path: Path) -> None:
    truth_root = tmp_path / "truth_sleeves" / "PRIMARY" / "PAPER"
    day = "2026-04-02"
    auth_path = truth_root / "reports" / "authorization_gate_verdict_v1" / day / "authorization_gate_verdict.v1.json"
    _write_json(
        auth_path,
        {
            "schema_id": "authorization_gate_verdict_v1",
            "schema_version": 1,
            "gate_classification_registry_id": "GATE_CLASSIFICATION_REGISTRY_V1",
            "gate_classification_registry_version": 1,
            "lifecycle_state_authority_ref": "ref",
            "day_utc": day,
            "mode": "PAPER",
            "produced_utc": f"{day}T00:00:00Z",
            "included_gates": [],
            "excluded_gates": [],
            "blocking_gates": [],
            "status": "PASS",
            "blocking_class": "NONE",
            "reason_codes": ["AUTHORIZATION_GATES_PASS"],
            "evidence_refs": [],
            "decision_ledger_ref": "ledger",
        },
    )
    _write_json(
        truth_root / "run_pointer_v2" / "canonical_authority_head.v1.json",
        {
            "schema_id": "c2_run_pointer_canonical_authority_head",
            "schema_version": "v1",
            "pointer_seq": 1,
            "day_utc": day,
            "attempt_id": "attempt-1",
            "attempt_seq": 1,
            "mode": "PAPER",
            "status": "PASS",
            "authoritative": True,
            "produced_utc": f"{day}T00:00:00Z",
            "producer_git_sha": "abcdef1",
            "points_to": str(auth_path),
        },
    )
    doc = capalloc._require_authority_head_pass_authoritative(day, truth_root)
    assert doc["points_to"] == str(auth_path)


def test_replay_same_inputs_same_outputs(tmp_path: Path) -> None:
    truth_root = tmp_path / "truth_sleeves" / "PRIMARY" / "PAPER"
    _prepare_pretrade_truth(truth_root)
    docs1 = gaf.build_gate_authority_docs(REPO_ROOT, truth_root, "2026-04-02", "2026-04-02T00:00:00Z", "PAPER")
    docs2 = gaf.build_gate_authority_docs(REPO_ROOT, truth_root, "2026-04-02", "2026-04-02T00:00:00Z", "PAPER")
    assert canonical_json_bytes_v1(docs1["authorization_verdict"]) == canonical_json_bytes_v1(docs2["authorization_verdict"])
    assert canonical_json_bytes_v1(docs1["ledger"]) == canonical_json_bytes_v1(docs2["ledger"])


def test_internal_self_failure_path(tmp_path: Path, monkeypatch) -> None:
    truth_root = tmp_path / "truth_sleeves" / "PRIMARY" / "PAPER"
    _prepare_pretrade_truth(truth_root)

    def _boom(*args, **kwargs):
        raise RuntimeError("boom")

    monkeypatch.setattr(gaf, "build_gate_authority_docs", _boom)
    writes = gaf.write_gate_authority_plane(
        repo_root=REPO_ROOT,
        truth_root=truth_root,
        day_utc="2026-04-02",
        produced_utc="2026-04-02T00:00:00Z",
        mode="PAPER",
    )
    assert writes["authorization_verdict"].action == "WROTE"
    doc = json.loads(
        (truth_root / "reports" / "authorization_gate_verdict_v1" / "2026-04-02" / "authorization_gate_verdict.v1.json").read_text(
            encoding="utf-8"
        )
    )
    assert doc["status"] == "FAIL"
    assert doc["reason_codes"][0].startswith("INTERNAL_FAILURE:")
