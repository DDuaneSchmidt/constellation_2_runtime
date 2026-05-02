from __future__ import annotations

import json
from pathlib import Path

import pytest
from jsonschema import Draft202012Validator

import ops.tools.run_advisory_evidence_gateway_v1 as gateway
from ops.tools.run_ai_advisory_review_v1 import build_ai_advisory_review_v1


DAY = "2026-05-02"
COMMIT = "8" * 40


def _write(path: Path, payload: dict | str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    if isinstance(payload, str):
        path.write_text(payload, encoding="utf-8")
    else:
        path.write_text(json.dumps(payload, sort_keys=True), encoding="utf-8")


def _producer_contract(commit: str = COMMIT, dirty: str = "CLEAN") -> dict:
    return {
        "schema_version": "producer_contract.v1",
        "producer_name": "producer.py",
        "producer_command": "python3 producer.py",
        "code_version_git_commit": commit,
        "source_dirty_status": dirty,
        "generated_at_utc": "2026-05-02T15:00:00Z",
        "input_artifacts": [],
        "output_artifacts": [],
        "schema_versions": {},
        "deterministic_fingerprint": "f" * 64,
    }


def _schema(name: str) -> dict:
    return json.loads((gateway.REPO_ROOT / "governance" / "04_DATA" / "SCHEMAS" / "C2" / "REPORTS" / name).read_text(encoding="utf-8"))


def _seed_production(tmp_path: Path, *, control_day: str = DAY, control_commit: str = COMMIT, dirty: str = "CLEAN") -> tuple[Path, Path]:
    runtime = tmp_path / "runtime"
    prod = runtime / "production_truth"
    _write(
        prod / "governance" / "production_version.v1.json",
        {
            "schema_version": "production_version.v1",
            "promoted_commit": COMMIT,
            "promoted_at_utc": "2026-05-02T15:00:00Z",
            "promoted_by": "test",
            "promotion_id": "p",
            "rollback_commit": "7" * 40,
            "status": "ACTIVE",
        },
    )
    _write(
        prod / "reports" / "aegis_promotion_validation_ledger_v1" / DAY / "promotion_validation_ledger.v1.json",
        {
            "promotion_status": "PROMOTED",
            "candidate_commit": COMMIT,
            "promoted_commit": COMMIT,
            "truth_root": str(prod.resolve()),
            "runtime_root": str(prod.resolve()),
            "blockers": [],
        },
    )
    _write(
        prod / "exports" / "aegis_state" / "latest" / "chatgpt_aegis_packet.md",
        f"- git_commit: {COMMIT}\n- git_dirty_status: CLEAN\n",
    )
    _write(
        prod / "reports" / "aegis_control_plane_v1" / DAY / "control_plane.v1.json",
        {
            "day_utc": control_day,
            "final_status": "NOT_READY",
            "current_domain": "SESSION_IDENTITY",
            "current_phase": "SESSION_AUTHORITY",
            "canonical_blocker": "NON_TRADING_DAY",
            "submit_allowed": False,
            "deferred_domains": ["BROKER_CONNECTIVITY"],
            "producer_contract_v1": _producer_contract(control_commit, dirty),
        },
    )
    return prod, runtime


@pytest.fixture(autouse=True)
def _stable_git(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(gateway, "git_commit_v1", lambda: COMMIT)


def test_gateway_copies_control_plane_and_excludes_legacy_truth(tmp_path: Path) -> None:
    prod, runtime = _seed_production(tmp_path)
    legacy = runtime / "truth" / "reports" / "eod_review_v1" / DAY / "eod_review.v1.json"
    _write(legacy, {"schema_id": "eod_review_v1", "day_utc": DAY, "status": "READY_IDLE"})

    payload = gateway.build_advisory_evidence_packet_v1(day_utc=DAY, truth_root=prod, runtime_root=prod)

    assert payload["status"] == "PASS"
    assert payload["control_plane_final_status"] == "NOT_READY"
    assert payload["control_plane_current_domain"] == "SESSION_IDENTITY"
    assert payload["control_plane_submit_allowed"] is False
    assert any(row["artifact_type"] == "aegis_control_plane_v1" for row in payload["included_artifacts"])
    assert any(row["reason"] == "LEGACY_TRUTH_EXCLUDED" for row in payload["excluded_artifacts"])
    assert payload["non_actionable_deferred_evidence"][0]["actionable"] is False


def test_missing_control_plane_blocks_ai_consumption(tmp_path: Path) -> None:
    prod, _runtime = _seed_production(tmp_path)
    (prod / "reports" / "aegis_control_plane_v1" / DAY / "control_plane.v1.json").unlink()

    payload = gateway.build_advisory_evidence_packet_v1(day_utc=DAY, truth_root=prod, runtime_root=prod)

    assert payload["status"] == "BLOCKED"
    assert payload["ai_consumption_allowed"] is False
    assert "CONTROL_PLANE_UNUSABLE_FOR_ADVISORY_PACKET" in payload["warnings"]


def test_wrong_day_control_plane_blocks_packet(tmp_path: Path) -> None:
    prod, _runtime = _seed_production(tmp_path, control_day="2026-05-01")

    payload = gateway.build_advisory_evidence_packet_v1(day_utc=DAY, truth_root=prod, runtime_root=prod)

    assert payload["status"] == "BLOCKED"
    assert any(row["reason"] == "WRONG_DAY" for row in payload["excluded_artifacts"])


def test_dirty_or_unpromoted_control_plane_blocks_packet(tmp_path: Path) -> None:
    prod, _runtime = _seed_production(tmp_path, dirty="DIRTY")
    payload = gateway.build_advisory_evidence_packet_v1(day_utc=DAY, truth_root=prod, runtime_root=prod)
    assert payload["status"] == "BLOCKED"
    assert any(row["reason"] == "DIRTY_SOURCE_ARTIFACT" for row in payload["excluded_artifacts"])

    prod, _runtime = _seed_production(tmp_path / "b", control_commit="9" * 40)
    payload = gateway.build_advisory_evidence_packet_v1(day_utc=DAY, truth_root=prod, runtime_root=prod)
    assert payload["status"] == "BLOCKED"
    assert any(row["reason"] == "PROMOTED_COMMIT_MISMATCH" for row in payload["excluded_artifacts"])


def test_forbidden_authority_fields_are_rejected() -> None:
    with pytest.raises(ValueError, match="FORBIDDEN_UNQUALIFIED_AUTHORITY_FIELDS"):
        gateway._assert_no_forbidden_authority_fields({"final_status": "READY"})


def test_ai_review_requires_governed_advisory_packet(tmp_path: Path) -> None:
    truth = tmp_path / "production_truth"

    payload = build_ai_advisory_review_v1(day_utc=DAY, truth_root=truth)

    assert payload["status"] == "NO_GOVERNED_ADVISORY_INPUT"
    assert payload["recommendations"] == []
    assert payload["authority"] == "ADVISORY_ONLY"


def test_ai_review_consumes_only_gateway_packet(tmp_path: Path) -> None:
    prod, runtime = _seed_production(tmp_path)
    path, packet = gateway.run_advisory_evidence_gateway_v1(DAY, str(prod), str(prod))
    assert packet["status"] == "PASS"

    payload = build_ai_advisory_review_v1(day_utc=DAY, truth_root=prod)

    assert payload["status"] == "PASS"
    assert payload["advisory_evidence_packet_path"] == str(path)
    assert payload["evidence_paths"] == [str(path)]
    assert payload["recommendations"] == []
    assert payload["control_plane_final_status_observed"] == "NOT_READY"


def test_ai_advisory_review_schema_rejects_missing_metadata_and_authority_fields(tmp_path: Path) -> None:
    schema = _schema("ai_advisory_review.v1.schema.json")
    valid = {
        "schema_id": "ai_advisory_review",
        "schema_version": "v1",
        "artifact_id": "ai_advisory_review_v1",
        "day_utc": DAY,
        "generated_at": "2026-05-02T15:00:00Z",
        "git_commit": COMMIT,
        "git_dirty_status": "CLEAN",
        "truth_root": str(tmp_path),
        "runtime_root": str(tmp_path),
        "producer": "ops/tools/run_ai_advisory_review_v1.py",
        "authority": "ADVISORY_ONLY",
        "readiness_authority": "aegis_control_plane_v1",
        "submit_authority": "aegis_submit_enforcement_v1",
        "operator_action_authority": "CONTROL_PLANE_OR_GOVERNED_RECOVERY_ONLY",
        "environment": "PAPER",
        "status": "NO_GOVERNED_ADVISORY_INPUT",
        "advisory_evidence_packet_path": str(tmp_path / "packet.json"),
        "advisory_evidence_packet_status": "BLOCKED",
        "control_plane_final_status_observed": "NOT_READY",
        "control_plane_current_domain_observed": "SESSION_IDENTITY",
        "control_plane_canonical_blocker_observed": "NON_TRADING_DAY",
        "control_plane_submit_allowed_observed": False,
        "recommendations": [],
        "warnings": [],
        "requires_human_review": True,
        "evidence_paths": [],
        "producer_contract_v1": _producer_contract(),
    }
    Draft202012Validator(schema).validate(valid)

    missing = dict(valid)
    missing.pop("git_commit")
    assert list(Draft202012Validator(schema).iter_errors(missing))

    forbidden = dict(valid)
    forbidden["submit_allowed"] = True
    assert list(Draft202012Validator(schema).iter_errors(forbidden))


def test_not_governed_eod_artifact_is_quarantined_even_when_schema_valid(tmp_path: Path) -> None:
    prod, _runtime = _seed_production(tmp_path)
    eod = prod / "reports" / "eod_review_v1" / DAY / "eod_review.v1.json"
    _write(
        eod,
        {
            "schema_id": "eod_review",
            "schema_version": "v1",
            "artifact_id": "eod_review_v1",
            "day_utc": DAY,
            "generated_at": "2026-05-02T15:00:00Z",
            "git_commit": COMMIT,
            "git_dirty_status": "CLEAN",
            "truth_root": str(prod.resolve()),
            "runtime_root": str(prod.resolve()),
            "producer": "ops/tools/run_eod_review_v1.py",
            "authority": "ADVISORY_ONLY",
            "readiness_authority": "aegis_control_plane_v1",
            "status": "PASS",
            "advisory_summary": {},
            "non_actionable_recommendations": [],
            "warnings": [],
            "requires_human_review": True,
            "producer_contract_v1": _producer_contract(),
        },
    )

    payload = gateway.build_advisory_evidence_packet_v1(day_utc=DAY, truth_root=prod, runtime_root=prod)

    assert not any(row["artifact_type"] == "eod_review_v1" for row in payload["included_artifacts"])
    assert any(row["artifact_type"] == "eod_review_v1" and row["reason"] == "NOT_GOVERNED" for row in payload["excluded_artifacts"])


def test_migrated_weekly_scorecard_is_included_only_when_governed_and_schema_valid(tmp_path: Path) -> None:
    prod, _runtime = _seed_production(tmp_path)
    weekly = prod / "reports" / "weekly_scorecard_view_v1" / DAY / "weekly_scorecard_view.v1.json"
    contract = _producer_contract()
    contract["producer_name"] = "constellation_2.common.governed_evaluation_v1"
    _write(
        weekly,
        {
            "schema_id": "C2_WEEKLY_SCORECARD_VIEW_V1",
            "schema_version": "v1",
            "artifact_id": "weekly_scorecard_view_v1",
            "day_utc": DAY,
            "generated_at": "2026-05-02T15:00:00Z",
            "generated_at_utc": "2026-05-02T15:00:00Z",
            "git_commit": COMMIT,
            "git_dirty_status": "CLEAN",
            "truth_root": str(prod.resolve()),
            "runtime_root": str(prod.resolve()),
            "producer": "constellation_2.common.governed_evaluation_v1",
            "authority": "ADVISORY_ONLY",
            "readiness_authority": "aegis_control_plane_v1",
            "advisory_status": "ANALYSIS_ONLY",
            "surface_kind": "projection",
            "window_label": "WEEK_ENDING_2026-05-02",
            "portfolio_summary": {},
            "sleeve_rows": [],
            "source_refs": [],
            "warnings": [],
            "requires_human_review": True,
            "producer_contract_v1": contract,
        },
    )

    payload = gateway.build_advisory_evidence_packet_v1(day_utc=DAY, truth_root=prod, runtime_root=prod)

    assert any(row["artifact_type"] == "weekly_scorecard_view_v1" for row in payload["included_artifacts"])


def test_readiness_like_fields_are_quarantined_for_advisory_artifacts(tmp_path: Path) -> None:
    prod, _runtime = _seed_production(tmp_path)
    weekly = prod / "reports" / "weekly_scorecard_view_v1" / DAY / "weekly_scorecard_view.v1.json"
    contract = _producer_contract()
    contract["producer_name"] = "constellation_2.common.governed_evaluation_v1"
    _write(
        weekly,
        {
            "schema_id": "C2_WEEKLY_SCORECARD_VIEW_V1",
            "schema_version": "v1",
            "artifact_id": "weekly_scorecard_view_v1",
            "day_utc": DAY,
            "generated_at": "2026-05-02T15:00:00Z",
            "git_commit": COMMIT,
            "git_dirty_status": "CLEAN",
            "truth_root": str(prod.resolve()),
            "runtime_root": str(prod.resolve()),
            "producer": "constellation_2.common.governed_evaluation_v1",
            "authority": "ADVISORY_ONLY",
            "readiness_authority": "aegis_control_plane_v1",
            "advisory_status": "ANALYSIS_ONLY",
            "portfolio_summary": {},
            "sleeve_rows": [],
            "source_refs": [],
            "warnings": [],
            "requires_human_review": True,
            "producer_contract_v1": contract,
            "final_status": "READY",
        },
    )

    payload = gateway.build_advisory_evidence_packet_v1(day_utc=DAY, truth_root=prod, runtime_root=prod)

    assert not any(row["artifact_type"] == "weekly_scorecard_view_v1" for row in payload["included_artifacts"])
    assert any(row["artifact_type"] == "weekly_scorecard_view_v1" and str(row["reason"]).startswith("SCHEMA_INVALID") for row in payload["excluded_artifacts"])
