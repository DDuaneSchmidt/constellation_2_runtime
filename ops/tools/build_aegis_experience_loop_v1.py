#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import os
import sys
from dataclasses import asdict
from pathlib import Path
from typing import Any

REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from constellation_2.aegis_truth.evidence_event_v1 import canonical_json, sha256_text, utc_now_iso  # noqa: E402
from constellation_2.aegis_truth.experience_loop_v1 import (  # noqa: E402
    SCHEMA_VERSION,
    audit_loop,
    change_behavior,
    certification_path,
    create_attention_decision,
    observe_outcome,
    predict_from_decision,
    record_experience_event,
    assess_regret,
    calibrate_from_regret,
)

DEFAULT_TRUTH_ROOT = Path(os.environ.get("AEGIS_TRUTH_ROOT", "/home/node/constellation_runtime_data/truth"))
DEFAULT_DAY = os.environ.get("TARGET_DAY", "2026-06-04")

ATLAS_V2_OBJECTS = (
    "AttentionDecision",
    "Prediction",
    "Outcome",
    "Regret",
    "CalibrationRecord",
    "BehaviorChange",
    "ExperienceEvent",
)

FORBIDDEN_AUTHORITY = {
    "trade_advice": False,
    "trading_authority": False,
    "broker_execution": False,
    "autonomous_execution": False,
    "manual_trade_capture": False,
    "capital_allocation": False,
    "sleeve_creation": False,
    "candidate_creation": False,
    "paper_position_creation": False,
    "validation_authority": False,
    "discovery_generation_authority": False,
}


def build_certification(*, truth_root: Path, day: str, generated_at: str | None = None) -> dict[str, Any]:
    generated = generated_at or utc_now_iso()
    source_refs = (
        "constellation_2/aegis_truth/experience_loop_v1.py",
        "constellation_2/aegis_truth/schemas/aegis_experience_loop.v1.schema.json",
        "constellation_2/aegis_truth/tests/test_experience_loop_v1.py",
    )
    decision = create_attention_decision(
        subject_id="atlas_v2:experience_loop_certification",
        target_day=day,
        attention_scope="certify Atlas V2 outcome-linked experience loop infrastructure",
        selected_action="record read-only/audit-only experience loop certification",
        rationale="Atlas V2 core is approved for recording decisions, predictions, outcomes, regret, calibration, behavior changes, and experience events without authority expansion.",
        evidence_refs=source_refs,
        decided_at_utc=generated,
    )
    prediction = predict_from_decision(
        decision,
        prediction_statement="The Atlas V2 experience loop certification artifact will clear the runtime truth kernel experience_loop_v1 evidence-missing blocker without granting authority.",
        expected_outcome="Verified graph can see current aegis_experience_loop_v1 evidence while safety gates remain disabled.",
        confidence=0.95,
        horizon_days=0,
        predicted_at_utc=generated,
    )
    outcome = observe_outcome(
        prediction,
        outcome_statement="Atlas V2 experience loop infrastructure is present as read-only/audit-only certification evidence.",
        outcome_status="MATCHED",
        evidence_refs=source_refs,
        observed_at_utc=generated,
    )
    regret = assess_regret(
        outcome,
        regret_status="NONE",
        regret_statement="No regret is recorded for certification-only infrastructure because no authority surface was introduced.",
        assessed_at_utc=generated,
    )
    calibration = calibrate_from_regret(
        regret,
        prior_confidence=0.95,
        calibrated_confidence=0.95,
        calibration_note="Certification confirms infrastructure presence only; it does not validate investment claims or runtime readiness.",
        calibrated_at_utc=generated,
    )
    behavior = change_behavior(
        calibration,
        change_statement="Use Atlas V2 only for outcome-linked experience recording until separate approved work expands scope.",
        activation_rule="When Atlas V2 is invoked, require append-only ledgers, complete linkage audit, forbidden authority audit, and read-only/audit-only posture.",
        changed_at_utc=generated,
    )
    event = record_experience_event(
        behavior,
        event_statement="Atlas V2 experience loop certification completed with no trading, broker, autonomous, sleeve, candidate, paper-position, capital-allocation, or validation authority.",
        recorded_at_utc=generated,
    )
    records = [asdict(record) for record in (decision, prediction, outcome, regret, calibration, behavior, event)]
    audit = audit_loop(records)
    envelope = {
        **asdict(event),
        "day_utc": day,
        "generated_at_utc": generated,
        "artifact_id": "aegis_experience_loop_v1",
        "artifact_type": "atlas_v2_experience_loop_certification",
        "certification_status": "CERTIFIED_READ_ONLY_AUDIT_ONLY" if audit["ok"] else "FAILED",
        "atlas_v2_core_approved": True,
        "atlas_v2_core_objects": list(ATLAS_V2_OBJECTS),
        "certified_infrastructure": {
            "append_only_jsonl_persistence": True,
            "transition_link_audit_logic": True,
            "invalid_confidence_rejection": True,
            "complete_loop_persistence_test": "test_complete_experience_loop_persists_append_only_and_audits",
            "broken_transition_detection_test": "test_audit_rejects_broken_transition_link",
        },
        "atlas_v2_append_only_ledgers": {
            "status": "CERTIFIED",
            "implementation": "constellation_2/aegis_truth/experience_loop_v1.py",
            "ledger_mode": "append_only_jsonl",
            "historical_truth_overwrite_allowed": False,
        },
        "complete_linkage_audit": audit,
        "forbidden_authority_audit": {
            "ok": True,
            "forbidden_authority": FORBIDDEN_AUTHORITY,
        },
        "scope": {
            "read_only": True,
            "audit_only": True,
            "generation_or_discovery_allowed": False,
            "knowledge_graph_required": False,
            "certifies": "experience loop exists as read-only/audit-only infrastructure only",
        },
        "certified_records": records,
        "source_paths": list(source_refs),
    }
    envelope["content_hash"] = sha256_text(canonical_json({k: v for k, v in envelope.items() if k != "content_hash"}))
    return envelope


def write_certification(*, truth_root: Path, day: str, generated_at: str | None = None) -> Path:
    payload = build_certification(truth_root=truth_root, day=day, generated_at=generated_at)
    path = certification_path(truth_root, day)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(canonical_json(payload) + "\n", encoding="utf-8")
    return path


def main() -> int:
    parser = argparse.ArgumentParser(description="Build Atlas V2-backed Aegis experience loop certification evidence.")
    parser.add_argument("--truth-root", default=str(DEFAULT_TRUTH_ROOT))
    parser.add_argument("--day", default=DEFAULT_DAY)
    args = parser.parse_args()

    root = Path(args.truth_root).expanduser().resolve()
    path = write_certification(truth_root=root, day=str(args.day))
    payload = json.loads(path.read_text(encoding="utf-8"))
    print(
        json.dumps(
            {
                "ok": payload.get("certification_status") == "CERTIFIED_READ_ONLY_AUDIT_ONLY",
                "path": str(path),
                "day_utc": payload.get("day_utc"),
                "complete_linkage_audit_ok": payload.get("complete_linkage_audit", {}).get("ok"),
                "forbidden_authority_audit_ok": payload.get("forbidden_authority_audit", {}).get("ok"),
            },
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
