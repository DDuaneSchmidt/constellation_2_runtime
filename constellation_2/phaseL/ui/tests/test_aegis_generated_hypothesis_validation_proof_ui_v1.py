from __future__ import annotations

from pathlib import Path

from constellation_2.phaseL.ui.server import run_ops_dashboard_v1 as server
from ops.aegis.generated_hypothesis_validation_proof_v1 import (
    build_generated_hypothesis_validation_proof_v1,
    write_generated_hypothesis_validation_proof_v1,
)

ROOT = Path(__file__).resolve().parents[4]
UI = ROOT / "constellation_2/phaseL/ui/static/operator_shell/pages/index.js"
TRUTH = Path("/home/node/constellation_runtime_data/truth")
DAY = "2026-06-01"


def test_ui_renders_generated_hypothesis_validation_proof_section() -> None:
    text = UI.read_text(encoding="utf-8")
    assert "GENERATED HYPOTHESIS VALIDATION PROOF" in text
    assert "generated-hypothesis-validation-proof" in text
    assert "furthest_stage_reached" in text
    assert "reached_validation_sample_count" in text
    assert text.index("${renderOperatorDecisionGeneratedProgress(payload)}") < text.index("${renderGeneratedHypothesisValidationProof(payload)}")


def test_operator_payload_exposes_generated_hypothesis_validation_proof() -> None:
    proof = build_generated_hypothesis_validation_proof_v1(truth_root=TRUTH, day_utc=DAY, computed_at_utc="2026-06-01T12:00:00Z")
    write_generated_hypothesis_validation_proof_v1(truth_root=TRUTH, day_utc=DAY, payload=proof)
    payload = server._today_build_operator_envelope_v1(TRUTH, DAY, DAY)
    exposed = payload.get("generated_hypothesis_validation_proof_v1")
    assert isinstance(exposed, dict)
    assert exposed.get("schema_id") == "aegis_generated_hypothesis_validation_proof"
    assert payload.get("source_paths", {}).get("generated_hypothesis_validation_proof", "").endswith("generated_hypothesis_validation_proof.v1.json")
