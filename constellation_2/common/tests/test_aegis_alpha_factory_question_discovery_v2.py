from __future__ import annotations

from pathlib import Path
import sys

_REPO_ROOT = Path(__file__).resolve().parents[3]
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

from ops.aegis.alpha_factory_question_discovery_v2 import (
    FORBIDDEN_ANSWER_LABELS,
    build_alpha_factory_question_discovery_v2,
    write_alpha_factory_question_discovery_v2,
)


DAY = "2026-06-03"


def test_question_discovery_v2_artifact_generation(tmp_path: Path) -> None:
    payload = build_alpha_factory_question_discovery_v2(truth_root=tmp_path, day_utc=DAY)

    assert payload["schema_id"] == "aegis_alpha_factory_question_discovery"
    assert payload["schema_version"] == "v2"
    assert payload["questions"]
    paths = write_alpha_factory_question_discovery_v2(truth_root=tmp_path, day_utc=DAY, payload=payload)
    assert Path(paths["json"]).exists()


def test_question_discovery_v2_output_is_deterministic(tmp_path: Path) -> None:
    first = build_alpha_factory_question_discovery_v2(truth_root=tmp_path, day_utc=DAY)
    second = build_alpha_factory_question_discovery_v2(truth_root=tmp_path, day_utc=DAY)

    assert first["content_hash"] == second["content_hash"]


def test_question_discovery_v2_has_no_forbidden_answer_labels(tmp_path: Path) -> None:
    payload = build_alpha_factory_question_discovery_v2(truth_root=tmp_path, day_utc=DAY)
    combined = "\n".join(str(row["question"]) for row in payload["questions"])

    for label in FORBIDDEN_ANSWER_LABELS:
        assert label.lower() not in combined.lower()
    assert payload["verdicts"]["fixed_template_leakage"] == "FIXED_TEMPLATE_LEAKAGE_REMOVED"


def test_question_discovery_v2_questions_have_source_refs(tmp_path: Path) -> None:
    payload = build_alpha_factory_question_discovery_v2(truth_root=tmp_path, day_utc=DAY)

    for question in payload["questions"]:
        assert question["source_feature_refs"] or question["source_relationship_refs"]
        assert question["source_observation_refs"]


def test_question_discovery_v2_required_verdict_fields(tmp_path: Path) -> None:
    payload = build_alpha_factory_question_discovery_v2(truth_root=tmp_path, day_utc=DAY)
    verdicts = payload["verdicts"]

    assert verdicts["execution"] == "QUESTION_DISCOVERY_V2_EXECUTION_VALID"
    assert verdicts["fixed_template_leakage"] in {"FIXED_TEMPLATE_LEAKAGE_REMOVED", "STILL_PRESENT"}
    assert verdicts["questions_data_derived"] in {"QUESTIONS_DATA_DERIVED", "NOT_DATA_DERIVED"}
    assert verdicts["lineage"] in {"LINEAGE_COMPLETE", "LINEAGE_INCOMPLETE"}
    assert verdicts["rebenchmark_readiness"] in {"READY_FOR_REBENCHMARK", "NOT_READY_FOR_REBENCHMARK"}


def test_question_discovery_v2_lineage_complete(tmp_path: Path) -> None:
    payload = build_alpha_factory_question_discovery_v2(truth_root=tmp_path, day_utc=DAY)

    assert payload["verdicts"]["lineage"] == "LINEAGE_COMPLETE"
    for question in payload["questions"]:
        lineage = set(question["lineage"])
        assert set(question["source_feature_refs"]).issubset(lineage)
        assert set(question["source_observation_refs"]).issubset(lineage)
