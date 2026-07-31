from __future__ import annotations

from pathlib import Path
import sys

_REPO_ROOT = Path(__file__).resolve().parents[3]
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

from ops.aegis.alpha_factory_naive_correlation_baseline_v1 import (
    build_alpha_factory_naive_correlation_baseline_v1,
    write_alpha_factory_naive_correlation_baseline_v1,
)
from ops.aegis.alpha_factory_poc_v1 import build_alpha_factory_poc_v1, write_alpha_factory_poc_v1
from ops.aegis.alpha_factory_question_discovery_v2 import (
    FORBIDDEN_ANSWER_LABELS,
    build_alpha_factory_question_discovery_v2,
    write_alpha_factory_question_discovery_v2,
)
from ops.aegis.alpha_factory_question_discovery_v2_rebenchmark_v1 import (
    build_alpha_factory_question_discovery_v2_rebenchmark_v1,
    write_alpha_factory_question_discovery_v2_rebenchmark_v1,
)


DAY = "2026-06-03"


def _seed_inputs(root: Path) -> None:
    poc = build_alpha_factory_poc_v1(truth_root=root, day_utc=DAY)
    write_alpha_factory_poc_v1(truth_root=root, day_utc=DAY, payload=poc)
    baseline = build_alpha_factory_naive_correlation_baseline_v1(truth_root=root, day_utc=DAY)
    write_alpha_factory_naive_correlation_baseline_v1(truth_root=root, day_utc=DAY, payload=baseline)
    v2 = build_alpha_factory_question_discovery_v2(truth_root=root, day_utc=DAY)
    write_alpha_factory_question_discovery_v2(truth_root=root, day_utc=DAY, payload=v2)


def test_v2_rebenchmark_artifact_generation(tmp_path: Path) -> None:
    _seed_inputs(tmp_path)
    payload = build_alpha_factory_question_discovery_v2_rebenchmark_v1(truth_root=tmp_path, day_utc=DAY)

    assert payload["schema_id"] == "aegis_alpha_factory_question_discovery_v2_rebenchmark"
    assert payload["hypotheses"]
    assert payload["evidence"]
    assert payload["baseline_comparison"]["baseline_recoverability_measured" if False else "baseline_recovers_any_candidate"] in {True, False}
    paths = write_alpha_factory_question_discovery_v2_rebenchmark_v1(truth_root=tmp_path, day_utc=DAY, payload=payload)
    assert Path(paths["json"]).exists()


def test_v2_rebenchmark_output_is_deterministic(tmp_path: Path) -> None:
    _seed_inputs(tmp_path)
    first = build_alpha_factory_question_discovery_v2_rebenchmark_v1(truth_root=tmp_path, day_utc=DAY)
    second = build_alpha_factory_question_discovery_v2_rebenchmark_v1(truth_root=tmp_path, day_utc=DAY)

    assert first["content_hash"] == second["content_hash"]


def test_v2_rebenchmark_required_verdict_fields(tmp_path: Path) -> None:
    _seed_inputs(tmp_path)
    payload = build_alpha_factory_question_discovery_v2_rebenchmark_v1(truth_root=tmp_path, day_utc=DAY)
    verdicts = payload["verdicts"]

    assert verdicts["execution"] == "V2_REBENCHMARK_EXECUTION_VALID"
    assert verdicts["v2_vs_baseline"] in {"V2_OUTPERFORMS_BASELINE", "BASELINE_MATCHES_OR_EXCEEDS_V2", "INCONCLUSIVE"}
    assert verdicts["discovery_advantage"] in {
        "DISCOVERY_ADVANTAGE_PRESENT",
        "DISCOVERY_ADVANTAGE_ABSENT",
        "DISCOVERY_ADVANTAGE_INCONCLUSIVE",
    }
    assert verdicts["candidate_naming"] in {"CANDIDATE_NAMING_VALID", "OVERNAMED", "INCONCLUSIVE"}
    assert verdicts["lineage"] in {"LINEAGE_COMPLETE", "LINEAGE_INCOMPLETE"}
    assert verdicts["minimum_next_action"]


def test_v2_rebenchmark_hostile_checks(tmp_path: Path) -> None:
    _seed_inputs(tmp_path)
    payload = build_alpha_factory_question_discovery_v2_rebenchmark_v1(truth_root=tmp_path, day_utc=DAY)
    checks = payload["hostile_checks"]

    assert checks["fixed_template_leakage_reintroduced"] is False
    assert checks["pre_labeled_research_asset_names_before_grouping"] is False
    assert checks["baseline_recoverability_measured"] is True
    for label in FORBIDDEN_ANSWER_LABELS:
        assert label not in checks["forbidden_label_hits"]


def test_v2_rebenchmark_lineage_complete(tmp_path: Path) -> None:
    _seed_inputs(tmp_path)
    payload = build_alpha_factory_question_discovery_v2_rebenchmark_v1(truth_root=tmp_path, day_utc=DAY)

    assert payload["verdicts"]["lineage"] == "LINEAGE_COMPLETE"
    for evidence in payload["evidence"]:
        assert evidence["lineage"]
        assert evidence["hypothesis_id"]
