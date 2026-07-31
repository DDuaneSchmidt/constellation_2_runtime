from __future__ import annotations

import hashlib
import json
import tempfile
from pathlib import Path
from typing import Any, Callable

import ops.aegis.alpha_factory_non_naive_evidence_test_v1 as non_naive_module
import ops.aegis.alpha_factory_question_discovery_v2 as qdv2_module
from ops.aegis.alpha_factory_naive_correlation_baseline_v1 import (
    build_alpha_factory_naive_correlation_baseline_v1,
    write_alpha_factory_naive_correlation_baseline_v1,
)
from ops.aegis.alpha_factory_out_of_fixture_benchmark_v1 import _alternate_fixture_rows, _write_fixture_csv
from ops.aegis.alpha_factory_poc_v1 import build_alpha_factory_poc_v1, load_observations_v1, write_alpha_factory_poc_v1
from ops.aegis.alpha_factory_v2_non_naive_rebenchmark_v1 import (
    _baseline_recoverable_targets,
    _candidate_groups,
    _hostile_checks,
    _lineage_complete,
    _supported_non_naive_artifacts,
)
from ops.aegis.intelligence_common_v1 import write_json_v1


REPORT_FAMILY = "aegis_alpha_factory_v2_out_of_fixture_non_naive_rebenchmark_v1"
SCHEMA_ID = "aegis_alpha_factory_v2_out_of_fixture_non_naive_rebenchmark"
SCHEMA_VERSION = "v1"


def build_alpha_factory_v2_out_of_fixture_non_naive_rebenchmark_v1(*, truth_root: Path, day_utc: str) -> dict[str, Any]:
    fixture_rows = _alternate_fixture_rows()
    fixture_hash = _hash({"rows": fixture_rows})
    with tempfile.TemporaryDirectory(prefix="aegis_alpha_factory_v2_oof_nn_") as tmp:
        tmp_root = Path(tmp) / "truth"
        fixture_csv = Path(tmp) / "alternate_fixture_v1.csv"
        _write_fixture_csv(fixture_csv, fixture_rows)
        oof_poc = build_alpha_factory_poc_v1(truth_root=tmp_root, day_utc=day_utc, observations_csv=fixture_csv)
        write_alpha_factory_poc_v1(truth_root=tmp_root, day_utc=day_utc, payload=oof_poc)
        oof_baseline = build_alpha_factory_naive_correlation_baseline_v1(truth_root=tmp_root, day_utc=day_utc)
        write_alpha_factory_naive_correlation_baseline_v1(truth_root=tmp_root, day_utc=day_utc, payload=oof_baseline)
        loader = lambda observations_csv=None: load_observations_v1(observations_csv=fixture_csv)
        with _patched_observation_loaders(loader):
            oof_v2 = qdv2_module.build_alpha_factory_question_discovery_v2(truth_root=tmp_root, day_utc=day_utc)
            qdv2_module.write_alpha_factory_question_discovery_v2(truth_root=tmp_root, day_utc=day_utc, payload=oof_v2)
            oof_non_naive = non_naive_module.build_alpha_factory_non_naive_evidence_test_v1(
                truth_root=tmp_root,
                day_utc=day_utc,
            )

    questions = {str(row.get("question_id")): row for row in _list(oof_v2.get("questions")) if isinstance(row, dict)}
    baseline_recoverable_assets = _baseline_recoverable_targets(oof_baseline)
    evidence_artifacts = _supported_non_naive_artifacts(oof_non_naive, questions, baseline_recoverable_assets)
    candidate_groups = _candidate_groups(evidence_artifacts)
    hostile_checks = _hostile_checks(evidence_artifacts, candidate_groups)
    hostile_checks["insufficient_unique_non_naive_evidence_group_ids"] = [
        row["candidate_group_id"]
        for row in candidate_groups
        if row["research_asset_candidate_qualification"]["unique_non_naive_evidence_count"] < 2
    ]
    hostile_checks["lineage_complete"] = _lineage_complete(evidence_artifacts, candidate_groups)
    qualified = [row for row in candidate_groups if row["research_asset_candidate_qualification"]["qualifies"]]
    qualified_unique_nonzero = [
        row
        for row in qualified
        if row["research_asset_candidate_qualification"]["unique_non_naive_evidence_count"] >= 2
        and not row["research_asset_candidate_qualification"]["zero_sample_support_counted"]
    ]
    baseline_fully_explained = all(bool(row["baseline_recoverable_support"]) for row in qualified) if qualified else False
    lineage_complete = bool(hostile_checks["lineage_complete"])
    execution_valid = bool(oof_v2.get("questions")) and bool(oof_non_naive.get("question_evidence")) and bool(oof_baseline)
    discovery_present = bool(qualified_unique_nonzero) and not baseline_fully_explained and lineage_complete
    generalizes = discovery_present

    if generalizes:
        generalization_verdict = "V2_NON_NAIVE_GENERALIZES"
        comparison_verdict = "V2_NON_NAIVE_OUTPERFORMS_BASELINE"
        discovery_verdict = "DISCOVERY_ADVANTAGE_PRESENT"
        rac_verdict = "RESEARCH_ASSET_CANDIDATE_VALID"
        minimum_next_action = "Run a fixture-suite rebenchmark before any broad Alpha Factory success claim."
    elif candidate_groups and hostile_checks["support_mostly_baseline_recoverable"]:
        generalization_verdict = "V2_NON_NAIVE_DOES_NOT_GENERALIZE"
        comparison_verdict = "BASELINE_MATCHES_OR_EXCEEDS_V2_NON_NAIVE"
        discovery_verdict = "DISCOVERY_ADVANTAGE_ABSENT"
        rac_verdict = "RESEARCH_ASSET_CANDIDATE_INVALID"
        minimum_next_action = "Do not claim broad Alpha Factory success; out-of-fixture support remains mostly baseline recoverable."
    elif candidate_groups:
        generalization_verdict = "INCONCLUSIVE"
        comparison_verdict = "INCONCLUSIVE"
        discovery_verdict = "DISCOVERY_ADVANTAGE_INCONCLUSIVE"
        rac_verdict = "RESEARCH_ASSET_CANDIDATE_INCONCLUSIVE"
        minimum_next_action = "Add more deterministic out-of-fixture variants before making a generalization claim."
    else:
        generalization_verdict = "V2_NON_NAIVE_DOES_NOT_GENERALIZE"
        comparison_verdict = "BASELINE_MATCHES_OR_EXCEEDS_V2_NON_NAIVE"
        discovery_verdict = "DISCOVERY_ADVANTAGE_ABSENT"
        rac_verdict = "RESEARCH_ASSET_CANDIDATE_INVALID"
        minimum_next_action = "Do not claim broad Alpha Factory success; no out-of-fixture non-naive candidate group emerged."

    payload = {
        "schema_id": SCHEMA_ID,
        "schema_version": SCHEMA_VERSION,
        "artifact_id": REPORT_FAMILY,
        "generated_at_utc": f"{day_utc}T00:00:00Z",
        "day_utc": day_utc,
        "scope": "alpha_factory_v2_out_of_fixture_non_naive_rebenchmark_v1",
        "constraints": {
            "question_discovery_v2_modified": False,
            "non_naive_evidence_test_v1_modified": False,
            "v2_non_naive_rebenchmark_v1_modified": False,
            "naive_baseline_modified": False,
            "evidence_thresholds_modified": False,
            "candidate_qualification_rules_modified": False,
            "trading_allowed": False,
            "broad_alpha_factory_success_claimed": False,
        },
        "alternate_fixture": {
            "fixture_id": "alpha_factory_alternate_deterministic_daily_history_v1",
            "fixture_hash": fixture_hash,
            "observation_count": len(fixture_rows),
            "source": "ops.aegis.alpha_factory_out_of_fixture_benchmark_v1._alternate_fixture_rows",
        },
        "source_artifacts": {
            "out_of_fixture_poc_hash": _stable_artifact_hash(oof_poc),
            "out_of_fixture_question_discovery_v2_hash": _stable_artifact_hash(oof_v2),
            "out_of_fixture_non_naive_evidence_test_v1_hash": _stable_artifact_hash(oof_non_naive),
            "out_of_fixture_naive_baseline_v1_hash": _stable_artifact_hash(oof_baseline),
        },
        "verdicts": {
            "execution": "OUT_OF_FIXTURE_NON_NAIVE_EXECUTION_VALID"
            if execution_valid
            else "OUT_OF_FIXTURE_NON_NAIVE_EXECUTION_INVALID",
            "generalization": generalization_verdict,
            "v2_non_naive_vs_baseline": comparison_verdict,
            "discovery_advantage": discovery_verdict,
            "research_asset_candidate": rac_verdict,
            "lineage": "LINEAGE_COMPLETE" if lineage_complete else "LINEAGE_INCOMPLETE",
            "minimum_next_action": minimum_next_action,
        },
        "evidence_artifacts": evidence_artifacts,
        "candidate_groups": candidate_groups,
        "hostile_checks": hostile_checks,
        "summary": {
            "question_count": len(questions),
            "non_naive_supported_evidence_count": len(evidence_artifacts),
            "unique_supported_evidence_count": len([row for row in evidence_artifacts if row["unique_support_vs_naive_baseline"]]),
            "baseline_recoverable_supported_evidence_count": len([row for row in evidence_artifacts if row["baseline_recoverable"]]),
            "candidate_group_count": len(candidate_groups),
            "qualified_research_asset_candidate_count": len(qualified),
            "qualified_unique_nonzero_candidate_count": len(qualified_unique_nonzero),
            "naive_baseline_fully_explains_qualified_candidates": baseline_fully_explained,
            "output_reproducible": True,
        },
    }
    payload["content_hash"] = _hash(payload)
    return payload


def write_alpha_factory_v2_out_of_fixture_non_naive_rebenchmark_v1(
    *,
    truth_root: Path,
    day_utc: str,
    payload: dict[str, Any],
) -> dict[str, str]:
    out_dir = Path(truth_root).expanduser().resolve() / "reports" / REPORT_FAMILY / day_utc
    path = write_json_v1(out_dir / "aegis_alpha_factory_v2_out_of_fixture_non_naive_rebenchmark_v1.json", payload)
    return {"json": str(path)}


class _patched_observation_loaders:
    def __init__(self, loader: Callable[..., Any]) -> None:
        self.loader = loader
        self._original_qdv2 = qdv2_module.load_observations_v1
        self._original_non_naive = non_naive_module.load_observations_v1

    def __enter__(self) -> None:
        qdv2_module.load_observations_v1 = self.loader
        non_naive_module.load_observations_v1 = self.loader

    def __exit__(self, _exc_type: object, _exc: object, _tb: object) -> None:
        qdv2_module.load_observations_v1 = self._original_qdv2
        non_naive_module.load_observations_v1 = self._original_non_naive


def _list(value: Any) -> list[Any]:
    return value if isinstance(value, list) else []


def _stable_artifact_hash(payload: dict[str, Any]) -> str:
    return _hash(_normalize_for_hash(payload))


def _normalize_for_hash(value: Any) -> Any:
    if isinstance(value, dict):
        return {
            key: _normalize_for_hash(row)
            for key, row in value.items()
            if key not in {"content_hash", "source_poc_artifact"} and not str(key).endswith("_hash")
        }
    if isinstance(value, list):
        return [_normalize_for_hash(row) for row in value]
    if isinstance(value, str) and value.startswith("/tmp/"):
        return "<TEMP_PATH>"
    return value


def _hash(payload: dict[str, Any]) -> str:
    clean = dict(payload)
    clean.pop("content_hash", None)
    return hashlib.sha256(json.dumps(clean, sort_keys=True, separators=(",", ":")).encode("utf-8")).hexdigest()
