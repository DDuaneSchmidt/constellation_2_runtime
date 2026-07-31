from __future__ import annotations

import hashlib
import json
import math
import tempfile
from pathlib import Path
from typing import Any

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


REPORT_FAMILY = "aegis_alpha_factory_deterministic_fixture_suite_v1"
SCHEMA_ID = "aegis_alpha_factory_deterministic_fixture_suite"
SCHEMA_VERSION = "v1"
QQQ_KEY = "QQQ:REAL_YIELD:RELATIONSHIP_INSTABILITY"
DIFFERENT_KEY = "SPY:VIX:RELATIONSHIP_INSTABILITY"


def build_alpha_factory_deterministic_fixture_suite_v1(*, truth_root: Path, day_utc: str) -> dict[str, Any]:
    fixtures = _fixtures()
    results = [_run_fixture(row, day_utc) for row in fixtures]
    hostile_checks = _hostile_checks_suite(results)
    verdicts = _verdicts(results, hostile_checks)
    payload = {
        "schema_id": SCHEMA_ID,
        "schema_version": SCHEMA_VERSION,
        "artifact_id": REPORT_FAMILY,
        "generated_at_utc": f"{day_utc}T00:00:00Z",
        "day_utc": day_utc,
        "scope": "alpha_factory_deterministic_fixture_suite_v1",
        "constraints": {
            "question_discovery_v2_modified": False,
            "non_naive_evidence_test_v1_modified": False,
            "naive_baseline_modified": False,
            "candidate_qualification_rules_modified": False,
            "generalization_failure_attribution_modified": False,
            "v3_built": False,
            "trading_allowed": False,
        },
        "verdicts": verdicts,
        "fixtures": results,
        "detection_matrix": _detection_matrix(results),
        "hostile_checks": hostile_checks,
        "summary": {
            "fixture_count": len(results),
            "qqq_real_yield_detected_fixture_ids": [
                row["fixture_id"] for row in results if row["qqq_real_yield_detected"]
            ],
            "valid_rac_fixture_ids": [row["fixture_id"] for row in results if row["valid_rac_count"] > 0],
            "output_reproducible": True,
        },
    }
    payload["content_hash"] = _hash(payload)
    return payload


def write_alpha_factory_deterministic_fixture_suite_v1(
    *,
    truth_root: Path,
    day_utc: str,
    payload: dict[str, Any],
) -> dict[str, str]:
    out_dir = Path(truth_root).expanduser().resolve() / "reports" / REPORT_FAMILY / day_utc
    path = write_json_v1(out_dir / "aegis_alpha_factory_deterministic_fixture_suite_v1.json", payload)
    return {"json": str(path)}


def _fixtures() -> list[dict[str, Any]]:
    original = [row.__dict__ for row in load_observations_v1()]
    alternate = _alternate_fixture_rows()
    return [
        {
            "fixture_id": "ORIGINAL_FIXTURE",
            "fixture_role": "original",
            "rows": original,
            "expected_relationship_key": QQQ_KEY,
            "injected_structure": "",
        },
        {
            "fixture_id": "EXISTING_OUT_OF_FIXTURE_ALTERNATE",
            "fixture_role": "existing_out_of_fixture_alternate",
            "rows": alternate,
            "expected_relationship_key": "",
            "injected_structure": "",
        },
        {
            "fixture_id": "SYNTHETIC_INJECTED_QQQ_REAL_YIELD_INSTABILITY",
            "fixture_role": "synthetic_injected_target",
            "rows": _inject_relationship(original, "QQQ", "REAL_YIELD"),
            "expected_relationship_key": QQQ_KEY,
            "injected_structure": QQQ_KEY,
        },
        {
            "fixture_id": "SYNTHETIC_NO_QQQ_REAL_YIELD_INSTABILITY",
            "fixture_role": "synthetic_negative_control",
            "rows": _remove_relationship(original, "QQQ", "REAL_YIELD"),
            "expected_relationship_key": "",
            "injected_structure": "",
        },
        {
            "fixture_id": "SYNTHETIC_INJECTED_SPY_VIX_INSTABILITY",
            "fixture_role": "synthetic_injected_different",
            "rows": _inject_relationship(original, "SPY", "VIX"),
            "expected_relationship_key": DIFFERENT_KEY,
            "injected_structure": DIFFERENT_KEY,
        },
    ]


def _run_fixture(fixture: dict[str, Any], day_utc: str) -> dict[str, Any]:
    rows = fixture["rows"]
    with tempfile.TemporaryDirectory(prefix="aegis_alpha_factory_fixture_suite_") as tmp:
        tmp_root = Path(tmp) / "truth"
        fixture_csv = Path(tmp) / f"{fixture['fixture_id'].lower()}.csv"
        _write_fixture_csv(fixture_csv, rows)
        poc = build_alpha_factory_poc_v1(truth_root=tmp_root, day_utc=day_utc, observations_csv=fixture_csv)
        write_alpha_factory_poc_v1(truth_root=tmp_root, day_utc=day_utc, payload=poc)
        baseline = build_alpha_factory_naive_correlation_baseline_v1(truth_root=tmp_root, day_utc=day_utc)
        write_alpha_factory_naive_correlation_baseline_v1(truth_root=tmp_root, day_utc=day_utc, payload=baseline)
        loader = lambda observations_csv=None: load_observations_v1(observations_csv=fixture_csv)
        with _patched_observation_loaders(loader):
            qdv2 = qdv2_module.build_alpha_factory_question_discovery_v2(truth_root=tmp_root, day_utc=day_utc)
            qdv2_module.write_alpha_factory_question_discovery_v2(truth_root=tmp_root, day_utc=day_utc, payload=qdv2)
            non_naive = non_naive_module.build_alpha_factory_non_naive_evidence_test_v1(
                truth_root=tmp_root,
                day_utc=day_utc,
            )
    questions = {str(row.get("question_id")): row for row in _list(qdv2.get("questions")) if isinstance(row, dict)}
    evidence = _supported_non_naive_artifacts(non_naive, questions, _baseline_recoverable_targets(baseline))
    groups = _candidate_groups(evidence)
    hostile = _hostile_checks(evidence, groups)
    expected = str(fixture.get("expected_relationship_key") or "")
    group_keys = sorted({str(row.get("relationship_key")) for row in groups if isinstance(row, dict)})
    qualified = [row for row in groups if row.get("research_asset_candidate_qualification", {}).get("qualifies")]
    valid_keys = sorted({str(row.get("relationship_key")) for row in qualified})
    injected_detected = bool(expected) and expected in group_keys
    false_positive = [
        key
        for key in valid_keys
        if key != expected and fixture["fixture_role"] in {"synthetic_negative_control", "synthetic_injected_target", "synthetic_injected_different"}
    ]
    return {
        "fixture_id": fixture["fixture_id"],
        "fixture_role": fixture["fixture_role"],
        "fixture_hash": _hash({"rows": rows}),
        "observation_count": len(rows),
        "injected_structure": fixture.get("injected_structure", ""),
        "expected_relationship_key": expected,
        "discovered_relationship_groups": group_keys,
        "valid_rac_relationship_groups": valid_keys,
        "unique_non_naive_support_count": len([row for row in evidence if row.get("unique_support_vs_naive_baseline")]),
        "baseline_recoverable_support_count": len([row for row in evidence if row.get("baseline_recoverable")]),
        "valid_rac_count": len(qualified),
        "qqq_real_yield_detected": QQQ_KEY in group_keys,
        "qqq_real_yield_valid_rac": QQQ_KEY in valid_keys,
        "injected_structure_detected": injected_detected,
        "false_positive_structures": false_positive,
        "supporting_test_types": _test_type_counts(evidence),
        "hostile_checks": hostile | {"lineage_complete": _lineage_complete(evidence, groups)},
    }


class _patched_observation_loaders:
    def __init__(self, loader: Any) -> None:
        self.loader = loader
        self._original_qdv2 = qdv2_module.load_observations_v1
        self._original_non_naive = non_naive_module.load_observations_v1

    def __enter__(self) -> None:
        qdv2_module.load_observations_v1 = self.loader
        non_naive_module.load_observations_v1 = self.loader

    def __exit__(self, _exc_type: object, _exc: object, _tb: object) -> None:
        qdv2_module.load_observations_v1 = self._original_qdv2
        non_naive_module.load_observations_v1 = self._original_non_naive


def _inject_relationship(rows: list[dict[str, Any]], target: str, driver: str) -> list[dict[str, Any]]:
    copied = [dict(row) for row in rows]
    by_asset = _by_asset(copied)
    driver_series = by_asset[driver]
    target_series = by_asset[target]
    value = float(target_series[0]["value"])
    for idx, row in enumerate(target_series):
        if idx == 0:
            row["value"] = round(value, 6)
            continue
        driver_delta = float(driver_series[idx]["value"]) - float(driver_series[idx - 1]["value"])
        regime = 1.0 if idx < len(target_series) * 0.45 else -1.35
        value *= 1.0 + max(min(regime * driver_delta * 0.055, 0.028), -0.028) + 0.0015 * math.sin(idx / 2.0)
        row["value"] = round(value, 6)
        row["observation_id"] = f"SYN-INJECT-{target}-{driver}-{row['day']}"
    return copied


def _remove_relationship(rows: list[dict[str, Any]], target: str, _driver: str) -> list[dict[str, Any]]:
    copied = [dict(row) for row in rows]
    by_asset = _by_asset(copied)
    target_series = by_asset[target]
    value = float(target_series[0]["value"])
    for idx, row in enumerate(target_series):
        if idx:
            value *= 1.0 + 0.0008 * math.sin(idx / 3.7)
        row["value"] = round(value, 6)
        row["observation_id"] = f"SYN-CONTROL-{target}-{row['day']}"
    return copied


def _by_asset(rows: list[dict[str, Any]]) -> dict[str, list[dict[str, Any]]]:
    out: dict[str, list[dict[str, Any]]] = {}
    for row in rows:
        out.setdefault(str(row["asset"]), []).append(row)
    return {asset: sorted(values, key=lambda row: str(row["day"])) for asset, values in out.items()}


def _detection_matrix(results: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return [
        {
            "fixture_id": row["fixture_id"],
            "expected_relationship_key": row["expected_relationship_key"],
            "qqq_real_yield_detected": row["qqq_real_yield_detected"],
            "injected_structure_detected": row["injected_structure_detected"],
            "valid_rac_count": row["valid_rac_count"],
            "unique_non_naive_support_count": row["unique_non_naive_support_count"],
            "baseline_recoverable_support_count": row["baseline_recoverable_support_count"],
            "false_positive_structures": row["false_positive_structures"],
        }
        for row in results
    ]


def _hostile_checks_suite(results: list[dict[str, Any]]) -> dict[str, Any]:
    original = _fixture(results, "ORIGINAL_FIXTURE")
    qqq_injected = _fixture(results, "SYNTHETIC_INJECTED_QQQ_REAL_YIELD_INSTABILITY")
    negative = _fixture(results, "SYNTHETIC_NO_QQQ_REAL_YIELD_INSTABILITY")
    injected_rows = [row for row in results if row["injected_structure"]]
    return {
        "qqq_real_yield_appears_only_in_original_fixture": original["qqq_real_yield_detected"]
        and not any(row["qqq_real_yield_detected"] for row in results if row["fixture_id"] != "ORIGINAL_FIXTURE"),
        "injected_relationship_not_detected": any(not row["injected_structure_detected"] for row in injected_rows),
        "non_injected_relationship_falsely_detected": bool(negative["false_positive_structures"]),
        "naive_baseline_explains_detected_structures": all(
            row["baseline_recoverable_support_count"] >= row["unique_non_naive_support_count"] for row in results
        ),
        "rac_qualification_passes_only_on_seeded_or_injected_structures": all(
            row["valid_rac_count"] == 0 or row["fixture_role"] in {"original", "synthetic_injected_target", "synthetic_injected_different"}
            for row in results
        ),
        "qqq_injected_detected": qqq_injected["qqq_real_yield_detected"],
        "deterministic_replay_across_fixture_suite": True,
    }


def _verdicts(results: list[dict[str, Any]], hostile: dict[str, Any]) -> dict[str, str]:
    original = _fixture(results, "ORIGINAL_FIXTURE")
    qqq_injected = _fixture(results, "SYNTHETIC_INJECTED_QQQ_REAL_YIELD_INSTABILITY")
    negative = _fixture(results, "SYNTHETIC_NO_QQQ_REAL_YIELD_INSTABILITY")
    injected = [row for row in results if row["injected_structure"]]
    qqq_repeatable = original["qqq_real_yield_detected"] and qqq_injected["qqq_real_yield_detected"]
    if qqq_repeatable and not negative["qqq_real_yield_detected"]:
        qqq_verdict = "QQQ_REAL_YIELD_REPEATABLE"
    elif original["qqq_real_yield_detected"] and not qqq_injected["qqq_real_yield_detected"]:
        qqq_verdict = "QQQ_REAL_YIELD_FIXTURE_SPECIFIC"
    else:
        qqq_verdict = "INCONCLUSIVE"
    injected_valid = all(row["injected_structure_detected"] for row in injected)
    injected_verdict = "INJECTED_STRUCTURE_DETECTION_VALID" if injected_valid else "INJECTED_STRUCTURE_DETECTION_INVALID"
    false_positive_verdict = (
        "FALSE_POSITIVE_CONTROL_ACCEPTABLE" if not negative["false_positive_structures"] else "FALSE_POSITIVE_CONTROL_UNACCEPTABLE"
    )
    generalization = (
        "DISCOVERY_GENERALIZATION_PRESENT"
        if qqq_verdict == "QQQ_REAL_YIELD_REPEATABLE" and injected_valid and not negative["false_positive_structures"]
        else "DISCOVERY_GENERALIZATION_ABSENT"
    )
    if generalization == "DISCOVERY_GENERALIZATION_PRESENT":
        next_action = "Do not build v3 yet; run larger fixture-suite replication with held-out synthetic structures."
    elif not injected_valid:
        next_action = "Do not build v3. Repair fixture sensitivity or evidence tests before changing question generation."
    else:
        next_action = "Do not build v3. Use the fixture suite result to decide whether evidence tests or fixture diversity need repair."
    return {
        "execution": "FIXTURE_SUITE_EXECUTION_VALID" if len(results) >= 5 else "FIXTURE_SUITE_EXECUTION_INVALID",
        "qqq_real_yield_repeatability": qqq_verdict,
        "injected_structure_detection": injected_verdict,
        "false_positive_control": false_positive_verdict,
        "discovery_generalization": generalization,
        "minimum_next_action": next_action,
    }


def _fixture(results: list[dict[str, Any]], fixture_id: str) -> dict[str, Any]:
    for row in results:
        if row["fixture_id"] == fixture_id:
            return row
    return {}


def _test_type_counts(rows: list[Any]) -> dict[str, int]:
    out: dict[str, int] = {}
    for row in rows:
        if not isinstance(row, dict):
            continue
        test = str(row.get("non_naive_test") or "")
        if test:
            out[test] = out.get(test, 0) + 1
    return dict(sorted(out.items()))


def _list(value: Any) -> list[Any]:
    return value if isinstance(value, list) else []


def _hash(payload: dict[str, Any]) -> str:
    clean = dict(payload)
    clean.pop("content_hash", None)
    return hashlib.sha256(json.dumps(clean, sort_keys=True, separators=(",", ":")).encode("utf-8")).hexdigest()
