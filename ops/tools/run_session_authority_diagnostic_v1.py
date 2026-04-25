#!/usr/bin/env python3
from __future__ import annotations

from pathlib import Path
from typing import Any, Dict, Iterable, Mapping

from constellation_2.common.artifact_authority_v1 import get_artifact_contract_v1
from constellation_2.common.session_authority_v1 import (
    HIDDEN_DEPENDENCY_STATUS_FAIL,
    HIDDEN_DEPENDENCY_STATUS_PASS,
)

REASON_HIDDEN_DEPENDENCY_DETECTED = "HIDDEN_DEPENDENCY_DETECTED"
REASON_PARTIAL_BUILD = "PARTIAL_BUILD"


def _source_ref_blocks_build_v1(
    ref: Mapping[str, Any],
    *,
    downstream_build_cycle_scripts: Iterable[str],
) -> bool:
    if not isinstance(ref, Mapping):
        return False
    script = str(ref.get("script") or "").strip()
    if script in {"", "source_ref"}:
        return False
    if script in set(downstream_build_cycle_scripts):
        return False
    if bool(ref.get("required_for_closure") is False):
        return False
    return int(ref.get("return_code") or 0) != 0


def compute_hidden_dependency_check_result_v1(
    *,
    repo_root: Path,
    artifact_results: Iterable[Mapping[str, Any]],
    source_refs: Iterable[Mapping[str, Any]],
    downstream_build_cycle_scripts: Iterable[str],
) -> Dict[str, Any]:
    declared = {
        str(row.get("artifact_id") or "").strip()
        for row in artifact_results
        if str(row.get("artifact_id") or "").strip()
    }
    contract_covered: set[str] = set()
    for artifact_id in declared:
        try:
            get_artifact_contract_v1(repo_root, artifact_id)
        except Exception:
            continue
        contract_covered.add(artifact_id)
    observed = {
        str(dep).strip()
        for row in artifact_results
        for dep in (row.get("observed_dependency_artifacts") or [])
        if str(dep).strip()
    }
    undeclared = sorted(observed - declared)
    partial_build = any(
        _source_ref_blocks_build_v1(
            ref,
            downstream_build_cycle_scripts=downstream_build_cycle_scripts,
        )
        for ref in source_refs
    )
    failing_producers = list(
        dict.fromkeys(
            str(ref.get("script") or "").strip()
            for ref in source_refs
            if str(ref.get("script") or "").strip() not in {"", "source_ref"}
            and int(ref.get("return_code") or 0) != 0
        )
    )
    summary_parts = []
    if undeclared:
        summary_parts.append(f"undeclared_dependency_artifacts={','.join(undeclared)}")
    if contract_covered:
        summary_parts.append(f"contract_covered={','.join(sorted(contract_covered))}")
    if failing_producers:
        summary_parts.append(f"failing_producers={','.join(failing_producers)}")
    if undeclared:
        status = HIDDEN_DEPENDENCY_STATUS_FAIL
        blocking_reason_code = REASON_HIDDEN_DEPENDENCY_DETECTED
    elif partial_build:
        status = HIDDEN_DEPENDENCY_STATUS_FAIL
        blocking_reason_code = REASON_PARTIAL_BUILD
    else:
        status = HIDDEN_DEPENDENCY_STATUS_PASS
        blocking_reason_code = ""
    return {
        "status": status,
        "blocking_reason_code": blocking_reason_code,
        "summary": ";".join(summary_parts),
        "declared_inventory_artifacts": sorted(declared),
        "observed_dependency_artifacts": sorted(observed),
        "undeclared_dependency_artifacts": undeclared,
        "failing_producers": failing_producers,
    }
