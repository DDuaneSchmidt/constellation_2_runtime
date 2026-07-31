#!/usr/bin/env python3
from __future__ import annotations

import argparse
import hashlib
import json
import os
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Optional

_THIS_FILE = Path(__file__).resolve()
REPO_ROOT = _THIS_FILE.parents[2].resolve()
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from constellation_2.common.runtime_contract_v1 import resolve_canonical_truth_root, resolve_truth_sleeves_root


BOND_OPERATOR_INPUT_ROOT = (REPO_ROOT / "constellation_2/operator_inputs/bond_sleeve").resolve()
BOND_POSITIONS_INPUT_PATH = (BOND_OPERATOR_INPUT_ROOT / "bond_positions_v1.json").resolve()
BOND_POLICY_INPUT_PATH = (BOND_OPERATOR_INPUT_ROOT / "bond_sleeve_policy_v1.json").resolve()


def _utc_now_isoz() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _read_json(path: Path) -> Optional[Dict[str, Any]]:
    try:
        obj = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return None
    return obj if isinstance(obj, dict) else None


def _sha256_file(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def _latest_legacy_decision(day: str) -> tuple[Optional[Path], Optional[Dict[str, Any]]]:
    roots = []
    try:
        roots.append((resolve_truth_sleeves_root().resolve() / "PRIMARY" / "PAPER").resolve())
    except Exception:
        pass
    seen = set()
    candidates: list[tuple[str, Path]] = []
    for root in roots:
        if str(root) in seen:
            continue
        seen.add(str(root))
        family_root = (root / "bond_decision_artifact_v1").resolve()
        if not family_root.exists():
            continue
        for day_dir in family_root.iterdir():
            if not day_dir.is_dir():
                continue
            if len(day_dir.name) != 10:
                continue
            artifact = (day_dir / "bond_decision_artifact.v1.json").resolve()
            if artifact.exists():
                candidates.append((day_dir.name, artifact))
    if not candidates:
        return None, None
    same_day = [item for item in candidates if item[0] == day]
    selected_day, selected_path = sorted(same_day or candidates, key=lambda item: item[0])[-1]
    return selected_path, _read_json(selected_path)


def _atomic_write(path: Path, payload: Dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    data = json.dumps(payload, sort_keys=True, indent=2) + "\n"
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(data, encoding="utf-8")
    os.replace(tmp, path)


def _append_pointer(path: Path, payload: Dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    line = json.dumps(payload, sort_keys=True, separators=(",", ":")) + "\n"
    with path.open("a", encoding="utf-8") as handle:
        handle.write(line)


def _current_operator_holdings_summary() -> Dict[str, Any]:
    positions_doc = _read_json(BOND_POSITIONS_INPUT_PATH) or {}
    policy_doc = _read_json(BOND_POLICY_INPUT_PATH) or {}
    positions = positions_doc.get("positions") if isinstance(positions_doc.get("positions"), list) else []
    watchlist = positions_doc.get("watchlist_positions") if isinstance(positions_doc.get("watchlist_positions"), list) else []
    needs_review = []
    for row in list(positions) + list(watchlist):
        if not isinstance(row, dict):
            continue
        if str(row.get("review_status") or "").strip().upper() == "NEEDS_REVIEW":
            needs_review.append(str(row.get("symbol") or row.get("instrument_id") or "UNKNOWN"))
    return {
        "positions_input_path": str(BOND_POSITIONS_INPUT_PATH),
        "positions_input_sha256": _sha256_file(BOND_POSITIONS_INPUT_PATH) if BOND_POSITIONS_INPUT_PATH.exists() else None,
        "policy_input_path": str(BOND_POLICY_INPUT_PATH),
        "policy_input_sha256": _sha256_file(BOND_POLICY_INPUT_PATH) if BOND_POLICY_INPUT_PATH.exists() else None,
        "core_symbols": [str(row.get("symbol") or row.get("instrument_id")) for row in positions if isinstance(row, dict)],
        "watchlist_symbols": [str(row.get("symbol") or row.get("instrument_id")) for row in watchlist if isinstance(row, dict)],
        "positions_count": len([row for row in positions if isinstance(row, dict)]),
        "watchlist_count": len([row for row in watchlist if isinstance(row, dict)]),
        "needs_review_symbols": sorted(set(needs_review)),
        "review_policy": policy_doc.get("review_policy") if isinstance(policy_doc.get("review_policy"), dict) else {},
    }


def build_manual_recommendation(day: str, produced_utc: str) -> Dict[str, Any]:
    legacy_path, legacy_doc = _latest_legacy_decision(day)
    summary = legacy_doc.get("decision_summary") if isinstance(legacy_doc, dict) and isinstance(legacy_doc.get("decision_summary"), dict) else {}
    allocation = legacy_doc.get("allocation_output") if isinstance(legacy_doc, dict) and isinstance(legacy_doc.get("allocation_output"), dict) else {}
    holdings_summary = _current_operator_holdings_summary()
    recommendation_state = str(summary.get("rebalance_state") or "MANUAL_REVIEW").strip().upper()
    if holdings_summary.get("needs_review_symbols"):
        recommendation_state = "NEEDS_REVIEW"
    return {
        "schema_id": "bond_sleeve_recommendation",
        "schema_version": "v2",
        "day_utc": day,
        "produced_utc": produced_utc,
        "sleeve_id": "BOND",
        "display_name": "Bond Sleeve",
        "execution_mode": "MANUAL",
        "manual_execution_only": True,
        "advisory_only": True,
        "broker_execution_allowed": False,
        "automated_execution_allowed": False,
        "recommendation_state": recommendation_state,
        "action_state": recommendation_state,
        "bond_strategy_label": str(summary.get("duration_posture") or "MANUAL_BOND_REVIEW"),
        "portfolio_role": "FIXED_INCOME_MANUAL_SLEEVE",
        "manual_status": "ADVISORY_ONLY",
        "operator_next_step": "Review the bond recommendation manually; do not route through automated paper orchestration.",
        "source_decision_artifact_path": str(legacy_path) if legacy_path else None,
        "source_decision_artifact_sha256": _sha256_file(legacy_path) if legacy_path else None,
        "legacy_decision_summary": summary,
        "legacy_allocation_output": allocation,
        "current_operator_holdings": holdings_summary,
        "reason_codes": ["BOND_MANUAL_ADVISORY_ONLY"],
    }


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="run_bond_manual_monitor_v1")
    parser.add_argument("--day_utc", required=True)
    parser.add_argument("--truth_root", default="")
    args = parser.parse_args(argv)

    day = str(args.day_utc or "").strip()
    if len(day) != 10 or day[4] != "-" or day[7] != "-":
        raise SystemExit(f"FAIL: bad day_utc expected YYYY-MM-DD got={day!r}")
    truth_root = Path(args.truth_root).resolve() if str(args.truth_root or "").strip() else resolve_canonical_truth_root().resolve()
    produced_utc = _utc_now_isoz()
    family_dir = (truth_root / "reports" / "bond_sleeve_recommendation_v2" / day).resolve()
    artifact_path = (family_dir / "bond_sleeve_recommendation.v2.json").resolve()
    artifact = build_manual_recommendation(day, produced_utc)
    _atomic_write(artifact_path, artifact)

    pointer = {
        "schema_id": "C2_BOND_DISPLAY_HEAD_POINTER_V1",
        "schema_version": "v1",
        "day_utc": day,
        "status": "PASS",
        "produced_utc": produced_utc,
        "points_to": str(artifact_path),
        "points_to_sha256": _sha256_file(artifact_path),
        "producer": {"repo": REPO_ROOT.name, "module": "ops/tools/run_bond_manual_monitor_v1.py"},
    }
    _atomic_write((family_dir / "display_head_pointer.v1.json").resolve(), pointer)
    _append_pointer(
        (family_dir / "canonical_pointer_index.v1.jsonl").resolve(),
        {
            "schema_id": "C2_BOND_MANUAL_POINTER_INDEX_V1",
            "schema_version": "v1",
            "day_utc": day,
            "status": "PASS",
            "produced_utc": produced_utc,
            "points_to": str(artifact_path),
            "points_to_sha256": pointer["points_to_sha256"],
            "producer": pointer["producer"],
        },
    )
    print(json.dumps({"ok": True, "status": "PASS", "artifact_path": str(artifact_path)}, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
