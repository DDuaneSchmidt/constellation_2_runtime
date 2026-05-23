from __future__ import annotations

import json
import sys
from pathlib import Path

SOURCE_ROOT = Path('/home/node/constellation')
if str(SOURCE_ROOT) not in sys.path:
    sys.path.insert(0, str(SOURCE_ROOT))

import pytest

from ops.aegis.universe.canonical_universe_authority_v1 import (
    CanonicalUniverseAuthorityError,
    blocked_authority_write_path,
    canonical_universe_health_v1,
    canonical_universe_authority_path,
    emit_canonical_universe_authority_v1,
    last_known_good_canonical_universe_path,
    load_last_known_good_canonical_universe_v1,
    require_current_canonical_universe_authority_v1,
)
from constellation_2.common.ranked_symbol_universe_v1 import build_ranked_symbol_universe_payload

DAY = "2026-05-20"


def _symbols(count: int, prefix: str = "SYM") -> list[str]:
    return [f"{prefix}{idx:03d}" for idx in range(count)]


def _emit(truth: Path, day: str, count: int, *, run_id: str = "run", writer: str = "ranked_symbol_universe_v1") -> dict:
    return emit_canonical_universe_authority_v1(
        truth_root=truth,
        source_day=day,
        source_run_id=run_id,
        universe_symbols=_symbols(count),
        writer_process=writer,
        source_data_artifacts=[],
        generation_pipeline=writer,
        discovery_targets={"target_symbol_count": 200},
        discovery_results={"test_symbol_count": count},
    )


def test_sleeve_local_manifest_cannot_overwrite_canonical_authority(tmp_path: Path) -> None:
    truth = tmp_path / "truth"
    _emit(truth, DAY, 434, run_id="broad")

    with pytest.raises(CanonicalUniverseAuthorityError, match="PROHIBITED_CANONICAL_UNIVERSE_WRITER"):
        _emit(truth, DAY, 10, run_id="sleeve-local", writer="sleeve_local_manifest")

    assert json.loads(canonical_universe_authority_path(truth_root=truth, day_utc=DAY).read_text())["universe_symbol_count"] == 434
    assert blocked_authority_write_path(truth_root=truth, day_utc=DAY, source_run_id="sleeve-local").exists()


def test_curated_manifest_rejected_as_canonical(tmp_path: Path) -> None:
    truth = tmp_path / "truth"
    with pytest.raises(CanonicalUniverseAuthorityError, match="NON_CANONICAL_UNIVERSE_TYPE_REJECTED"):
        emit_canonical_universe_authority_v1(
            truth_root=truth,
            source_day=DAY,
            source_run_id="curated",
            universe_symbols=["SPY", "QQQ"],
            writer_process="ranked_symbol_universe_v1",
            universe_type="CURATED",
            source_data_artifacts=[],
            generation_pipeline="ranked_symbol_universe_v1",
            discovery_targets={"target_symbol_count": 200},
            discovery_results={},
        )


def test_universe_collapse_over_20_percent_blocked_and_lkg_preserved(tmp_path: Path) -> None:
    truth = tmp_path / "truth"
    broad = _emit(truth, DAY, 434, run_id="broad")

    with pytest.raises(CanonicalUniverseAuthorityError, match="UNIVERSE_COLLAPSE_PROTECTED"):
        _emit(truth, "2026-05-21", 10, run_id="collapsed")

    lkg = load_last_known_good_canonical_universe_v1(truth_root=truth)
    assert lkg["canonical_universe_authority_id"] == broad["canonical_universe_authority_id"]
    assert lkg["universe_symbol_count"] == 434
    assert last_known_good_canonical_universe_path(truth_root=truth).exists()


def test_downstream_processes_are_read_only(tmp_path: Path) -> None:
    truth = tmp_path / "truth"
    _emit(truth, DAY, 200, run_id="broad")

    with pytest.raises(CanonicalUniverseAuthorityError, match="PROHIBITED_CANONICAL_UNIVERSE_WRITER"):
        _emit(truth, DAY, 200, run_id="allocation", writer="allocation_v1")



def test_lineage_attached_to_ranked_derived_universe(tmp_path: Path) -> None:
    truth = tmp_path / "truth"
    authority = _emit(truth, DAY, 200, run_id="broad")
    manifest = truth / "market_data_snapshot_v1" / "dataset_manifest.json"
    manifest.parent.mkdir(parents=True, exist_ok=True)
    manifest.write_text(json.dumps({"dataset_version": "v1", "symbols": _symbols(200), "files": []}), encoding="utf-8")

    payload = build_ranked_symbol_universe_payload(
        day_utc=DAY,
        produced_utc="2026-05-20T12:00:00Z",
        truth_root=truth,
        canonical_universe_authority=authority,
    )

    assert payload["universe_type"] == "RANKED_DYNAMIC"
    assert payload["parent_canonical_universe_authority_id"] == authority["canonical_universe_authority_id"]
    assert payload["parent_universe_type"] == "CANONICAL_DYNAMIC"


def test_dynamic_sleeves_refuse_curated_authority(tmp_path: Path) -> None:
    truth = tmp_path / "truth"
    with pytest.raises(CanonicalUniverseAuthorityError):
        emit_canonical_universe_authority_v1(
            truth_root=truth,
            source_day=DAY,
            source_run_id="curated",
            universe_symbols=["SPY", "QQQ"],
            writer_process="ranked_symbol_universe_v1",
            universe_type="CURATED",
            source_data_artifacts=[],
            generation_pipeline="ranked_symbol_universe_v1",
            discovery_targets={"target_symbol_count": 200},
            discovery_results={},
        )


def test_434_symbol_canonical_universe_preserved_across_downstream_runs(tmp_path: Path) -> None:
    truth = tmp_path / "truth"
    _emit(truth, DAY, 434, run_id="broad")

    with pytest.raises(CanonicalUniverseAuthorityError):
        _emit(truth, DAY, 10, run_id="operator-state", writer="operator_state_rebuild")

    current = json.loads(canonical_universe_authority_path(truth_root=truth, day_utc=DAY).read_text())
    assert current["universe_symbol_count"] == 434


def test_stale_but_valid_universe_requires_degraded_state(tmp_path: Path) -> None:
    truth = tmp_path / "truth"
    _emit(truth, DAY, 200, run_id="broad")

    health = canonical_universe_health_v1(truth_root=truth, day_utc="2026-05-21")
    assert health["status"] == "DEGRADED"
    assert health["last_known_good_count"] == 200
    with pytest.raises(CanonicalUniverseAuthorityError, match="STALE_DEGRADED"):
        require_current_canonical_universe_authority_v1(truth_root=truth, day_utc="2026-05-21", minimum_symbol_count=160)
    stale = require_current_canonical_universe_authority_v1(truth_root=truth, day_utc="2026-05-21", minimum_symbol_count=160, allow_stale_degraded=True)
    assert stale["universe_symbol_count"] == 200


def test_explicit_override_required_for_narrowing(tmp_path: Path) -> None:
    truth = tmp_path / "truth"
    _emit(truth, DAY, 300, run_id="broad")
    override = tmp_path / "override.json"
    override.write_text(json.dumps({"allow_universe_narrowing_override": True, "override_scope": "CANONICAL_DYNAMIC_UNIVERSE_NARROWING"}), encoding="utf-8")

    narrowed = emit_canonical_universe_authority_v1(
        truth_root=truth,
        source_day="2026-05-21",
        source_run_id="approved-narrow",
        universe_symbols=_symbols(200),
        writer_process="ranked_symbol_universe_v1",
        source_data_artifacts=[],
        generation_pipeline="ranked_symbol_universe_v1",
        discovery_targets={"target_symbol_count": 200},
        discovery_results={},
        override_artifact_path=override,
    )
    assert narrowed["authority_status"] == "PASS"
    assert narrowed["universe_symbol_count"] == 200
