from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any


class EngineUniverseError(RuntimeError):
    """Fail-closed error for candidate-basis resolution."""


@dataclass(frozen=True)
class UniverseCandidateBasis:
    engine_id: str
    policy_id: str
    day_utc: str
    basis_day_utc: str
    basis_mode: str
    configured_rule: dict[str, Any]
    symbols_considered: list[str]
    candidate_symbols: list[str]
    exclusions_by_reason: list[dict[str, Any]]
    notes: list[str]


def _read_json(path: Path) -> dict[str, Any]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise EngineUniverseError(f"CANDIDATE_BASIS_INVALID_JSON:{path}")
    return payload


def _basis_path(*, truth_root: Path, day_utc: str, engine_id: str) -> Path:
    return (
        Path(truth_root).resolve()
        / "reports"
        / "engine_universe_candidate_basis_v1"
        / str(day_utc).strip()
        / str(engine_id).strip().upper()
        / "engine_universe_candidate_basis.v1.json"
    ).resolve()


def resolve_engine_candidate_basis(*, engine_id: str, day_utc: str, truth_root: Path) -> UniverseCandidateBasis:
    normalized_engine_id = str(engine_id).strip().upper()
    normalized_day = str(day_utc).strip()
    path = _basis_path(truth_root=Path(truth_root), day_utc=normalized_day, engine_id=normalized_engine_id)
    if not path.exists() or not path.is_file():
        raise EngineUniverseError(
            f"CANDIDATE_BASIS_SAME_DAY_REQUIRED:{normalized_engine_id}:{normalized_day}"
        )

    payload = _read_json(path)
    basis_day_utc = str(payload.get("basis_day_utc") or payload.get("day_utc") or "").strip()
    if basis_day_utc != normalized_day:
        raise EngineUniverseError(
            f"CANDIDATE_BASIS_SAME_DAY_REQUIRED:{normalized_engine_id}:{normalized_day}"
        )

    candidate_symbols = [
        str(symbol).strip().upper()
        for symbol in (payload.get("candidate_symbols") or [])
        if str(symbol).strip()
    ]

    return UniverseCandidateBasis(
        engine_id=normalized_engine_id,
        policy_id=str(payload.get("policy_id") or normalized_engine_id).strip().upper(),
        day_utc=normalized_day,
        basis_day_utc=basis_day_utc,
        basis_mode=str(payload.get("basis_mode") or "SAME_DAY_ELIGIBLE_SYMBOLS").strip() or "SAME_DAY_ELIGIBLE_SYMBOLS",
        configured_rule=payload.get("configured_rule") if isinstance(payload.get("configured_rule"), dict) else {},
        symbols_considered=[
            str(symbol).strip().upper()
            for symbol in (payload.get("symbols_considered") or [])
            if str(symbol).strip()
        ],
        candidate_symbols=sorted(set(candidate_symbols)),
        exclusions_by_reason=[
            row for row in (payload.get("exclusions_by_reason") or []) if isinstance(row, dict)
        ],
        notes=[str(note).strip() for note in (payload.get("notes") or []) if str(note).strip()],
    )
