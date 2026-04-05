from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from constellation_2.phaseD.lib.validate_against_schema_v1 import validate_against_repo_schema_v1

REPO_ROOT = Path(__file__).resolve().parents[3]
SCHEMA_RELPATH = "governance/04_DATA/SCHEMAS/C2/ADVISOR_BRIDGE/advisor_trade_translation.v1.schema.json"


def _read_json_obj(path: Path) -> dict[str, Any]:
    with path.open("r", encoding="utf-8") as handle:
        obj = json.load(handle)
    if not isinstance(obj, dict):
        raise ValueError(f"TOP_LEVEL_NOT_OBJECT: {path}")
    return obj


@dataclass(frozen=True, slots=True)
class AdvisorTradeTranslationRecordV1:
    decision_action_id: str
    action_type: str
    translation_status: str
    bridge_classification: str
    source_account: str | None
    proposed_amount_cents: int | None
    periodicity: str | None
    rationale_refs: tuple[str, ...]
    evidence_refs: tuple[str, ...]
    notes: tuple[str, ...]

    @classmethod
    def from_dict(cls, obj: dict[str, Any]) -> "AdvisorTradeTranslationRecordV1":
        return cls(
            decision_action_id=str(obj["decision_action_id"]),
            action_type=str(obj["action_type"]),
            translation_status=str(obj["translation_status"]),
            bridge_classification=str(obj["bridge_classification"]),
            source_account=obj["source_account"],
            proposed_amount_cents=obj["proposed_amount_cents"],
            periodicity=obj["periodicity"],
            rationale_refs=tuple(str(item) for item in obj["rationale_refs"]),
            evidence_refs=tuple(str(item) for item in obj["evidence_refs"]),
            notes=tuple(str(item) for item in obj["notes"]),
        )

    def to_dict(self) -> dict[str, Any]:
        return {
            "decision_action_id": self.decision_action_id,
            "action_type": self.action_type,
            "translation_status": self.translation_status,
            "bridge_classification": self.bridge_classification,
            "source_account": self.source_account,
            "proposed_amount_cents": self.proposed_amount_cents,
            "periodicity": self.periodicity,
            "rationale_refs": list(self.rationale_refs),
            "evidence_refs": list(self.evidence_refs),
            "notes": list(self.notes),
        }


@dataclass(frozen=True, slots=True)
class AdvisorTradeTranslationV1:
    schema_id: str
    schema_version: str
    produced_utc: str
    run_id: str
    planning_snapshot_id: str
    decision_plan_id: str
    translations: tuple[AdvisorTradeTranslationRecordV1, ...]

    @classmethod
    def from_dict(cls, obj: dict[str, Any]) -> "AdvisorTradeTranslationV1":
        validate_against_repo_schema_v1(obj, REPO_ROOT, SCHEMA_RELPATH)
        return cls(
            schema_id=str(obj["schema_id"]),
            schema_version=str(obj["schema_version"]),
            produced_utc=str(obj["produced_utc"]),
            run_id=str(obj["run_id"]),
            planning_snapshot_id=str(obj["planning_snapshot_id"]),
            decision_plan_id=str(obj["decision_plan_id"]),
            translations=tuple(AdvisorTradeTranslationRecordV1.from_dict(item) for item in obj["translations"]),
        )

    @classmethod
    def load_file(cls, path: str | Path) -> "AdvisorTradeTranslationV1":
        return cls.from_dict(_read_json_obj(Path(path).expanduser().resolve()))

    def to_dict(self) -> dict[str, Any]:
        obj = {
            "schema_id": self.schema_id,
            "schema_version": self.schema_version,
            "produced_utc": self.produced_utc,
            "run_id": self.run_id,
            "planning_snapshot_id": self.planning_snapshot_id,
            "decision_plan_id": self.decision_plan_id,
            "translations": [item.to_dict() for item in self.translations],
        }
        validate_against_repo_schema_v1(obj, REPO_ROOT, SCHEMA_RELPATH)
        return obj
