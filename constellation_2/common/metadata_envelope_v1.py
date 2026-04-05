from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from constellation_2.phaseD.lib.canon_json_v1 import canonical_sha256_hex_v1


@dataclass(frozen=True, slots=True)
class MetadataEnvelopeV1:
    produced_utc: str
    day_utc: str
    mode: str
    source_artifact_refs: tuple[str, ...]
    artifact_family: str
    selection_basis: str | None = None

    def to_run_payload(self) -> dict[str, Any]:
        return {
            'artifact_family': self.artifact_family,
            'day_utc': self.day_utc,
            'mode': self.mode,
            'source_artifact_refs': sorted(self.source_artifact_refs),
            'selection_basis': self.selection_basis,
        }

    def run_id(self) -> str:
        return canonical_sha256_hex_v1(self.to_run_payload())


def metadata_envelope_v1(*, produced_utc: str, day_utc: str, mode: str, source_artifact_refs: list[str] | tuple[str, ...], artifact_family: str, selection_basis: str | None = None) -> MetadataEnvelopeV1:
    refs = tuple(str(item) for item in source_artifact_refs)
    if not produced_utc:
        raise ValueError('PRODUCED_UTC_REQUIRED')
    if not day_utc:
        raise ValueError('DAY_UTC_REQUIRED')
    if not mode:
        raise ValueError('MODE_REQUIRED')
    if not artifact_family:
        raise ValueError('ARTIFACT_FAMILY_REQUIRED')
    if any(not item for item in refs):
        raise ValueError('SOURCE_ARTIFACT_REF_EMPTY')
    return MetadataEnvelopeV1(produced_utc=str(produced_utc), day_utc=str(day_utc), mode=str(mode), source_artifact_refs=refs, artifact_family=str(artifact_family), selection_basis=None if selection_basis is None else str(selection_basis))


def artifact_base_v1(*, schema_id: str, envelope: MetadataEnvelopeV1) -> dict[str, Any]:
    if not schema_id:
        raise ValueError('SCHEMA_ID_REQUIRED')
    return {
        'schema_id': schema_id,
        'schema_version': 'v1',
        'produced_utc': envelope.produced_utc,
        'run_id': envelope.run_id(),
    }
