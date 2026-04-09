from __future__ import annotations

import hashlib
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from constellation_2.phaseD.lib.canon_json_v1 import canonical_json_bytes_v1
from constellation_2.phaseD.lib.validate_against_schema_v1 import validate_against_repo_schema_v1

from constellation_2.common.paper_session_path_alignment_v1 import (
    resolve_paper_session_evidence_manifest_path,
)


SCHEMA_RELPATH_V1 = "governance/04_DATA/SCHEMAS/C2/REPORTS/paper_session_evidence_manifest.v1.schema.json"
REPO_ROOT = Path(__file__).resolve().parents[2]


def _sorted_codes(codes: list[str] | tuple[str, ...]) -> list[str]:
    return sorted({str(code).strip() for code in codes if str(code).strip()})


@dataclass(frozen=True, slots=True)
class PaperSessionEvidenceManifestV1:
    schema_id: str
    schema_version: str
    authority_scope: str
    day_utc: str
    session_id: str
    manifest_id: str
    evaluated_at_utc: str
    evidence_digest: str
    overall_manifest_status: str
    blocking_codes: tuple[str, ...]
    inputs: tuple[dict[str, Any], ...]

    @classmethod
    def from_dict(cls, obj: dict[str, Any]) -> "PaperSessionEvidenceManifestV1":
        validate_against_repo_schema_v1(obj, REPO_ROOT, SCHEMA_RELPATH_V1)
        return cls(
            schema_id=str(obj["schema_id"]),
            schema_version=str(obj["schema_version"]),
            authority_scope=str(obj["authority_scope"]),
            day_utc=str(obj["day_utc"]),
            session_id=str(obj["session_id"]),
            manifest_id=str(obj["manifest_id"]),
            evaluated_at_utc=str(obj["evaluated_at_utc"]),
            evidence_digest=str(obj["evidence_digest"]),
            overall_manifest_status=str(obj["overall_manifest_status"]),
            blocking_codes=tuple(_sorted_codes(list(obj["blocking_codes"]))),
            inputs=tuple(dict(item) for item in obj["inputs"]),
        )

    def to_dict(self) -> dict[str, Any]:
        obj = {
            "schema_id": self.schema_id,
            "schema_version": self.schema_version,
            "authority_scope": self.authority_scope,
            "day_utc": self.day_utc,
            "session_id": self.session_id,
            "manifest_id": self.manifest_id,
            "evaluated_at_utc": self.evaluated_at_utc,
            "evidence_digest": self.evidence_digest,
            "overall_manifest_status": self.overall_manifest_status,
            "blocking_codes": list(self.blocking_codes),
            "inputs": [dict(item) for item in self.inputs],
        }
        validate_against_repo_schema_v1(obj, REPO_ROOT, SCHEMA_RELPATH_V1)
        return obj


def build_paper_session_evidence_manifest_v1(
    *,
    day_utc: str,
    session_id: str,
    evaluated_at_utc: str,
    evidence_digest: str,
    overall_manifest_status: str,
    blocking_codes: list[str] | tuple[str, ...],
    inputs: list[dict[str, Any]] | tuple[dict[str, Any], ...],
) -> PaperSessionEvidenceManifestV1:
    normalized_inputs = [dict(item) for item in inputs]
    manifest_seed = canonical_json_bytes_v1(
        {
            "day_utc": str(day_utc),
            "session_id": str(session_id),
            "evidence_digest": str(evidence_digest),
            "inputs": normalized_inputs,
        }
    )
    manifest_id = f"paper_session_evidence_manifest:{str(day_utc)}:{hashlib.sha256(manifest_seed).hexdigest()[:16]}"
    return PaperSessionEvidenceManifestV1.from_dict(
        {
            "schema_id": "paper_session_evidence_manifest",
            "schema_version": "v1",
            "authority_scope": "NON_AUTHORITY_EVIDENCE_MANIFEST",
            "day_utc": str(day_utc),
            "session_id": str(session_id),
            "manifest_id": manifest_id,
            "evaluated_at_utc": str(evaluated_at_utc),
            "evidence_digest": str(evidence_digest),
            "overall_manifest_status": str(overall_manifest_status),
            "blocking_codes": _sorted_codes(list(blocking_codes)),
            "inputs": normalized_inputs,
        }
    )


def write_paper_session_evidence_manifest_v1(
    *,
    truth_root: Path,
    manifest: PaperSessionEvidenceManifestV1,
) -> Path:
    path = resolve_paper_session_evidence_manifest_path(truth_root=truth_root.resolve(), day_utc=manifest.day_utc)
    payload = manifest.to_dict()
    raw = canonical_json_bytes_v1(payload) + b"\n"
    path.parent.mkdir(parents=True, exist_ok=True)
    if path.exists():
        existing = PaperSessionEvidenceManifestV1.from_dict(__import__("json").loads(path.read_text(encoding="utf-8")))
        if existing.manifest_id != manifest.manifest_id or existing.evidence_digest != manifest.evidence_digest:
            raise ValueError(f"PAPER_SESSION_EVIDENCE_MANIFEST_IMMUTABLE_MISMATCH:path={path}")
        return path
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_bytes(raw)
    tmp.replace(path)
    return path
