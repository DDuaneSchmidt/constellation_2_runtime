from __future__ import annotations

import json
from dataclasses import replace
from pathlib import Path
from typing import Any

from .artifact_models import Artifact, ArtifactType, DERIVED_TYPES, FORBIDDEN_ARTIFACT_TYPES, LineageRecord, ROOT_TYPES
from .governance import GovernanceError, validate_artifact_allowed, validate_research_os_write

DEFAULT_STORE_ROOT = Path("reports/atlas_v2_research_os")


class ArtifactStoreError(ValueError):
    pass


class ArtifactStore:
    def __init__(self, root: str | Path = DEFAULT_STORE_ROOT) -> None:
        self.root = Path(root)
        self.artifact_dir = self.root / "artifacts"
        self.index_path = self.root / "artifact_index.json"
        self.artifact_dir.mkdir(parents=True, exist_ok=True)
        self._write_index_if_missing()

    def create_artifact(
        self,
        *,
        artifact_id: str,
        artifact_type: str,
        created_at: str,
        created_by: str,
        source_artifact_ids: list[str] | None = None,
        supersedes_artifact_ids: list[str] | None = None,
        confidence: float = 0.0,
        evidence_level: str = "GENERATED_ONLY",
        lifecycle_state: str = "NEW",
        labels: list[str] | None = None,
        metadata: dict[str, Any] | None = None,
        is_root: bool = False,
    ) -> Artifact:
        source_ids = list(source_artifact_ids or [])
        supersedes_ids = list(supersedes_artifact_ids or [])
        artifact = Artifact(
            artifact_id=artifact_id,
            artifact_type=artifact_type,
            created_at=created_at,
            created_by=created_by,
            source_artifact_ids=source_ids,
            supersedes_artifact_ids=supersedes_ids,
            confidence=float(confidence),
            evidence_level=evidence_level,
            lifecycle_state=lifecycle_state,
            labels=list(labels or []),
            metadata=dict(metadata or {}),
            is_root=bool(is_root),
        )
        validate_research_os_write(artifact.to_dict())
        if self.artifact_path(artifact_id).exists():
            raise ArtifactStoreError(f"artifact already exists: {artifact_id}")
        if artifact_type in FORBIDDEN_ARTIFACT_TYPES:
            raise GovernanceError(f"forbidden Research OS artifact type: {artifact_type}")
        if artifact_type in DERIVED_TYPES and not source_ids:
            raise ArtifactStoreError("derived artifact requires at least one source_artifact_id")
        if artifact_type in ROOT_TYPES and not is_root and not source_ids:
            raise ArtifactStoreError("root artifact must be explicitly marked is_root=true")
        missing_sources = [item for item in source_ids + supersedes_ids if not self.exists(item)]
        if missing_sources:
            raise ArtifactStoreError(f"source or superseded artifact missing: {missing_sources}")
        validate_artifact_allowed(artifact.to_dict())
        self._write_json(self.artifact_path(artifact_id), artifact.to_dict())
        self._append_index(artifact)
        if source_ids or supersedes_ids:
            lineage = LineageRecord(
                lineage_id=f"lineage-{artifact_id}",
                artifact_id=artifact_id,
                parent_artifact_ids=source_ids,
                supersedes_artifact_ids=supersedes_ids,
                created_at=created_at,
                created_by=created_by,
            )
            self._write_json(self.artifact_dir / f"{lineage.lineage_id}.json", {"record_type": ArtifactType.LINEAGE_RECORD.value, **lineage.to_dict()})
        return artifact

    def artifact_path(self, artifact_id: str) -> Path:
        return self.artifact_dir / f"{artifact_id}.json"

    def exists(self, artifact_id: str) -> bool:
        return self.artifact_path(artifact_id).exists()

    def get_artifact(self, artifact_id: str) -> dict[str, Any]:
        path = self.artifact_path(artifact_id)
        if not path.exists():
            raise ArtifactStoreError(f"artifact not found: {artifact_id}")
        return json.loads(path.read_text(encoding="utf-8"))

    def list_artifacts(self, artifact_type: str | None = None) -> list[dict[str, Any]]:
        artifacts = []
        for row in self._read_index().get("artifacts", []):
            artifact = self.get_artifact(row["artifact_id"])
            if artifact_type is None or artifact["artifact_type"] == artifact_type:
                artifacts.append(artifact)
        return artifacts

    def write_updated_artifact_copy(self, artifact_id: str, *, new_artifact_id: str, created_at: str, created_by: str, metadata_updates: dict[str, Any] | None = None) -> Artifact:
        original = self.get_artifact(artifact_id)
        metadata = dict(original.get("metadata", {}))
        metadata.update(metadata_updates or {})
        return self.create_artifact(
            artifact_id=new_artifact_id,
            artifact_type=original["artifact_type"],
            created_at=created_at,
            created_by=created_by,
            source_artifact_ids=original.get("source_artifact_ids", []),
            supersedes_artifact_ids=[artifact_id],
            confidence=float(original.get("confidence", 0.0)),
            evidence_level=original.get("evidence_level", "GENERATED_ONLY"),
            lifecycle_state=original.get("lifecycle_state", "NEW"),
            labels=original.get("labels", []),
            metadata=metadata,
            is_root=bool(original.get("is_root", False)),
        )

    def _write_index_if_missing(self) -> None:
        if not self.index_path.exists():
            self._write_json(self.index_path, {"artifacts": []})

    def _read_index(self) -> dict[str, Any]:
        return json.loads(self.index_path.read_text(encoding="utf-8"))

    def _append_index(self, artifact: Artifact) -> None:
        index = self._read_index()
        index.setdefault("artifacts", []).append(
            {
                "artifact_id": artifact.artifact_id,
                "artifact_type": artifact.artifact_type,
                "created_at": artifact.created_at,
                "evidence_level": artifact.evidence_level,
                "lifecycle_state": artifact.lifecycle_state,
                "is_root": artifact.is_root,
            }
        )
        self._write_json(self.index_path, index)

    @staticmethod
    def _write_json(path: Path, payload: dict[str, Any]) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
