from __future__ import annotations

import json
from pathlib import Path

from constellation_2.phaseL.ui.server.run_ops_dashboard_v1 import _release_identity_payload_v1


SOURCE_COMMIT = "451bff29e36b8cd4cf28519ead6ac710ec5c3e53"


def test_release_identity_returns_only_governed_identity(monkeypatch, tmp_path: Path) -> None:
    manifest = tmp_path / "release-manifest.json"
    manifest.write_text(
        json.dumps(
            {
                "release_id": "FCC-AEGIS-LITE-451bff29e36b",
                "source_commit": SOURCE_COMMIT,
                "artifact_sha256": "sha256:artifact",
                "image_digest": "sha256:image",
                "build_id": "build-1",
                "secret": "must-not-be-served",
            }
        ),
        encoding="utf-8",
    )
    monkeypatch.setenv("AEGIS_RELEASE_MANIFEST", str(manifest))

    payload = _release_identity_payload_v1()

    assert payload == {
        "schema_version": "aegis-release-identity.v1",
        "status": "READY",
        "release_id": "FCC-AEGIS-LITE-451bff29e36b",
        "source_commit": SOURCE_COMMIT,
        "artifact_sha256": "sha256:artifact",
        "image_digest": "sha256:image",
        "build_id": "build-1",
    }


def test_release_identity_fails_closed_without_manifest(monkeypatch, tmp_path: Path) -> None:
    monkeypatch.setenv("AEGIS_RELEASE_MANIFEST", str(tmp_path / "missing.json"))

    payload = _release_identity_payload_v1()

    assert payload["status"] == "BLOCKED"
    assert payload["reason"] == "FILE_NOT_FOUND"


def test_release_identity_rejects_noncanonical_commit(monkeypatch, tmp_path: Path) -> None:
    manifest = tmp_path / "release-manifest.json"
    manifest.write_text(
        json.dumps(
            {
                "release_id": "release-1",
                "source_commit": "not-a-commit",
                "artifact_sha256": "sha256:artifact",
                "image_digest": "sha256:image",
            }
        ),
        encoding="utf-8",
    )
    monkeypatch.setenv("AEGIS_RELEASE_MANIFEST", str(manifest))

    payload = _release_identity_payload_v1()

    assert payload["status"] == "BLOCKED"
    assert "source_commit" in payload["invalid_or_missing_fields"]
