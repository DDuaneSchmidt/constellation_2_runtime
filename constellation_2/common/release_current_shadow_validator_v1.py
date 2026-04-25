from __future__ import annotations

import json
import logging
from pathlib import Path
from typing import Any, Mapping

from constellation_2.common.paper_session_fact_plane_v1 import SurfaceRefV1, read_validated_surface_v1


LOGGER = logging.getLogger(__name__)
RELEASE_CURRENT_SCHEMA_RELPATH_V1 = "governance/04_DATA/SCHEMAS/C2/RUNTIME/release_current.v1.schema.json"


def _require_text(value: Any, code: str) -> str:
    text = str(value or "").strip()
    if not text:
        raise ValueError(code)
    return text


def _current_path(*, truth_root: Path) -> Path:
    return (Path(truth_root).resolve() / "release_current_v1" / "current.json").resolve()


def load_release_current_shadow_ref_if_present(*, truth_root: Path) -> SurfaceRefV1 | None:
    path = _current_path(truth_root=truth_root)
    if not path.exists():
        return None
    try:
        return read_validated_surface_v1(path=path, schema_relpath=RELEASE_CURRENT_SCHEMA_RELPATH_V1)
    except Exception as exc:
        raise SystemExit(
            f"FAIL: release_current_shadow_invalid path={path} err={type(exc).__name__}:{exc}"
        ) from exc


def compare_release_current_shadow_v1(
    *,
    active_runtime_contract: Mapping[str, Any],
    release_manifest: Mapping[str, Any],
    configuration_state: Mapping[str, Any],
    release_current: Mapping[str, Any],
) -> list[dict[str, str]]:
    mismatches: list[dict[str, str]] = []

    def _check(field: str, old_value: Any, new_value: Any) -> None:
        old_text = str(old_value or "").strip()
        new_text = str(new_value or "").strip()
        if old_text != new_text:
            mismatches.append(
                {
                    "field": field,
                    "old_value": old_text,
                    "new_value": new_text,
                }
            )

    _check("release_id", release_manifest.get("release_id"), release_current.get("release_id"))
    _check("git_sha", str(release_manifest.get("git_sha") or "").lower(), str(release_current.get("git_sha") or "").lower())
    _check(
        "release_root",
        str(Path(_require_text(active_runtime_contract.get("release_root"), "RUNTIME_RELEASE_ROOT_MISSING")).resolve()),
        str(Path(_require_text(release_current.get("release_root"), "RELEASE_CURRENT_RELEASE_ROOT_MISSING")).resolve()),
    )
    _check(
        "truth_root",
        str(Path(_require_text(active_runtime_contract.get("canonical_truth_root"), "RUNTIME_TRUTH_ROOT_MISSING")).resolve()),
        str(Path(_require_text(release_current.get("canonical_truth_root"), "RELEASE_CURRENT_TRUTH_ROOT_MISSING")).resolve()),
    )
    _check(
        "environment",
        _require_text(active_runtime_contract.get("runtime_environment"), "RUNTIME_ENVIRONMENT_MISSING"),
        _require_text(release_current.get("runtime_environment"), "RELEASE_CURRENT_ENVIRONMENT_MISSING"),
    )
    _check(
        "configuration_state_id",
        _require_text(configuration_state.get("configuration_state_id"), "CONFIGURATION_STATE_ID_MISSING"),
        _require_text(release_current.get("configuration_state_id"), "RELEASE_CURRENT_CONFIGURATION_STATE_ID_MISSING"),
    )
    _check(
        "compiled_active_config_path",
        _require_text(
            (configuration_state.get("compiled_active_config_ref") or {}).get("path"),
            "CONFIGURATION_COMPILED_ACTIVE_CONFIG_PATH_MISSING",
        ),
        _require_text(
            (release_current.get("compiled_active_config_ref") or {}).get("path"),
            "RELEASE_CURRENT_COMPILED_ACTIVE_CONFIG_PATH_MISSING",
        ),
    )
    _check(
        "configuration_activation_transaction_path",
        _require_text(
            (configuration_state.get("configuration_activation_transaction_ref") or {}).get("path"),
            "CONFIGURATION_ACTIVATION_TRANSACTION_PATH_MISSING",
        ),
        _require_text(
            (release_current.get("configuration_activation_transaction_ref") or {}).get("path"),
            "RELEASE_CURRENT_ACTIVATION_TRANSACTION_PATH_MISSING",
        ),
    )
    return mismatches


def validate_release_current_shadow_v1(
    *,
    active_runtime_contract: Mapping[str, Any],
    release_manifest: Mapping[str, Any],
    configuration_state: Mapping[str, Any],
) -> list[dict[str, str]]:
    truth_root = Path(
        _require_text(active_runtime_contract.get("canonical_truth_root"), "RUNTIME_TRUTH_ROOT_MISSING")
    ).resolve()
    release_current_ref = load_release_current_shadow_ref_if_present(truth_root=truth_root)
    if release_current_ref is None:
        return []

    mismatches = compare_release_current_shadow_v1(
        active_runtime_contract=active_runtime_contract,
        release_manifest=release_manifest,
        configuration_state=configuration_state,
        release_current=release_current_ref.payload,
    )
    if mismatches:
        LOGGER.warning(
            "release_current_shadow_mismatch %s",
            json.dumps(
                {
                    "shadow_path": str(release_current_ref.path),
                    "release_current_id": str(release_current_ref.payload.get("release_current_id") or ""),
                    "mismatches": mismatches,
                },
                sort_keys=True,
            ),
        )
    return mismatches
