from __future__ import annotations

from pathlib import Path
from typing import Any, Dict, Optional

from constellation_2.phaseL.ui_api.common import SLEEVE_TRUTH_ROOT, read_json_dict, utc_now_iso


def aegis_operator_state_artifact_path(truth_root: Optional[Path] = None) -> Path:
    root = Path(truth_root or SLEEVE_TRUTH_ROOT).resolve()
    return root / "control_plane" / "aegis_operator_state.v1.json"


def get_operator_state(truth_root: Optional[Path] = None) -> Dict[str, Any]:
    path = aegis_operator_state_artifact_path(truth_root)
    payload, error = read_json_dict(path)
    if error is not None or payload is None:
        return {
            "ok": False,
            "errors": [error or "AEGIS_OPERATOR_STATE_UNAVAILABLE"],
            "generated_utc": utc_now_iso(),
            "artifact_path": str(path),
            "data": None,
        }
    return {
        "ok": True,
        "errors": [],
        "generated_utc": utc_now_iso(),
        "artifact_path": str(path),
        "data": payload,
    }


def build_aegis_operator_state_view(truth_root: Optional[Path] = None) -> Dict[str, Any]:
    return get_operator_state(truth_root)
