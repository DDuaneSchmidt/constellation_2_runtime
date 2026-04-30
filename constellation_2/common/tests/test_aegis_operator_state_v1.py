from __future__ import annotations

import ast
import hashlib
import json
import sys
from datetime import UTC, datetime, timedelta
from pathlib import Path

SOURCE_ROOT = Path("/home/node/constellation")
if str(SOURCE_ROOT) not in sys.path:
    sys.path.insert(0, str(SOURCE_ROOT))

from ops.tools.build_aegis_operator_state_v1 import (
    STATE_ATTENTION,
    STATE_ERROR,
    STATE_SELECTED,
    STATE_STALE,
    STATE_WAITING,
    aegis_operator_state_path,
    build_aegis_operator_alert_state_v1,
    build_aegis_operator_state_v1,
)
from constellation_2.phaseL.ui_api.aegis_operator_state_read_model import get_operator_state


NOW = datetime(2026, 4, 30, 15, 0, 0, tzinfo=UTC)


def _write_json(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, sort_keys=True) + "\n", encoding="utf-8")


def _write_jsonl(path: Path, rows: list[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("".join(json.dumps(row, sort_keys=True) + "\n" for row in rows), encoding="utf-8")


def _source_hashes(paths: list[Path]) -> dict[str, str]:
    return {str(path): hashlib.sha256(path.read_bytes()).hexdigest() for path in paths}


def _fixture(
    truth: Path,
    *,
    cycle_id: str = "cycle_a",
    selected_cycle_id: str | None = None,
    selected_intent_id: str = "",
    ready: int = 5,
    blocked: int = 0,
    unknown: int = 0,
    disabled: int = 2,
    submit_enabled: bool = False,
    execution_path_touched: bool = False,
    completed_at: datetime | None = None,
    ledger_cycle_id: str | None = None,
) -> dict[str, Path]:
    day = "2026-04-30"
    completed = completed_at or (NOW - timedelta(seconds=30))
    artifact_root = truth / "reports" / "sleeve_scan_session_v1" / day / cycle_id
    operator_path = artifact_root / "operator_status.v1.json"
    readiness_path = artifact_root / "operator_readiness_summary.v1.json"
    arbitration_path = artifact_root / "arbitration_result.v1.json"
    ledger_path = truth / "reports" / "scan_ledger_v1" / f"{day}.scan_ledger.v1.jsonl"
    latest_pointer_path = truth / "pointers" / "latest_scan_cycle_pointer.v1.json"
    selected_pointer_path = truth / "pointers" / "selected_intent_pointer.v1.json"
    selected_cycle = selected_cycle_id if selected_cycle_id is not None else cycle_id
    selected_intent = {"intent_id": selected_intent_id, "cycle_id": selected_cycle} if selected_intent_id else {}
    ready_sleeves = [f"READY_{idx}" for idx in range(ready)]
    blocked_sleeves = [f"BLOCKED_{idx}" for idx in range(blocked)]
    unknown_sleeves = [f"UNKNOWN_{idx}" for idx in range(unknown)]
    disabled_sleeves = [f"DISABLED_{idx}" for idx in range(disabled)]

    _write_json(
        latest_pointer_path,
        {
            "schema_id": "latest_scan_cycle_pointer",
            "schema_version": "v1",
            "cycle_id": cycle_id,
            "day_utc": day,
            "environment": "PAPER",
            "status": "PASS",
            "canonical_blocker": "",
            "artifact_root": str(artifact_root),
            "operator_status_path": str(operator_path),
            "updated_at_utc": completed.isoformat().replace("+00:00", "Z"),
        },
    )
    _write_json(
        selected_pointer_path,
        {
            "schema_id": "selected_intent_pointer",
            "schema_version": "v1",
            "cycle_id": selected_cycle,
            "day_utc": day,
            "environment": "PAPER",
            "status": "SELECTED" if selected_intent_id else "NO_EXECUTABLE_INTENT",
            "canonical_blocker": "" if selected_intent_id else "NO_EXECUTABLE_INTENT",
            "selected_intent": selected_intent,
            "source_arbitration_path": str(arbitration_path),
            "source_rollup_path": str(artifact_root / "scan_rollup.v1.json"),
            "created_at_utc": completed.isoformat().replace("+00:00", "Z"),
        },
    )
    _write_json(
        operator_path,
        {
            "schema_id": "operator_status",
            "schema_version": "v1",
            "cycle_id": cycle_id,
            "day_utc": day,
            "environment": "PAPER",
            "status": "PASS",
            "selected_intent_id": selected_intent_id or None,
            "selected_intent_present": bool(selected_intent_id),
            "canonical_blocker": "",
            "submit_enabled": submit_enabled,
            "execution_path_touched": execution_path_touched,
            "operator_readiness_summary_path": str(readiness_path),
            "completed_at_utc": completed.isoformat().replace("+00:00", "Z"),
        },
    )
    top_blockers = [{"blocker": "missing_input", "count": blocked, "sleeves": blocked_sleeves, "example_path": ""}] if blocked else []
    _write_json(
        readiness_path,
        {
            "schema_id": "operator_readiness_summary",
            "schema_version": "v1",
            "cycle_id": cycle_id,
            "day_utc": day,
            "environment": "PAPER",
            "ready_count": ready,
            "blocked_count": blocked,
            "unknown_count": unknown,
            "disabled_count": disabled,
            "top_blockers": top_blockers,
            "sleeves_by_status": {
                "READY": ready_sleeves,
                "BLOCKED": blocked_sleeves,
                "UNKNOWN": unknown_sleeves,
                "DISABLED": disabled_sleeves,
            },
        },
    )
    _write_json(
        arbitration_path,
        {
            "schema_id": "intent_arbitration",
            "schema_version": "v1",
            "cycle_id": cycle_id,
            "day_utc": day,
            "environment": "PAPER",
            "status": "SELECTED" if selected_intent_id else "NO_EXECUTABLE_INTENT",
            "canonical_blocker": "",
            "selected_intent": selected_intent,
        },
    )
    _write_jsonl(
        ledger_path,
        [
            {
                "cycle_id": ledger_cycle_id or cycle_id,
                "day_utc": day,
                "environment": "PAPER",
                "status": "PASS",
                "operator_status_path": str(operator_path),
                "completed_at_utc": completed.isoformat().replace("+00:00", "Z"),
                "submit_enabled": False,
            }
        ],
    )
    return {
        "latest": latest_pointer_path,
        "selected": selected_pointer_path,
        "operator": operator_path,
        "readiness": readiness_path,
        "arbitration": arbitration_path,
        "ledger": ledger_path,
    }


def _state(truth: Path, **kwargs) -> dict:
    _fixture(truth, **kwargs)
    return build_aegis_operator_state_v1(truth_root=truth, now_utc=NOW, max_age_seconds=300)


def test_waiting_for_intent_healthy_baseline(tmp_path: Path) -> None:
    payload = _state(tmp_path / "truth", ready=5, blocked=0, unknown=0, disabled=2)
    assert payload["state"] == STATE_WAITING
    assert payload["readiness_counts"] == {"ready": 5, "blocked": 0, "unknown": 0, "disabled": 2}


def test_selected_intent_requires_review(tmp_path: Path) -> None:
    payload = _state(tmp_path / "truth", selected_intent_id="intent_123")
    assert payload["state"] == STATE_SELECTED
    assert payload["selected_intent_id"] == "intent_123"


def test_attention_required_on_blocked_count(tmp_path: Path) -> None:
    payload = _state(tmp_path / "truth", ready=5, blocked=1)
    assert payload["state"] == STATE_ATTENTION
    assert "blocked_count > 0" in payload["state_reason"]


def test_attention_required_on_unknown_count(tmp_path: Path) -> None:
    payload = _state(tmp_path / "truth", ready=5, unknown=1)
    assert payload["state"] == STATE_ATTENTION
    assert "unknown_count > 0" in payload["state_reason"]


def test_attention_required_on_submit_enabled(tmp_path: Path) -> None:
    payload = _state(tmp_path / "truth", submit_enabled=True)
    assert payload["state"] == STATE_ATTENTION
    assert "submit_enabled != false" in payload["state_reason"]


def test_attention_required_on_execution_path_touched(tmp_path: Path) -> None:
    payload = _state(tmp_path / "truth", execution_path_touched=True)
    assert payload["state"] == STATE_ATTENTION
    assert "execution_path_touched != false" in payload["state_reason"]


def test_attention_required_on_selected_latest_pointer_mismatch(tmp_path: Path) -> None:
    payload = _state(tmp_path / "truth", selected_cycle_id="cycle_previous")
    assert payload["state"] == STATE_ATTENTION
    assert "selected/latest pointer mismatch" in payload["state_reason"]


def test_stale_when_latest_cycle_age_exceeds_threshold(tmp_path: Path) -> None:
    payload = _state(tmp_path / "truth", completed_at=NOW - timedelta(seconds=301))
    assert payload["state"] == STATE_STALE
    assert "latest cycle age exceeds max_age_seconds" in payload["state_reason"]


def test_error_on_missing_required_artifacts(tmp_path: Path) -> None:
    payload = build_aegis_operator_state_v1(truth_root=tmp_path / "truth", now_utc=NOW)
    assert payload["state"] == STATE_ERROR
    assert "MISSING_FILE" in payload["state_reason"]


def test_error_on_malformed_required_artifacts(tmp_path: Path) -> None:
    truth = tmp_path / "truth"
    _fixture(truth)
    (truth / "pointers" / "selected_intent_pointer.v1.json").write_text("{not-json", encoding="utf-8")
    payload = build_aegis_operator_state_v1(truth_root=truth, now_utc=NOW)
    assert payload["state"] == STATE_ERROR
    assert "MALFORMED_JSON" in payload["state_reason"]


def test_duplicate_alert_suppression() -> None:
    first = build_aegis_operator_alert_state_v1(state=STATE_ATTENTION, latest_cycle_id="cycle_a", selected_intent_id="")
    duplicate = build_aegis_operator_alert_state_v1(
        state=STATE_ATTENTION,
        latest_cycle_id="cycle_a",
        selected_intent_id="",
        previous_alert_key=first["alert_key"],
    )
    waiting = build_aegis_operator_alert_state_v1(state=STATE_WAITING, latest_cycle_id="cycle_a", selected_intent_id="")
    assert first["should_alert"] is True
    assert duplicate["should_alert"] is False
    assert duplicate["suppressed_duplicate"] is True
    assert waiting["should_alert"] is False


def test_derived_state_does_not_mutate_scan_artifacts(tmp_path: Path) -> None:
    truth = tmp_path / "truth"
    paths = list(_fixture(truth).values())
    before = _source_hashes(paths)
    payload = build_aegis_operator_state_v1(truth_root=truth, now_utc=NOW)
    after = _source_hashes(paths)
    assert before == after
    assert Path(aegis_operator_state_path(truth_root=truth)).is_file()
    assert payload["state"] == STATE_WAITING


def test_builder_has_no_execution_boundary_imports() -> None:
    checked = [
        Path("ops/tools/build_aegis_operator_state_v1.py"),
        Path("constellation_2/phaseL/ui_api/aegis_operator_state_read_model.py"),
    ]
    def banned_import(name: str) -> bool:
        components = [part for chunk in name.split(".") for part in chunk.split("_")]
        if "ib" in components:
            return True
        return any(token in name for token in ("submit", "broker", "execution", "order"))

    for path in checked:
        tree = ast.parse(path.read_text(encoding="utf-8"))
        imports: list[str] = []
        for node in ast.walk(tree):
            if isinstance(node, ast.Import):
                imports.extend(alias.name.lower() for alias in node.names)
            if isinstance(node, ast.ImportFrom):
                imports.append(str(node.module or "").lower())
                imports.extend(alias.name.lower() for alias in node.names)
        assert not [name for name in imports if banned_import(name)]


def test_aegis_runtime_ui_wiring_is_read_only() -> None:
    pages = (SOURCE_ROOT / "constellation_2/phaseL/ui/static/operator_shell/pages/index.js").read_text(encoding="utf-8")
    domain_client = (SOURCE_ROOT / "constellation_2/phaseL/ui/static/operator_shell/domain_client/index.js").read_text(encoding="utf-8")
    navigation = (SOURCE_ROOT / "constellation_2/phaseL/ui/static/operator_shell/navigation_schema.js").read_text(encoding="utf-8")
    server = (SOURCE_ROOT / "constellation_2/phaseL/ui/server/run_ops_dashboard_v1.py").read_text(encoding="utf-8")
    assert 'const path = "/api/aegis/operator-state"' in domain_client
    assert "window.location.origin" in domain_client
    assert 'path: "/aegis-runtime"' in pages
    assert 'case "aegis_runtime"' in pages
    assert "aegis_runtime_last_alert_key_v1" in pages
    assert "Notification" in pages
    assert "webhook_enabled" in pages
    assert "payload.data || payload.operator_state" in pages
    assert 'label: "Aegis Runtime"' in navigation
    assert 'path == "/api/aegis/operator-state"' in server
    assert "get_operator_state(GLOBAL_TRUTH_ROOT)" in server
    assert '"/aegis-runtime"' in server
    assert "get_operator_state" in server
    new_sections = "\n".join([pages, domain_client, navigation, server])
    assert "postJson(\"/api/aegis" not in new_sections
    assert "patchJson(\"/api/aegis" not in new_sections


def test_aegis_operator_state_api_contract_returns_ok_data(tmp_path: Path) -> None:
    truth = tmp_path / "truth"
    expected = build_aegis_operator_state_v1(truth_root=truth, now_utc=NOW, write=True)
    payload = get_operator_state(truth)
    assert payload["ok"] is True
    assert payload["errors"] == []
    assert payload["data"]["state"] == expected["state"]
    assert payload["data"]["latest_cycle_id"] == expected["latest_cycle_id"]
    assert "operator_state" not in payload
