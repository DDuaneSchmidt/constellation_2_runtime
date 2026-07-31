from __future__ import annotations

import json
import sys
from pathlib import Path

REPO = Path(__file__).resolve().parents[3]
if str(REPO) not in sys.path:
    sys.path.insert(0, str(REPO))

from ops.aegis.operator_route_inventory_v1 import build_operator_route_inventory_v1, run_operator_route_inventory_self_check_v1, write_operator_route_inventory_v1

DAY = "2026-05-30"
SURFACES = ["command_center", "engineering", "positions", "history", "performance", "position_review", "sleeve_analytics", "research", "ask_aegis"]


def _write_contract(root: Path) -> None:
    rows = []
    for surface in SURFACES:
        rows.append({
            "surface_id": surface,
            "status": "READY",
            "requested_day": DAY,
            "source_day": DAY,
            "context_day": DAY if surface == "ask_aegis" else "",
            "render_allowed": True,
            "actions_allowed": surface == "command_center",
            "primary_message": f"{surface} primary message",
            "reason": "Surface checks passed for the requested day.",
            "impact": "Primary workflow may render.",
            "next_step": "Continue.",
            "metrics_allowed": surface in {"performance", "sleeve_analytics"},
            "diagnostics_allowed": True,
            "ask_aegis_prompt": "Explain this surface state.",
            "evidence_refs": [],
            "artifact_refs": [],
            "surface_readiness_status": "READY",
            "semantic_invariant_status": "PASS",
            "generated_at": "2026-05-30T00:00:00Z",
        })
    payload = {"day_utc": DAY, "surfaces": rows, "contract_by_id": {row["surface_id"]: row for row in rows}}
    path = root / "reports" / "aegis_operator_surface_contract_v1" / DAY / "operator_surface_contract.v1.json"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")


def test_operator_route_inventory_declares_all_operator_routes_and_templates(tmp_path: Path) -> None:
    _write_contract(tmp_path)
    payload = build_operator_route_inventory_v1(truth_root=tmp_path, day_utc=DAY)

    assert payload["summary"]["orphan_route_count"] == 0
    assert payload["summary"]["route_count"] >= 10
    by_path = {row["path"]: row for row in payload["routes"]}
    assert by_path["/aegis-command-center"]["template_type"] == "OperatorInboxTemplate"
    assert by_path["/aegis-history"]["surface_id"] == "history"
    assert by_path["/aegis-paper-performance"]["template_type"] == "AnalyticsTemplate"
    assert by_path["/aegis-opportunities"]["template_type"] == "TroubleshootingTemplate"
    assert all(row["contract_required"] is True for row in payload["routes"])


def test_operator_route_inventory_self_check_fails_orphan_routes(tmp_path: Path) -> None:
    _write_contract(tmp_path)
    path = write_operator_route_inventory_v1(truth_root=tmp_path, day_utc=DAY)
    result = run_operator_route_inventory_self_check_v1(truth_root=tmp_path, day_utc=DAY)
    assert path.exists()
    assert result["ok"] is True

    payload = json.loads(path.read_text(encoding="utf-8"))
    payload["routes"][0]["contract_row_present"] = False
    path.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")
    failed = run_operator_route_inventory_self_check_v1(truth_root=tmp_path, day_utc=DAY)
    assert failed["ok"] is False
    assert any(row["check"] == "contract_row_present" for row in failed["failures"])
