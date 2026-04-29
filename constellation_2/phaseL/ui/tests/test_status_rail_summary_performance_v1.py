from __future__ import annotations

from constellation_2.phaseL.ui_api.kernel_operator_shell_v1 import build_kernel_status_rail_summary_view


def test_kernel_status_rail_summary_does_not_hydrate_workspaces(monkeypatch) -> None:
    def _fail() -> dict:
        raise AssertionError("workspace hydration should not run for summary rail")

    monkeypatch.setattr("constellation_2.phaseL.ui_api.kernel_operator_shell_v1.build_control_workspace_view", _fail)
    monkeypatch.setattr("constellation_2.phaseL.ui_api.kernel_operator_shell_v1.build_state_workspace_view", _fail)
    monkeypatch.setattr("constellation_2.phaseL.ui_api.kernel_operator_shell_v1.build_advisory_workspace_view", _fail)
    monkeypatch.setattr("constellation_2.phaseL.ui_api.kernel_operator_shell_v1.build_submission_workspace_view", _fail)
    monkeypatch.setattr("constellation_2.phaseL.ui_api.kernel_operator_shell_v1.build_lifecycle_workspace_view", _fail)

    payload = build_kernel_status_rail_summary_view()

    assert payload["ok"] is True
    assert payload["summary_only"] is True
    assert [row["status"]["code"] for row in payload["kernels"]] == ["unknown", "unknown", "unknown", "unknown", "unknown"]
