from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path
from types import SimpleNamespace

import pytest

SOURCE_ROOT = Path("/home/node/constellation")
if str(SOURCE_ROOT) not in sys.path:
    sys.path.insert(0, str(SOURCE_ROOT))

from constellation_2.common import single_node_hosted_deployment_v1


REPO_ROOT = Path("/home/node/constellation")


def _runtime_identity(tmp_path: Path) -> dict[str, object]:
    runtime_data_root = tmp_path / "runtime_data"
    return {
        "contract_path": str(tmp_path / "runtime_contract_v1" / "active_runtime_contract.v1.json"),
        "contract_sha256": "a" * 64,
        "authoritative_repo_root": str(REPO_ROOT),
        "runtime_environment": "PAPER",
        "primary_execution_identity_ref": {
            "authority_owner": "execution_identity_binding_v1",
            "sleeve_id": "PRIMARY",
        },
        "runtime_data_root": str(runtime_data_root),
        "canonical_truth_root": str(runtime_data_root / "truth"),
        "truth_sleeves_root": str(runtime_data_root / "truth_sleeves"),
        "release_root": str(tmp_path / "release_root"),
    }


def test_select_hosted_python_prefers_repo_local_venv(tmp_path: Path) -> None:
    repo_root = tmp_path / "repo"
    (repo_root / ".venv_c2/bin").mkdir(parents=True)
    (repo_root / ".venv_c2/bin/python").write_text("", encoding="utf-8")

    selection = single_node_hosted_deployment_v1.resolve_hosted_python_selection_v1(
        repo_root=repo_root,
        requested_python="",
    )

    assert selection["python_selection_mode"] == "REPO_LOCAL_VENV"
    assert selection["resolved_python_executable"].endswith("/.venv_c2/bin/python")


def test_select_hosted_python_falls_back_to_python3(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(single_node_hosted_deployment_v1.shutil, "which", lambda _: "/usr/bin/python3.12")

    selection = single_node_hosted_deployment_v1.resolve_hosted_python_selection_v1(
        repo_root=tmp_path,
        requested_python="",
    )

    assert selection["requested_python"] == "python3"
    assert selection["python_selection_mode"] == "SYSTEM_PYTHON3"
    assert selection["resolved_python_executable"] == "/usr/bin/python3.12"


def test_derive_hosted_preflight_payload_fails_on_legacy_runtime_python(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    runtime_identity = _runtime_identity(tmp_path)
    Path(runtime_identity["runtime_data_root"]).mkdir(parents=True)
    Path(runtime_identity["canonical_truth_root"]).mkdir(parents=True)
    Path(runtime_identity["truth_sleeves_root"]).mkdir(parents=True)
    monkeypatch.setattr(
        single_node_hosted_deployment_v1,
        "load_active_runtime_identity_snapshot_v1",
        lambda repo_root=None: runtime_identity,
    )
    monkeypatch.setattr(
        single_node_hosted_deployment_v1,
        "resolve_hosted_python_selection_v1",
        lambda **_: {
            "requested_python": "/home/node/constellation_2_runtime/.venv_c2/bin/python",
            "resolved_python_executable": "/home/node/constellation_2_runtime/.venv_c2/bin/python",
            "python_selection_mode": "ENV_OVERRIDE",
        },
    )
    monkeypatch.setattr(
        single_node_hosted_deployment_v1,
        "_run_repo_authority_proof",
        lambda **_: (True, "PASS"),
    )
    monkeypatch.setattr(
        single_node_hosted_deployment_v1,
        "_probe_python_modules",
        lambda **_: (True, "modules importable"),
    )
    monkeypatch.setattr(
        single_node_hosted_deployment_v1,
        "_check_service_unit_alignment",
        lambda **_: (True, "aligned"),
    )
    monkeypatch.setattr(
        single_node_hosted_deployment_v1,
        "_require_release_current_startup_guarantee_v1",
        lambda: {"path": str(tmp_path / "runtime_data" / "truth" / "release_current_v1/current.json"), "release_current_id": "f" * 64},
    )

    payload = single_node_hosted_deployment_v1.derive_single_node_hosted_preflight_payload_v1(
        repo_root=REPO_ROOT,
        entrypoint_name="c2_execution_observer_v1.sh",
        entrypoint_path=REPO_ROOT / "ops/run/c2_execution_observer_v1.sh",
        service_name="c2-execution-observer.service",
        requested_python="/home/node/constellation_2_runtime/.venv_c2/bin/python",
        required_python_modules=["ib_insync", "ibapi"],
    )

    assert payload["status"] == "FAIL"
    assert "HOSTED_PREFLIGHT_LEGACY_RUNTIME_PYTHON_FORBIDDEN" in payload["blocking_codes"]


def test_write_hosted_preflight_receipt_round_trip(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    runtime_identity = _runtime_identity(tmp_path)
    Path(runtime_identity["runtime_data_root"]).mkdir(parents=True)
    Path(runtime_identity["canonical_truth_root"]).mkdir(parents=True)
    Path(runtime_identity["truth_sleeves_root"]).mkdir(parents=True)
    monkeypatch.setattr(
        single_node_hosted_deployment_v1,
        "load_active_runtime_identity_snapshot_v1",
        lambda repo_root=None: runtime_identity,
    )
    monkeypatch.setattr(
        single_node_hosted_deployment_v1,
        "resolve_hosted_python_selection_v1",
        lambda **_: {
            "requested_python": "python3",
            "resolved_python_executable": "/usr/bin/python3.12",
            "python_selection_mode": "SYSTEM_PYTHON3",
        },
    )
    monkeypatch.setattr(
        single_node_hosted_deployment_v1,
        "_run_repo_authority_proof",
        lambda **_: (True, "PASS"),
    )
    monkeypatch.setattr(
        single_node_hosted_deployment_v1,
        "_probe_python_modules",
        lambda **_: (True, "modules importable"),
    )
    monkeypatch.setattr(
        single_node_hosted_deployment_v1,
        "_check_service_unit_alignment",
        lambda **_: (True, "aligned"),
    )
    monkeypatch.setattr(
        single_node_hosted_deployment_v1,
        "_require_release_current_startup_guarantee_v1",
        lambda: {"path": str(tmp_path / "runtime_data" / "truth" / "release_current_v1/current.json"), "release_current_id": "f" * 64},
    )

    payload = single_node_hosted_deployment_v1.derive_single_node_hosted_preflight_payload_v1(
        repo_root=REPO_ROOT,
        entrypoint_name="c2_paper_day_orchestrator_systemd_entry_v1.sh",
        entrypoint_path=REPO_ROOT / "ops/run/c2_paper_day_orchestrator_systemd_entry_v1.sh",
        service_name="c2-paper-day-orchestrator.service",
        requested_python="python3",
        required_python_modules=[],
    )
    receipt_path = single_node_hosted_deployment_v1.write_single_node_hosted_preflight_receipt_v1(
        repo_root=REPO_ROOT,
        payload=payload,
    )

    written = json.loads(receipt_path.read_text(encoding="utf-8"))
    assert receipt_path.name == "single_node_hosted_preflight.v1.json"
    assert written["status"] == "PASS"
    assert written["python_selection_mode"] == "SYSTEM_PYTHON3"
    assert written["runtime_identity_ref"]["runtime_environment"] == "PAPER"
    check_ids = {row["check_id"] for row in written["checks"]}
    assert "release_current_startup_guarantee" in check_ids


def test_service_unit_alignment_accepts_active_release_symlink_paths(tmp_path: Path) -> None:
    repo_root = tmp_path / "repo"
    entrypoint_path = repo_root / "ops/run/c2_paper_day_orchestrator_systemd_entry_v1.sh"
    entrypoint_path.parent.mkdir(parents=True, exist_ok=True)
    entrypoint_path.write_text("#!/usr/bin/env bash\n", encoding="utf-8")

    release_root = tmp_path / "releases/20260423"
    release_entrypoint_path = release_root / "ops/run/c2_paper_day_orchestrator_systemd_entry_v1.sh"
    release_entrypoint_path.parent.mkdir(parents=True, exist_ok=True)
    release_entrypoint_path.write_text("#!/usr/bin/env bash\n", encoding="utf-8")

    active_release = tmp_path / "constellation_active"
    active_release.symlink_to(release_root, target_is_directory=True)

    service_path = tmp_path / "c2-paper-day-orchestrator.service"
    service_path.write_text(
        "\n".join(
            [
                "[Service]",
                f"WorkingDirectory={active_release}",
                (
                    "ExecStart=/usr/bin/bash -lc "
                    f"'set -euo pipefail; exec {active_release}/ops/run/c2_paper_day_orchestrator_systemd_entry_v1.sh'"
                ),
            ]
        )
        + "\n",
        encoding="utf-8",
    )

    aligned, detail = single_node_hosted_deployment_v1._check_service_unit_alignment(
        service_unit_path=service_path,
        repo_root=repo_root,
        entrypoint_path=entrypoint_path,
        release_root=release_root,
    )
    assert aligned is True
    assert detail == "aligned"


def test_service_unit_alignment_fails_when_entrypoint_does_not_match(tmp_path: Path) -> None:
    repo_root = tmp_path / "repo"
    entrypoint_path = repo_root / "ops/run/c2_paper_day_orchestrator_systemd_entry_v1.sh"
    entrypoint_path.parent.mkdir(parents=True, exist_ok=True)
    entrypoint_path.write_text("#!/usr/bin/env bash\n", encoding="utf-8")

    service_path = tmp_path / "c2-paper-day-orchestrator.service"
    service_path.write_text(
        "\n".join(
            [
                "[Service]",
                f"WorkingDirectory={repo_root}",
                "ExecStart=/usr/bin/bash -lc 'set -euo pipefail; exec /tmp/not-the-right-entrypoint.sh'",
            ]
        )
        + "\n",
        encoding="utf-8",
    )

    aligned, detail = single_node_hosted_deployment_v1._check_service_unit_alignment(
        service_unit_path=service_path,
        repo_root=repo_root,
        entrypoint_path=entrypoint_path,
        release_root=None,
    )
    assert aligned is False
    assert detail.startswith("MISSING_ENTRYPOINT:")


def test_release_current_startup_guarantee_fails_closed_when_missing(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    truth_root = tmp_path / "truth"
    truth_root.mkdir(parents=True)
    monkeypatch.setattr(
        single_node_hosted_deployment_v1,
        "read_control_plane_surface_v1",
        lambda **_: SimpleNamespace(payload={"canonical_truth_root": str(truth_root)}),
    )
    monkeypatch.setattr(
        single_node_hosted_deployment_v1,
        "load_release_current_shadow_ref_if_present",
        lambda **_: None,
    )

    with pytest.raises(SystemExit, match="HOSTED_PREFLIGHT_RELEASE_CURRENT_MISSING"):
        single_node_hosted_deployment_v1._require_release_current_startup_guarantee_v1()


def test_release_current_startup_guarantee_fails_closed_when_invalid(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    truth_root = tmp_path / "truth"
    truth_root.mkdir(parents=True)
    monkeypatch.setattr(
        single_node_hosted_deployment_v1,
        "read_control_plane_surface_v1",
        lambda **_: SimpleNamespace(payload={"canonical_truth_root": str(truth_root)}),
    )
    monkeypatch.setattr(
        single_node_hosted_deployment_v1,
        "load_release_current_shadow_ref_if_present",
        lambda **_: (_ for _ in ()).throw(SystemExit("FAIL: release_current_shadow_invalid path=/tmp/current.json err=ValueError:bad")),
    )

    with pytest.raises(SystemExit, match="release_current_shadow_invalid"):
        single_node_hosted_deployment_v1._require_release_current_startup_guarantee_v1()


def test_release_current_startup_guarantee_validates_before_runtime_identity(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    called = {"runtime_identity": False}
    monkeypatch.setattr(
        single_node_hosted_deployment_v1,
        "_require_release_current_startup_guarantee_v1",
        lambda: (_ for _ in ()).throw(SystemExit("FAIL: HOSTED_PREFLIGHT_RELEASE_CURRENT_MISSING:/tmp/current.json")),
    )
    monkeypatch.setattr(
        single_node_hosted_deployment_v1,
        "load_active_runtime_identity_snapshot_v1",
        lambda repo_root=None: called.__setitem__("runtime_identity", True),
    )

    with pytest.raises(SystemExit, match="HOSTED_PREFLIGHT_RELEASE_CURRENT_MISSING"):
        single_node_hosted_deployment_v1.derive_single_node_hosted_preflight_payload_v1(
            repo_root=REPO_ROOT,
            entrypoint_name="c2_paper_day_orchestrator_systemd_entry_v1.sh",
            entrypoint_path=REPO_ROOT / "ops/run/c2_paper_day_orchestrator_systemd_entry_v1.sh",
            service_name="c2-paper-day-orchestrator.service",
        )
    assert called["runtime_identity"] is False


def test_release_current_startup_guarantee_happy_path(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    truth_root = tmp_path / "truth"
    truth_root.mkdir(parents=True)
    current_path = truth_root / "release_current_v1" / "current.json"
    current_path.parent.mkdir(parents=True)
    current_path.write_text("{}", encoding="utf-8")
    monkeypatch.setattr(
        single_node_hosted_deployment_v1,
        "read_control_plane_surface_v1",
        lambda **_: SimpleNamespace(payload={"canonical_truth_root": str(truth_root)}),
    )
    monkeypatch.setattr(
        single_node_hosted_deployment_v1,
        "load_release_current_shadow_ref_if_present",
        lambda **_: SimpleNamespace(path=current_path, payload={"release_current_id": "f" * 64}),
    )

    result = single_node_hosted_deployment_v1._require_release_current_startup_guarantee_v1()
    assert result["path"] == str(current_path.resolve())
    assert result["release_current_id"] == "f" * 64


def test_touched_wrappers_are_shell_parseable_after_hosted_preflight_change() -> None:
    for relpath in (
        "ops/run/c2_paper_day_orchestrator_systemd_entry_v1.sh",
        "ops/run/c2_execution_observer_v1.sh",
    ):
        subprocess.run(["bash", "-n", str(REPO_ROOT / relpath)], check=True)


def test_paper_day_entrypoint_bootstrap_gate_precedes_day_open_attempt() -> None:
    script_path = REPO_ROOT / "ops/run/c2_paper_day_orchestrator_systemd_entry_v1.sh"
    script = script_path.read_text(encoding="utf-8")
    bootstrap_step = 'echo "STEP=paper_session_bootstrap"'
    bootstrap_call = '"${PY}" "${BOOTSTRAP_TOOL}"'
    bootstrap_guard = "if (( BOOTSTRAP_RC != 0 )); then"
    day_open_call = 'if "${PY}" "${REPO_ROOT}/ops/tools/run_day_open_attempt_v1.py"'
    assert bootstrap_step in script
    assert bootstrap_call in script
    assert bootstrap_guard in script
    assert day_open_call in script
    assert script.index(bootstrap_step) < script.index(day_open_call)
