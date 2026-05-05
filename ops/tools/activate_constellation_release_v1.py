#!/usr/bin/env python3
from __future__ import annotations

import argparse
import hashlib
import json
import os
import subprocess
import sys
from datetime import UTC, datetime
from pathlib import Path

_THIS_FILE = Path(__file__).resolve()
REPO_ROOT = _THIS_FILE.parents[2].resolve()
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))
from constellation_2.common.control_plane_read_gateway_v1 import read_control_plane_surface_v1
from constellation_2.common.release_current_shadow_validator_v1 import (
    load_release_current_shadow_ref_if_present,
)
RELEASES_ROOT = Path("/home/node/constellation_releases").resolve()
ACTIVE_POINTER = Path("/home/node/constellation_active")
RUNTIME_DATA_ROOT = Path("/home/node/constellation_runtime_data").resolve()
CURRENT_RELEASE_MANIFEST = (RUNTIME_DATA_ROOT / "truth" / "releases" / "current_release.v1.json").resolve()
RELEASE_SCHEMA_RELPATH = "governance/04_DATA/SCHEMAS/C2/RELEASES/release_manifest.v1.schema.json"
ACTIVATION_SCHEMA_RELPATH = "governance/04_DATA/SCHEMAS/C2/RELEASES/activation_receipt.v1.schema.json"
WRITE_RUNTIME_CONTRACT_TOOL = (REPO_ROOT / "ops/tools/write_active_runtime_contract_v1.py").resolve()
REDUCE_RELEASE_CURRENT_TOOL = (REPO_ROOT / "ops/tools/run_release_current_reducer_v1.py").resolve()
ACTIVATION_IN_PROGRESS_LOCK = (RUNTIME_DATA_ROOT / "activations_v1" / ".activation_in_progress.lock").resolve()


def _sha256_file(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as fh:
        for chunk in iter(lambda: fh.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def _utc_now_iso() -> str:
    return datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _utc_now_compact() -> str:
    return datetime.now(UTC).replace(microsecond=0).strftime("%Y%m%dT%H%M%SZ")


def _load_json(path: Path) -> dict:
    return json.loads(path.read_text(encoding="utf-8"))


def _load_json_if_present(path: Path) -> dict | None:
    if not path.exists() or not path.is_file():
        return None
    return _load_json(path)


def _latest_release_id_or_fail() -> str:
    if not RELEASES_ROOT.exists() or not RELEASES_ROOT.is_dir():
        raise SystemExit(f"FAIL: releases root missing: {RELEASES_ROOT}")
    candidates: list[str] = []
    for child in RELEASES_ROOT.iterdir():
        if not child.is_dir():
            continue
        manifest_path = (child / "release_manifest.v1.json").resolve()
        if not manifest_path.exists() or not manifest_path.is_file():
            continue
        candidates.append(child.name)
    if not candidates:
        raise SystemExit("FAIL: no release directories with release_manifest.v1.json found")
    return sorted(candidates)[-1]


def _require_manifest_for_release(release_root: Path) -> dict:
    manifest_path = (release_root / "release_manifest.v1.json").resolve()
    if not manifest_path.exists() or not manifest_path.is_file():
        raise SystemExit(f"FAIL: release manifest missing: {manifest_path}")
    import sys

    if str(release_root) not in sys.path:
        sys.path.insert(0, str(release_root))
    from constellation_2.phaseD.lib.validate_against_schema_v1 import validate_against_repo_schema_v1

    manifest = _load_json(manifest_path)
    validate_against_repo_schema_v1(manifest, release_root, RELEASE_SCHEMA_RELPATH)
    return manifest


def _verify_release_parity_or_fail(*, release_root: Path, manifest: dict) -> None:
    for rel in manifest["included_files"]:
        rel_path = Path(str(rel))
        path = (release_root / rel_path).resolve()
        if not path.exists() or not path.is_file():
            raise SystemExit(f"FAIL: parity missing release file: {path}")
        actual = _sha256_file(path)
        expected = str(manifest["included_file_hashes"].get(str(rel_path)) or "").strip().lower()
        if actual != expected:
            raise SystemExit(
                f"FAIL: parity hash mismatch rel={rel_path} expected={expected} actual={actual}"
            )


def _current_active_release_id() -> str | None:
    if not ACTIVE_POINTER.exists():
        return None
    if not ACTIVE_POINTER.is_symlink():
        raise SystemExit(f"FAIL: active pointer exists but is not symlink: {ACTIVE_POINTER}")
    target = ACTIVE_POINTER.resolve()
    manifest_path = (target / "release_manifest.v1.json").resolve()
    if not manifest_path.exists():
        return None
    manifest = _load_json(manifest_path)
    return str(manifest.get("release_id") or "").strip() or None


def _atomic_activate_symlink(release_root: Path) -> None:
    parent = ACTIVE_POINTER.parent
    parent.mkdir(parents=True, exist_ok=True)
    tmp_link = parent / f".constellation_active.tmp.{os.getpid()}"
    if tmp_link.exists() or tmp_link.is_symlink():
        tmp_link.unlink()
    os.symlink(str(release_root), str(tmp_link))
    os.replace(str(tmp_link), str(ACTIVE_POINTER))


def _restore_prior_active_pointer_or_fail(prior_target: Path | None) -> None:
    if prior_target is None:
        if ACTIVE_POINTER.exists() or ACTIVE_POINTER.is_symlink():
            ACTIVE_POINTER.unlink()
        return
    _atomic_activate_symlink(prior_target)


def _write_current_release_manifest_v1(
    *,
    release_id: str,
    release_root: Path,
    manifest: dict,
    release_manifest_hash: str,
    activated_at_utc: str,
    approval_id: str,
    previous_release: dict | None,
) -> None:
    CURRENT_RELEASE_MANIFEST.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        "schema_version": "aegis_current_release.v1",
        "release_id": str(release_id),
        "release_path": str(release_root.resolve()),
        "commit": str(manifest["git_sha"]),
        "release_manifest_hash": str(release_manifest_hash),
        "bundle_hash": "",
        "approval_id": str(approval_id),
        "activated_at_utc": str(activated_at_utc),
        "previous_release": previous_release,
    }
    tmp = CURRENT_RELEASE_MANIFEST.with_name(
        f".{CURRENT_RELEASE_MANIFEST.name}.tmp.{os.getpid()}"
    )
    tmp.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    os.replace(str(tmp), str(CURRENT_RELEASE_MANIFEST))


def _restore_current_release_manifest_v1(prior_payload: dict | None) -> None:
    if prior_payload is None:
        if CURRENT_RELEASE_MANIFEST.exists():
            CURRENT_RELEASE_MANIFEST.unlink()
        return
    CURRENT_RELEASE_MANIFEST.parent.mkdir(parents=True, exist_ok=True)
    tmp = CURRENT_RELEASE_MANIFEST.with_name(
        f".{CURRENT_RELEASE_MANIFEST.name}.restore.{os.getpid()}"
    )
    tmp.write_text(json.dumps(prior_payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    os.replace(str(tmp), str(CURRENT_RELEASE_MANIFEST))


def _acquire_activation_lock_or_fail(*, release_id: str, release_root: Path) -> None:
    ACTIVATION_IN_PROGRESS_LOCK.parent.mkdir(parents=True, exist_ok=True)
    if ACTIVATION_IN_PROGRESS_LOCK.exists():
        raise SystemExit(f"FAIL: activation_in_progress_lock_present path={ACTIVATION_IN_PROGRESS_LOCK}")
    payload = {
        "schema_id": "activation_in_progress.lock.v1",
        "release_id": str(release_id),
        "release_root": str(release_root.resolve()),
        "created_at_utc": _utc_now_iso(),
        "pid": os.getpid(),
    }
    ACTIVATION_IN_PROGRESS_LOCK.write_text(
        json.dumps(payload, sort_keys=True, separators=(",", ":")) + "\n",
        encoding="utf-8",
    )


def _clear_activation_lock() -> None:
    try:
        if ACTIVATION_IN_PROGRESS_LOCK.exists():
            ACTIVATION_IN_PROGRESS_LOCK.unlink()
    except Exception:
        pass


def _write_active_runtime_contract_or_fail() -> None:
    proc = subprocess.run(
        [sys.executable, str(WRITE_RUNTIME_CONTRACT_TOOL)],
        cwd=str(REPO_ROOT),
        capture_output=True,
        text=True,
        check=False,
    )
    if proc.returncode != 0:
        raise SystemExit(
            "FAIL: active runtime contract write failed "
            f"rc={proc.returncode} stdout={proc.stdout.strip()!r} stderr={proc.stderr.strip()!r}"
        )


def _materialize_release_current_or_fail() -> None:
    proc = subprocess.run(
        [sys.executable, str(REDUCE_RELEASE_CURRENT_TOOL)],
        cwd=str(REPO_ROOT),
        capture_output=True,
        text=True,
        check=False,
    )
    if proc.returncode != 0:
        raise SystemExit(
            "FAIL: release_current reducer failed "
            f"rc={proc.returncode} stdout={proc.stdout.strip()!r} stderr={proc.stderr.strip()!r}"
        )
    try:
        contract_ref = read_control_plane_surface_v1(domain="release", surface="active_runtime_contract")
    except Exception as exc:
        raise SystemExit(
            "FAIL: release_current contract unreadable after activation "
            f"err={type(exc).__name__}:{exc}"
        ) from exc
    canonical_truth_root = Path(str(contract_ref.payload.get("canonical_truth_root") or "")).resolve()
    if not canonical_truth_root.is_absolute() or not canonical_truth_root.exists() or not canonical_truth_root.is_dir():
        raise SystemExit(
            f"FAIL: release_current canonical_truth_root invalid after activation: {canonical_truth_root}"
        )
    release_current_ref = load_release_current_shadow_ref_if_present(truth_root=canonical_truth_root)
    if release_current_ref is None:
        missing_path = (canonical_truth_root / "release_current_v1" / "current.json").resolve()
        raise SystemExit(f"FAIL: release_current publication missing after activation path={missing_path}")


def _post_activation_verify_or_fail(*, release_root: Path) -> None:
    from constellation_2.common.deployment_state_machine_v1 import evaluate_post_activation_verification

    verification = evaluate_post_activation_verification(release_root=release_root)
    if verification["passed"] is not True:
        raise SystemExit(
            "FAIL: post-activation verification failed "
            f"blocking_codes={verification['blocking_codes']!r} "
            f"service_unit_path={verification['service_unit_path']!r}"
        )


def _activate_runtime_authority_stack_or_fail(
    *,
    release_id: str,
    release_root: Path,
    manifest: dict,
    release_manifest_hash: str,
    activated_at_utc: str,
    approval_id: str,
    prior_target: Path | None,
    prior_current_release: dict | None,
) -> None:
    _acquire_activation_lock_or_fail(release_id=release_id, release_root=release_root)
    current_release_written = False
    try:
        _atomic_activate_symlink(release_root)
        try:
            _write_current_release_manifest_v1(
                release_id=release_id,
                release_root=release_root,
                manifest=manifest,
                release_manifest_hash=release_manifest_hash,
                activated_at_utc=activated_at_utc,
                approval_id=approval_id,
                previous_release=prior_current_release,
            )
            current_release_written = True
            _write_active_runtime_contract_or_fail()
            _materialize_release_current_or_fail()
            _post_activation_verify_or_fail(release_root=release_root)
        except SystemExit:
            _restore_prior_active_pointer_or_fail(prior_target)
            if current_release_written:
                _restore_current_release_manifest_v1(prior_current_release)
            if prior_target is not None:
                _write_active_runtime_contract_or_fail()
            raise
        except Exception:
            _restore_prior_active_pointer_or_fail(prior_target)
            if current_release_written:
                _restore_current_release_manifest_v1(prior_current_release)
            if prior_target is not None:
                _write_active_runtime_contract_or_fail()
            raise
    finally:
        _clear_activation_lock()


def main() -> int:
    ap = argparse.ArgumentParser(prog="activate_constellation_release_v1")
    selection = ap.add_mutually_exclusive_group(required=True)
    selection.add_argument("--release_id", default="")
    selection.add_argument("--latest", action="store_true")
    args = ap.parse_args()

    if bool(args.latest):
        release_id = _latest_release_id_or_fail()
    else:
        release_id = str(args.release_id).strip()
    if not release_id:
        raise SystemExit("FAIL: release_id empty")
    release_root = (RELEASES_ROOT / release_id).resolve()
    if not release_root.exists() or not release_root.is_dir():
        raise SystemExit(f"FAIL: release root missing: {release_root}")

    manifest = _require_manifest_for_release(release_root)
    _verify_release_parity_or_fail(release_root=release_root, manifest=manifest)
    release_manifest_hash = _sha256_file((release_root / "release_manifest.v1.json").resolve())
    prior_release_id = _current_active_release_id()

    RUNTIME_DATA_ROOT.mkdir(parents=True, exist_ok=True)
    prior_target = ACTIVE_POINTER.resolve() if ACTIVE_POINTER.exists() else None
    prior_current_release = _load_json_if_present(CURRENT_RELEASE_MANIFEST)
    activated_at_utc = _utc_now_iso()
    receipt_id = f"{_utc_now_compact()}__{release_id}"
    approval_id = f"activation_receipt:{receipt_id}"
    _activate_runtime_authority_stack_or_fail(
        release_id=release_id,
        release_root=release_root,
        manifest=manifest,
        release_manifest_hash=release_manifest_hash,
        activated_at_utc=activated_at_utc,
        approval_id=approval_id,
        prior_target=prior_target,
        prior_current_release=prior_current_release,
    )

    receipt_dir = (RUNTIME_DATA_ROOT / "activations_v1" / receipt_id).resolve()
    receipt_dir.mkdir(parents=True, exist_ok=False)
    receipt = {
        "schema_id": "activation_receipt.v1",
        "schema_version": "v1",
        "release_id": release_id,
        "git_sha": str(manifest["git_sha"]),
        "prior_release_id": prior_release_id,
        "active_pointer_path": str(ACTIVE_POINTER),
        "runtime_data_root": str(RUNTIME_DATA_ROOT),
        "services_reloaded": [],
        "parity_verified": True,
        "generated_at_utc": activated_at_utc,
        "status": "ACTIVATED",
    }

    if str(release_root) not in sys.path:
        sys.path.insert(0, str(release_root))
    from constellation_2.phaseD.lib.validate_against_schema_v1 import validate_against_repo_schema_v1
    from constellation_2.phaseC.lib.canon_json_v1 import canonical_json_bytes_v1

    validate_against_repo_schema_v1(receipt, release_root, ACTIVATION_SCHEMA_RELPATH)
    receipt_path = (receipt_dir / "activation_receipt.v1.json").resolve()
    receipt_path.write_bytes(canonical_json_bytes_v1(receipt) + b"\n")

    print(
        json.dumps(
            {
                "release_id": release_id,
                "release_root": str(release_root),
                "active_pointer_target": str(ACTIVE_POINTER.resolve()),
                "runtime_data_root": str(RUNTIME_DATA_ROOT),
                "activation_receipt_path": str(receipt_path),
                "prior_release_id": prior_release_id,
            },
            indent=2,
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
