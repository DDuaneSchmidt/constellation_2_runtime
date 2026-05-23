#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any

REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from ops.aegis.pure_runtime_evaluator_v1 import evaluate_runtime  # noqa: E402
from ops.aegis.runtime_evaluation_v1 import stable_hash_v1, stable_json_bytes_v1  # noqa: E402


def replay_aegis_runtime_v1(*, audit_bundle: Path) -> dict[str, Any]:
    bundle = Path(audit_bundle).expanduser().resolve()
    manifest = _read_json(bundle / "bundle_manifest.json")
    stored_hash = (bundle / "bundle_hash.txt").read_text(encoding="utf-8").strip() if (bundle / "bundle_hash.txt").exists() else ""
    current_manifest_hash = stable_hash_v1(manifest)
    if stored_hash != current_manifest_hash:
        return _fail("BUNDLE_MANIFEST_HASH_MISMATCH", expected=stored_hash, actual=current_manifest_hash)
    for name, info in (manifest.get("files") or {}).items():
        path = bundle / str(info.get("path") or "")
        if not path.exists():
            return _fail("BUNDLE_FILE_MISSING", file=name, path=str(path))
        actual = _file_hash(path)
        if actual != info.get("sha256"):
            return _fail("BUNDLE_FILE_HASH_MISMATCH", file=name, expected=info.get("sha256"), actual=actual)
    snapshot = _read_json(bundle / "evidence_snapshot.v1.json")
    policy = _read_json(bundle / "policy_bundle.v1.json")
    expected = _read_json(bundle / "runtime_evaluation.v1.json")
    replayed = evaluate_runtime(str(expected["day_utc"]), snapshot, policy)
    expected_hash = str(expected.get("deterministic_output_hash") or "")
    actual_hash = str(replayed.get("deterministic_output_hash") or "")
    if expected_hash != actual_hash:
        return {
            "ok": False,
            "status": "RUNTIME_EVALUATION_HASH_MISMATCH",
            "expected_hash": expected_hash,
            "actual_hash": actual_hash,
            "expected_runtime_truth_classification": expected.get("runtime_truth_classification"),
            "actual_runtime_truth_classification": replayed.get("runtime_truth_classification"),
        }
    return {"ok": True, "status": "REPLAY_PASS", "runtime_evaluation_hash": actual_hash}


def _read_json(path: Path) -> dict[str, Any]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError(f"JSON object required: {path}")
    return payload


def _file_hash(path: Path) -> str:
    return stable_hash_v1(path.read_bytes().decode("utf-8", errors="replace"))


def _fail(status: str, **details: Any) -> dict[str, Any]:
    return {"ok": False, "status": status, **details}


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="replay_aegis_runtime_v1")
    parser.add_argument("--audit-bundle", "--audit_bundle", dest="audit_bundle", required=True)
    args = parser.parse_args(argv)
    result = replay_aegis_runtime_v1(audit_bundle=Path(args.audit_bundle))
    print(json.dumps(result, sort_keys=True))
    return 0 if result.get("ok") else 2


if __name__ == "__main__":
    raise SystemExit(main())
