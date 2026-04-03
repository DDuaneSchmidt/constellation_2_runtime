#!/usr/bin/env python3
from __future__ import annotations

import argparse
import hashlib
import json
import subprocess
import sys
from pathlib import Path
from typing import Any, Dict

_THIS_FILE = Path(__file__).resolve()
_REPO_ROOT_FROM_FILE = _THIS_FILE.parents[2]
if str(_REPO_ROOT_FROM_FILE) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT_FROM_FILE))

if not (_REPO_ROOT_FROM_FILE / "constellation_2").exists():
    raise SystemExit(f"FATAL: repo_root_missing_constellation_2: derived={_REPO_ROOT_FROM_FILE}")
if not (_REPO_ROOT_FROM_FILE / "governance").exists():
    raise SystemExit(f"FATAL: repo_root_missing_governance: derived={_REPO_ROOT_FROM_FILE}")

from constellation_2.phaseD.lib.canon_json_v1 import canonical_json_bytes_v1  # noqa: E402
from constellation_2.phaseD.lib.validate_against_schema_v1 import SchemaValidationError, validate_against_repo_schema_v1  # noqa: E402
from constellation_2.phaseF.accounting.lib.immut_write_v1 import ImmutableWriteError, write_file_immutable_v1  # noqa: E402

REPO_ROOT = Path("/home/node/constellation_2_runtime").resolve()
SCHEMA_EXPOSURE = "constellation_2/schemas/exposure_intent.v1.schema.json"
SCHEMA_OPTIONS = "constellation_2/schemas/options_intent.v2.schema.json"
SCHEMA_POLICY = "governance/04_DATA/SCHEMAS/C2/OPTIONS/exposure_to_options_intent_policy.v1.schema.json"
SCHEMA_ADAPTER_RECORD = "constellation_2/schemas/exposure_to_options_adapter_record.v1.schema.json"


class AdapterError(Exception):
    pass


def _run_git_head_short() -> str:
    try:
        cp = subprocess.run(
            ["git", "rev-parse", "--short=12", "HEAD"],
            cwd=str(REPO_ROOT),
            text=True,
            capture_output=True,
            check=False,
        )
    except Exception as e:
        raise AdapterError(f"GIT_SHA_QUERY_FAILED: {e}") from e
    if cp.returncode != 0:
        raise AdapterError(f"GIT_SHA_QUERY_FAILED: rc={cp.returncode} stderr={cp.stderr.strip()!r}")
    out = (cp.stdout or "").strip()
    if not out:
        raise AdapterError("GIT_SHA_QUERY_EMPTY")
    return out


def _read_json_obj(path: Path) -> Dict[str, Any]:
    if not path.exists() or not path.is_file():
        raise AdapterError(f"INPUT_FILE_MISSING: {path}")
    obj = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(obj, dict):
        raise AdapterError(f"TOP_LEVEL_NOT_OBJECT: {path}")
    return obj


def _sha256_file(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _with_self_hash(obj: Dict[str, Any]) -> Dict[str, Any]:
    x = dict(obj)
    x["canonical_json_hash"] = None
    digest = hashlib.sha256(canonical_json_bytes_v1(x) + b"\n").hexdigest()
    x["canonical_json_hash"] = digest
    return x


def _immutable_write_json(path: Path, obj: Dict[str, Any]) -> None:
    write_file_immutable_v1(path=path, data=canonical_json_bytes_v1(obj) + b"\n", create_dirs=True)


def _find_engine_policy(policy_obj: Dict[str, Any], engine_id: str) -> Dict[str, Any]:
    entries = policy_obj.get("engine_policies")
    if not isinstance(entries, list):
        raise AdapterError("POLICY_ENGINE_POLICIES_NOT_ARRAY")
    for item in entries:
        if not isinstance(item, dict):
            continue
        if str(item.get("engine_id") or "").strip() == engine_id:
            return item
    raise AdapterError(f"ENGINE_NOT_POLICY_ALLOWLISTED: engine_id={engine_id}")


def _require_in(value: Any, allowed: list[Any], *, label: str) -> None:
    if value not in allowed:
        raise AdapterError(f"POLICY_REQUIREMENT_FAILED:{label}:value={value!r}:allowed={allowed!r}")


def _adapt(exposure_obj: Dict[str, Any], engine_policy: Dict[str, Any]) -> Dict[str, Any]:
    req = engine_policy.get("exposure_requirements")
    tpl = engine_policy.get("options_template")
    if not isinstance(req, dict):
        raise AdapterError("POLICY_EXPOSURE_REQUIREMENTS_NOT_OBJECT")
    if not isinstance(tpl, dict):
        raise AdapterError("POLICY_OPTIONS_TEMPLATE_NOT_OBJECT")

    exposure_type = str(exposure_obj.get("exposure_type") or "").strip()
    engine = exposure_obj.get("engine")
    option = exposure_obj.get("option")
    underlying = exposure_obj.get("underlying")
    if not isinstance(engine, dict):
        raise AdapterError("EXPOSURE_ENGINE_MISSING")
    if not isinstance(option, dict):
        raise AdapterError("EXPOSURE_OPTION_MISSING")
    if not isinstance(underlying, dict):
        raise AdapterError("EXPOSURE_UNDERLYING_MISSING")

    _require_in(exposure_type, [req.get("exposure_type")], label="exposure_type")
    _require_in(str(engine.get("suite") or "").strip(), [req.get("required_engine_suite")], label="engine.suite")
    _require_in(str(option.get("structure") or "").strip(), [req.get("required_option_structure")], label="option.structure")
    _require_in(str(option.get("direction") or "").strip(), [req.get("required_option_direction")], label="option.direction")
    _require_in(str(exposure_obj.get("target_notional_pct") or "").strip(), list(req.get("allowed_target_notional_pct") or []), label="target_notional_pct")
    _require_in(int(exposure_obj.get("expected_holding_days")), list(req.get("allowed_expected_holding_days") or []), label="expected_holding_days")
    _require_in(str(exposure_obj.get("risk_class") or "").strip(), list(req.get("allowed_risk_class") or []), label="risk_class")

    suite_out = str(tpl.get("suite") or "").strip()
    strategy_out = tpl.get("strategy")
    risk_out = tpl.get("risk")
    exit_policy_out = tpl.get("exit_policy")
    selection_policy_out = tpl.get("selection_policy")
    if not isinstance(strategy_out, dict):
        raise AdapterError("POLICY_TEMPLATE_STRATEGY_NOT_OBJECT")
    if not isinstance(risk_out, dict):
        raise AdapterError("POLICY_TEMPLATE_RISK_NOT_OBJECT")
    if not isinstance(exit_policy_out, dict):
        raise AdapterError("POLICY_TEMPLATE_EXIT_POLICY_NOT_OBJECT")
    if not isinstance(selection_policy_out, dict):
        raise AdapterError("POLICY_TEMPLATE_SELECTION_POLICY_NOT_OBJECT")

    options_obj = {
        "schema_id": "options_intent",
        "schema_version": "v2",
        "intent_id": str(exposure_obj.get("intent_id") or "").strip(),
        "created_at_utc": str(exposure_obj.get("created_at_utc") or "").strip(),
        "engine": {
            "engine_id": str(engine.get("engine_id") or "").strip(),
            "suite": suite_out,
            "mode": str(engine.get("mode") or "").strip(),
        },
        "underlying": {
            "symbol": str(underlying.get("symbol") or "").strip(),
            "currency": str(underlying.get("currency") or "").strip(),
        },
        "strategy": dict(strategy_out),
        "risk": dict(risk_out),
        "exit_policy": dict(exit_policy_out),
        "selection_policy": dict(selection_policy_out),
        "canonical_json_hash": None,
    }
    return _with_self_hash(options_obj)


def main() -> int:
    ap = argparse.ArgumentParser(description="Governed adapter: exposure_intent.v1 -> options_intent.v2 (fail-closed).")
    ap.add_argument("--exposure_intent_path", required=True)
    ap.add_argument("--out_dir", required=True)
    ap.add_argument("--produced_utc", required=True)
    ap.add_argument(
        "--policy_path",
        default=str((REPO_ROOT / "governance" / "02_REGISTRIES" / "C2_EXPOSURE_TO_OPTIONS_INTENT_POLICY_V1.json").resolve()),
    )
    args = ap.parse_args()

    exposure_path = Path(str(args.exposure_intent_path)).expanduser().resolve()
    out_dir = Path(str(args.out_dir)).expanduser().resolve()
    policy_path = Path(str(args.policy_path)).expanduser().resolve()

    try:
        exposure = _read_json_obj(exposure_path)
        policy = _read_json_obj(policy_path)

        validate_against_repo_schema_v1(exposure, REPO_ROOT, SCHEMA_EXPOSURE)
        validate_against_repo_schema_v1(policy, REPO_ROOT, SCHEMA_POLICY)

        if str(exposure.get("schema_id") or "").strip() != "exposure_intent" or str(exposure.get("schema_version") or "").strip() != "v1":
            raise AdapterError("EXPOSURE_SCHEMA_ID_OR_VERSION_MISMATCH")
        if str(exposure.get("exposure_type") or "").strip() != "SHORT_VOL_DEFINED":
            raise AdapterError("UNSUPPORTED_EXPOSURE_TYPE")

        engine = exposure.get("engine")
        if not isinstance(engine, dict):
            raise AdapterError("EXPOSURE_ENGINE_MISSING")
        engine_id = str(engine.get("engine_id") or "").strip()
        if not engine_id:
            raise AdapterError("EXPOSURE_ENGINE_ID_MISSING")
        engine_policy = _find_engine_policy(policy, engine_id)

        options_obj = _adapt(exposure, engine_policy)
        validate_against_repo_schema_v1(options_obj, REPO_ROOT, SCHEMA_OPTIONS)

        out_intent = (out_dir / "options_intent.v2.json").resolve()
        _immutable_write_json(out_intent, options_obj)

        adapter_record = {
            "schema_id": "exposure_to_options_adapter_record",
            "schema_version": "v1",
            "status": "OK",
            "reason_codes": ["EXPOSURE_TO_OPTIONS_ADAPTER_OK"],
            "input_exposure_intent": {"path": str(exposure_path), "sha256": _sha256_file(exposure_path)},
            "output_options_intent": {"path": str(out_intent), "sha256": _sha256_file(out_intent)},
            "policy": {"path": str(policy_path), "sha256": _sha256_file(policy_path)},
            "produced_utc": str(args.produced_utc).strip(),
            "producer": {
                "module": "ops/tools/run_exposure_to_options_intent_adapter_v1.py",
                "repo": "constellation_2_runtime",
                "git_sha": _run_git_head_short(),
            },
            "canonical_json_hash": None,
        }
        adapter_record = _with_self_hash(adapter_record)
        validate_against_repo_schema_v1(adapter_record, REPO_ROOT, SCHEMA_ADAPTER_RECORD)
        _immutable_write_json((out_dir / "exposure_to_options_adapter_record.v1.json").resolve(), adapter_record)

        print(f"OK: {out_intent}")
        return 0
    except (AdapterError, SchemaValidationError, ImmutableWriteError) as e:
        print(f"FAIL: {e}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
