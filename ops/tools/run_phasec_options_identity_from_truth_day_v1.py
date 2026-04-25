#!/usr/bin/env python3
from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import subprocess
import sys
import tempfile
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any, Dict, Iterable, Optional, Tuple

_THIS_FILE = Path(__file__).resolve()
_REPO_ROOT_FROM_FILE = _THIS_FILE.parents[2]
if str(_REPO_ROOT_FROM_FILE) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT_FROM_FILE))

if not (_REPO_ROOT_FROM_FILE / "constellation_2").exists():
    raise SystemExit(f"FATAL: repo_root_missing_constellation_2: derived={_REPO_ROOT_FROM_FILE}")
if not (_REPO_ROOT_FROM_FILE / "governance").exists():
    raise SystemExit(f"FATAL: repo_root_missing_governance: derived={_REPO_ROOT_FROM_FILE}")

from constellation_2.phaseD.lib.canon_json_v1 import canonical_hash_for_c2_artifact_v1, canonical_json_bytes_v1  # noqa: E402
from constellation_2.common.execution_identity_authority_v1 import (  # noqa: E402
    build_execution_identity_record_v1,
    classify_duplicate_classification_v1,
    derive_trade_instance_id_v1,
    resolve_intent_id_v1,
)
from constellation_2.phaseC.lib.validate_against_schema_v1 import validate_against_repo_schema_v1  # noqa: E402
from constellation_2.phaseF.accounting.lib.immut_write_v1 import ImmutableWriteError, write_file_immutable_v1  # noqa: E402
from constellation_2.common.truth_root_v1 import resolve_truth_root  # noqa: E402

REPO_ROOT = _REPO_ROOT_FROM_FILE.resolve()
RUNTIME_DATA_ROOT = Path("/home/node/constellation_runtime_data").resolve()


def _require_truth_root_allowed(truth_root: Path) -> Path:
    pr = truth_root.expanduser().resolve()
    if not pr.is_absolute():
        raise SystemExit(f"FAIL: truth_root must be absolute: {pr}")
    if not pr.exists() or not pr.is_dir():
        raise SystemExit(f"FAIL: truth_root missing or not dir: {pr}")
    try:
        pr.relative_to(REPO_ROOT)
        return pr
    except ValueError:
        pass
    try:
        pr.relative_to(RUNTIME_DATA_ROOT)
        return pr
    except ValueError as e:
        raise SystemExit(
            "FAIL: truth_root not under allowed roots: "
            f"truth_root={pr} repo_root={REPO_ROOT} runtime_data_root={RUNTIME_DATA_ROOT}"
        ) from e
    return pr


def _resolve_truth_root(truth_root_arg: str) -> Path:
    arg = (truth_root_arg or "").strip()
    if arg:
        return _require_truth_root_allowed(Path(arg))
    env_root = (os.environ.get("C2_TRUTH_ROOT") or "").strip()
    if env_root:
        return _require_truth_root_allowed(Path(env_root))
    return _require_truth_root_allowed(resolve_truth_root(repo_root=REPO_ROOT))

MAP_VERTICAL_V1 = (REPO_ROOT / "constellation_2" / "phaseA" / "tools" / "c2_map_vertical_v1.py").resolve()
PREFLIGHT_OPTIONS_V1 = (REPO_ROOT / "constellation_2" / "phaseC" / "tools" / "c2_submit_preflight_offline_v1.py").resolve()
EXPOSURE_TO_OPTIONS_ADAPTER_V1 = (REPO_ROOT / "ops" / "tools" / "run_exposure_to_options_intent_adapter_v1.py").resolve()
EXPOSURE_TO_OPTIONS_POLICY_V1 = (
    REPO_ROOT / "governance" / "02_REGISTRIES" / "C2_EXPOSURE_TO_OPTIONS_INTENT_POLICY_V1.json"
).resolve()

SOURCE_REASON_FAIL_CLOSED = "C2_SUBMIT_FAIL_CLOSED_REQUIRED"


class OptionsIdentityError(Exception):
    pass


def _read_json_obj(path: Path) -> Dict[str, Any]:
    if not path.exists() or not path.is_file():
        raise OptionsIdentityError(f"INPUT_FILE_MISSING: {path}")
    obj = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(obj, dict):
        raise OptionsIdentityError(f"TOP_LEVEL_NOT_OBJECT: {path}")
    return obj


def _sha256_file(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _immutable_write_json(path: Path, obj: Dict[str, Any]) -> None:
    data = canonical_json_bytes_v1(obj) + b"\n"
    write_file_immutable_v1(path=path, data=data, create_dirs=True)


def _immutable_copy(src: Path, dst: Path) -> None:
    if not src.exists() or not src.is_file():
        raise OptionsIdentityError(f"SOURCE_FILE_MISSING: {src}")
    write_file_immutable_v1(path=dst, data=src.read_bytes(), create_dirs=True)


def _source_ref(*, ref_type: str, path: Path) -> Dict[str, str]:
    rp = path.resolve()
    return {
        "ref_type": str(ref_type or "").strip(),
        "path": str(rp),
        "sha256": _sha256_file(rp),
    }


def _write_failclosed_veto(
    *,
    out_day_dir: Path,
    day_utc: str,
    eval_time_utc: str,
    intent_hash: str,
    intent_path: Path,
    reason_detail: str,
) -> Path:
    veto_path = (out_day_dir / f"{intent_hash}.veto_record.v1.json").resolve()
    veto_obj = {
        "schema_id": "veto_record",
        "schema_version": "v1",
        "boundary": "SUBMIT",
        "observed_at_utc": eval_time_utc,
        "reason_code": SOURCE_REASON_FAIL_CLOSED,
        "reason_detail": reason_detail,
        "inputs": {
            "intent_hash": intent_hash,
            "plan_hash": None,
            "chain_snapshot_hash": None,
            "freshness_cert_hash": None,
        },
        "input_manifest": [
            {
                "type": "intent",
                "path": str(intent_path.resolve()),
                "sha256": _sha256_file(intent_path),
                "day_utc": day_utc,
                "producer": "intents_v1",
            }
        ],
        "pointers": [],
        "upstream_hash": intent_hash,
        "canonical_json_hash": None,
    }
    _immutable_write_json(veto_path, veto_obj)
    return veto_path


def _find_latest_snapshot_and_cert(truth_root: Path, day_utc: str, symbol: str) -> Tuple[Path, Path]:
    root = (truth_root / "options_chain_snapshot_v1" / day_utc).resolve()
    if not root.exists() or not root.is_dir():
        raise OptionsIdentityError(f"OPTIONS_SNAPSHOT_ROOT_MISSING: {root}")

    candidates = []
    for d in sorted(root.iterdir()):
        snap = (d / "options_chain_snapshot.v1.json").resolve()
        cert = (d / "freshness_certificate.v1.json").resolve()
        if not snap.exists() or not cert.exists():
            continue
        try:
            snap_obj = _read_json_obj(snap)
        except Exception:
            continue
        underlying = snap_obj.get("underlying")
        if not isinstance(underlying, dict):
            continue
        if str(underlying.get("symbol") or "").strip().upper() == symbol.upper():
            candidates.append((snap, cert))

    if not candidates:
        raise OptionsIdentityError(f"SAME_DAY_OPTIONS_TRUTH_MISSING: day={day_utc} symbol={symbol}")
    return candidates[-1]


def _run(cmd: list[str], *, cwd: Path) -> subprocess.CompletedProcess[str]:
    env = dict(os.environ)
    repo_path = str(REPO_ROOT)
    pythonpath = str(env.get("PYTHONPATH") or "").strip()
    if pythonpath:
        env["PYTHONPATH"] = f"{repo_path}{os.pathsep}{pythonpath}"
    else:
        env["PYTHONPATH"] = repo_path
    return subprocess.run(cmd, cwd=str(cwd), env=env, text=True, capture_output=True, check=False)


def _parse_decimal_str(value: str, *, label: str) -> Decimal:
    s = str(value or "").strip()
    if not s:
        raise OptionsIdentityError(f"DECIMAL_STRING_REQUIRED: {label}")
    try:
        return Decimal(s)
    except InvalidOperation as e:
        raise OptionsIdentityError(f"DECIMAL_PARSE_FAILED: {label}={value!r}") from e


def _decimal_quantum_str(value: str, *, label: str) -> str:
    d = _parse_decimal_str(value, label=label)
    if d <= Decimal("0"):
        raise OptionsIdentityError(f"DECIMAL_MUST_BE_POSITIVE: {label}={value!r}")
    normalized = d.normalize()
    exponent = normalized.as_tuple().exponent
    if exponent >= 0:
        return "1"
    return format(Decimal(1).scaleb(exponent), "f")


def _iter_price_strings_from_snapshot(snap_obj: Dict[str, Any]) -> Iterable[str]:
    contracts = snap_obj.get("contracts")
    if not isinstance(contracts, list):
        raise OptionsIdentityError("SNAPSHOT_CONTRACTS_NOT_LIST")

    found = 0
    for idx, contract in enumerate(contracts):
        if not isinstance(contract, dict):
            continue
        for field in ("bid", "ask"):
            raw = contract.get(field)
            if raw is None:
                continue
            s = str(raw).strip()
            if not s:
                continue
            found += 1
            yield s

    if found <= 0:
        raise OptionsIdentityError("NO_PRICE_FIELDS_AVAILABLE_TO_DERIVE_TICK_SIZE")


def _derive_tick_size_from_snapshot(snap_obj: Dict[str, Any]) -> str:
    tick_sizes = set()
    for idx, price_str in enumerate(_iter_price_strings_from_snapshot(snap_obj)):
        tick_sizes.add(_decimal_quantum_str(price_str, label=f"snapshot_price[{idx}]"))

    if not tick_sizes:
        raise OptionsIdentityError("TICK_SIZE_DERIVATION_EMPTY")

    ordered = sorted(tick_sizes, key=lambda s: _parse_decimal_str(s, label=f"tick_size_candidate:{s}"))
    return ordered[0]


def _parse_execution_scope_from_out_day_dir(*, out_day_dir: Path, day_utc: str) -> Tuple[str, str, str]:
    parts = out_day_dir.resolve().parts
    try:
        idx = parts.index("phaseC_preflight_v1")
    except ValueError as e:
        raise OptionsIdentityError(f"PHASEC_OUT_DAY_DIR_INVALID:missing_phaseC_preflight_v1:path={out_day_dir}") from e
    if idx < 2 or idx + 2 >= len(parts):
        raise OptionsIdentityError(f"PHASEC_OUT_DAY_DIR_INVALID:insufficient_scope_parts:path={out_day_dir}")
    path_day = str(parts[idx + 1]).strip()
    if path_day != day_utc:
        raise OptionsIdentityError(f"PHASEC_OUT_DAY_DIR_DAY_MISMATCH:expected={day_utc}:actual={path_day}:path={out_day_dir}")
    attempt_part = str(parts[idx + 2]).strip()
    if not attempt_part.startswith("attempt_"):
        raise OptionsIdentityError(f"PHASEC_OUT_DAY_DIR_ATTEMPT_MISSING:path={out_day_dir}")
    attempt_id = attempt_part[len("attempt_") :]
    if not re.fullmatch(r"A[0-9]{4}", attempt_id):
        raise OptionsIdentityError(f"PHASEC_OUT_DAY_DIR_ATTEMPT_INVALID:attempt_id={attempt_id}:path={out_day_dir}")
    sleeve_id = str(parts[idx - 2]).strip().upper()
    environment = str(parts[idx - 1]).strip().upper()
    if not sleeve_id:
        raise OptionsIdentityError(f"PHASEC_OUT_DAY_DIR_SLEEVE_MISSING:path={out_day_dir}")
    if environment not in {"PAPER", "LIVE", "GLOBAL"}:
        raise OptionsIdentityError(f"PHASEC_OUT_DAY_DIR_ENV_INVALID:environment={environment}:path={out_day_dir}")
    return sleeve_id, environment, attempt_id


def _resolve_prior_identity_context(
    *,
    out_day_dir: Path,
    current_attempt_id: str,
    intent_hash: str,
) -> Tuple[Optional[str], Optional[str], Optional[Path]]:
    day_dir = out_day_dir.resolve().parent
    current_num = int(current_attempt_id[1:])
    best_record_path: Optional[Path] = None
    best_num = -1
    for candidate in sorted(day_dir.iterdir()):
        if not candidate.is_dir():
            continue
        name = candidate.name
        if not name.startswith("attempt_"):
            continue
        attempt_id = name[len("attempt_") :]
        if not re.fullmatch(r"A[0-9]{4}", attempt_id):
            continue
        attempt_num = int(attempt_id[1:])
        if attempt_num >= current_num:
            continue
        record_path = (candidate / intent_hash / "execution_identity_record.v1.json").resolve()
        if record_path.exists() and record_path.is_file() and attempt_num > best_num:
            best_record_path = record_path
            best_num = attempt_num
    if best_record_path is None:
        return None, None, None
    record_obj = _read_json_obj(best_record_path)
    prior_trade_instance_id = str(record_obj.get("trade_instance_id") or "").strip() or None
    prior_plan_hash = str(record_obj.get("plan_hash") or "").strip() or None
    return prior_trade_instance_id, prior_plan_hash, best_record_path


def _require_positive_decimal_from_string(*, value: Any, label: str) -> Decimal:
    raw = str(value or "").strip()
    if not raw:
        raise OptionsIdentityError(f"{label}_MISSING")
    try:
        parsed = Decimal(raw)
    except InvalidOperation as e:
        raise OptionsIdentityError(f"{label}_INVALID_DECIMAL:{value!r}") from e
    if parsed <= Decimal("0"):
        raise OptionsIdentityError(f"{label}_MUST_BE_POSITIVE:{value!r}")
    return parsed


def _write_execution_identity_record(
    *,
    day_utc: str,
    eval_time_utc: str,
    intent_hash: str,
    intent_path: Path,
    intent_obj: Dict[str, Any],
    out_day_dir: Path,
    final_identity_dir: Path,
    order_plan_path: Path,
    mapping_path: Path,
    binding_path: Path,
    decision_path: Path,
    options_intent_path: Path,
    adapter_record_path: Path,
) -> Path:
    if not order_plan_path.exists() or not order_plan_path.is_file():
        raise OptionsIdentityError(f"ORDER_PLAN_MISSING:path={order_plan_path}")
    if not mapping_path.exists() or not mapping_path.is_file():
        raise OptionsIdentityError(f"MAPPING_LEDGER_RECORD_MISSING:path={mapping_path}")
    if not binding_path.exists() or not binding_path.is_file():
        raise OptionsIdentityError(f"BINDING_RECORD_MISSING:path={binding_path}")
    if not decision_path.exists() or not decision_path.is_file():
        raise OptionsIdentityError(f"SUBMIT_PREFLIGHT_DECISION_MISSING:path={decision_path}")
    if not options_intent_path.exists() or not options_intent_path.is_file():
        raise OptionsIdentityError(f"OPTIONS_INTENT_MISSING:path={options_intent_path}")
    if not adapter_record_path.exists() or not adapter_record_path.is_file():
        raise OptionsIdentityError(f"EXPOSURE_TO_OPTIONS_ADAPTER_RECORD_MISSING:path={adapter_record_path}")

    sleeve_id, environment, attempt_id = _parse_execution_scope_from_out_day_dir(out_day_dir=out_day_dir, day_utc=day_utc)

    order_plan_obj = _read_json_obj(order_plan_path)
    mapping_obj = _read_json_obj(mapping_path)
    binding_obj = _read_json_obj(binding_path)
    decision_obj = _read_json_obj(decision_path)
    options_intent_obj = _read_json_obj(options_intent_path)

    legs = order_plan_obj.get("legs")
    if not isinstance(legs, list) or not legs:
        raise OptionsIdentityError(f"OPTIONS_ORDER_PLAN_LEGS_MISSING:path={order_plan_path}")
    for idx, leg in enumerate(legs):
        if not isinstance(leg, dict):
            raise OptionsIdentityError(f"OPTIONS_ORDER_PLAN_LEG_INVALID:index={idx}:path={order_plan_path}")
        for req_field in ("action", "right", "expiry_utc", "strike", "ratio", "ib_conId"):
            if leg.get(req_field) in (None, ""):
                raise OptionsIdentityError(
                    f"OPTIONS_ORDER_PLAN_LEG_FIELD_MISSING:index={idx}:field={req_field}:path={order_plan_path}"
                )

    risk_proof = order_plan_obj.get("risk_proof")
    if not isinstance(risk_proof, dict):
        raise OptionsIdentityError(f"OPTIONS_ORDER_PLAN_RISK_PROOF_MISSING:path={order_plan_path}")
    if risk_proof.get("defined_risk_proven") is not True:
        raise OptionsIdentityError(f"OPTIONS_DEFINED_RISK_NOT_PROVEN:path={order_plan_path}")
    _require_positive_decimal_from_string(value=risk_proof.get("max_loss_usd"), label="OPTIONS_MAX_LOSS_USD")

    decision = str(decision_obj.get("decision") or "").strip().upper()
    if decision != "ALLOW":
        raise OptionsIdentityError(
            f"OPTIONS_SUBMIT_PREFLIGHT_NOT_ALLOW:decision={decision or 'MISSING'}:path={decision_path}"
        )

    plan_hash = canonical_hash_for_c2_artifact_v1(order_plan_obj)
    mapping_hash = canonical_hash_for_c2_artifact_v1(mapping_obj)
    binding_hash = canonical_hash_for_c2_artifact_v1(binding_obj)

    mapped_plan_hash = str(mapping_obj.get("plan_hash") or "").strip()
    if mapped_plan_hash and mapped_plan_hash != plan_hash:
        raise OptionsIdentityError(
            f"OPTIONS_MAPPING_PLAN_HASH_MISMATCH:expected={plan_hash}:actual={mapped_plan_hash}:path={mapping_path}"
        )
    binding_schema_version = str(binding_obj.get("schema_version") or "").strip().lower()
    if binding_schema_version == "v2":
        binding_plan_hash = str(binding_obj.get("plan_hash") or "").strip()
        if binding_plan_hash and binding_plan_hash != plan_hash:
            raise OptionsIdentityError(
                f"OPTIONS_BINDING_PLAN_HASH_MISMATCH:expected={plan_hash}:actual={binding_plan_hash}:path={binding_path}"
            )
        binding_mapping_hash = str(binding_obj.get("mapping_ledger_hash") or "").strip()
        if binding_mapping_hash and binding_mapping_hash != mapping_hash:
            raise OptionsIdentityError(
                "OPTIONS_BINDING_MAPPING_HASH_MISMATCH:"
                f"expected={mapping_hash}:actual={binding_mapping_hash}:path={binding_path}"
            )
    plan_intent_hash = str(order_plan_obj.get("intent_hash") or "").strip().lower()
    mapping_intent_hash = str(mapping_obj.get("intent_hash") or "").strip().lower()
    if plan_intent_hash and mapping_intent_hash and plan_intent_hash != mapping_intent_hash:
        raise OptionsIdentityError(
            "OPTIONS_PLAN_MAPPING_INTENT_HASH_MISMATCH:"
            f"plan={plan_intent_hash}:mapping={mapping_intent_hash}:path={mapping_path}"
        )

    intent_id = resolve_intent_id_v1(intent_obj=intent_obj, plan_obj=options_intent_obj)
    trade_instance_id = derive_trade_instance_id_v1(
        day_utc=day_utc,
        attempt_id=attempt_id,
        sleeve_id=sleeve_id,
        environment=environment,
        intent_id=intent_id,
        intent_hash=intent_hash,
    )
    prior_trade_instance_id, prior_plan_hash, prior_identity_path = _resolve_prior_identity_context(
        out_day_dir=out_day_dir,
        current_attempt_id=attempt_id,
        intent_hash=intent_hash,
    )
    duplicate_classification = classify_duplicate_classification_v1(
        prior_trade_instance_id=prior_trade_instance_id,
        prior_plan_hash=prior_plan_hash,
        current_trade_instance_id=trade_instance_id,
        current_plan_hash=plan_hash,
    )
    submission_id = str(binding_obj.get("submission_id") or "").strip() or binding_hash

    source_refs = [
        _source_ref(ref_type="exposure_intent_ref", path=intent_path),
        _source_ref(ref_type="options_intent_ref", path=options_intent_path),
        _source_ref(ref_type="order_plan_ref", path=order_plan_path),
        _source_ref(ref_type="mapping_ledger_record_ref", path=mapping_path),
        _source_ref(ref_type="binding_record_ref", path=binding_path),
        _source_ref(ref_type="submit_preflight_decision_ref", path=decision_path),
        _source_ref(ref_type="exposure_to_options_adapter_record_ref", path=adapter_record_path),
    ]
    if prior_identity_path is not None:
        source_refs.append(_source_ref(ref_type="prior_execution_identity_ref", path=prior_identity_path))

    execution_identity = build_execution_identity_record_v1(
        created_at_utc=eval_time_utc,
        day_utc=day_utc,
        attempt_id=attempt_id,
        sleeve_id=sleeve_id,
        environment=environment,
        intent_id=intent_id,
        intent_hash=intent_hash,
        plan_hash=plan_hash,
        binding_hash=binding_hash,
        trade_instance_id=trade_instance_id,
        submission_id=submission_id,
        duplicate_classification=duplicate_classification,
        source_refs=source_refs,
    )
    validate_against_repo_schema_v1(
        execution_identity,
        REPO_ROOT,
        "constellation_2/schemas/execution_identity_record.v1.schema.json",
    )
    output_path = (final_identity_dir / "execution_identity_record.v1.json").resolve()
    _immutable_write_json(output_path, execution_identity)
    return output_path

def _materialize(
    *,
    truth_root: Path,
    day_utc: str,
    eval_time_utc: str,
    intent_path: Path,
    out_day_dir: Path,
) -> Tuple[str, str]:

    intent_obj = _read_json_obj(intent_path)
    intent_hash = _sha256_file(intent_path)

    exposure_type = str(intent_obj.get("exposure_type") or "").strip().upper()
    if exposure_type != "SHORT_VOL_DEFINED":
        raise OptionsIdentityError(f"UNSUPPORTED_OPTIONS_EXPOSURE_TYPE: {exposure_type}")

    underlying = intent_obj.get("underlying")
    if not isinstance(underlying, dict):
        raise OptionsIdentityError("INTENT_UNDERLYING_MISSING")
    symbol = str(underlying.get("symbol") or "").strip().upper()
    if not symbol:
        raise OptionsIdentityError("INTENT_SYMBOL_MISSING")

    try:
        snap_path, cert_path = _find_latest_snapshot_and_cert(truth_root, day_utc, symbol)
    except Exception as e:
        veto_path = _write_failclosed_veto(
            out_day_dir=out_day_dir,
            day_utc=day_utc,
            eval_time_utc=eval_time_utc,
            intent_hash=intent_hash,
            intent_path=intent_path,
            reason_detail=str(e),
        )
        return ("BLOCKED", str(veto_path))

    snap_obj = _read_json_obj(snap_path)
    cert_obj = _read_json_obj(cert_path)
    snap_hash = str(snap_obj.get("canonical_json_hash") or "").strip()
    cert_hash = str(cert_obj.get("canonical_json_hash") or "").strip()
    if not snap_hash:
        raise OptionsIdentityError(f"SNAPSHOT_CANONICAL_HASH_MISSING: {snap_path}")
    if not cert_hash:
        raise OptionsIdentityError(f"FRESHNESS_CANONICAL_HASH_MISSING: {cert_path}")

    tick_size = _derive_tick_size_from_snapshot(snap_obj)

    with tempfile.TemporaryDirectory(prefix=f"phasec_options_{day_utc}_{intent_hash[:12]}_") as td:
        temp_root = Path(td).resolve()
        phasea_root = (temp_root / "phasea_cwd").resolve()
        map_out = (phasea_root / "constellation_2" / "phaseA" / "outputs").resolve()
        run_id = f"phasec_options_{day_utc}_{intent_hash}"
        run_dir = (map_out / run_id).resolve()
        preflight_out = (temp_root / "preflight_out").resolve()
        adapter_out = (temp_root / "adapter_out").resolve()

        map_out.mkdir(parents=True, exist_ok=True)
        adapter_out.mkdir(parents=True, exist_ok=True)

        adapter_cmd = [
            "python3",
            str(EXPOSURE_TO_OPTIONS_ADAPTER_V1),
            "--exposure_intent_path",
            str(intent_path.resolve()),
            "--policy_path",
            str(EXPOSURE_TO_OPTIONS_POLICY_V1),
            "--out_dir",
            str(adapter_out),
            "--produced_utc",
            str(eval_time_utc),
        ]
        res_adapter = _run(adapter_cmd, cwd=REPO_ROOT)
        if res_adapter.returncode != 0:
            veto_path = _write_failclosed_veto(
                out_day_dir=out_day_dir,
                day_utc=day_utc,
                eval_time_utc=eval_time_utc,
                intent_hash=intent_hash,
                intent_path=intent_path,
                reason_detail=(
                    "EXPOSURE_TO_OPTIONS_ADAPTER_FAILED:"
                    f"rc={res_adapter.returncode};"
                    f"stdout={res_adapter.stdout.strip()!r};"
                    f"stderr={res_adapter.stderr.strip()!r}"
                ),
            )
            return ("BLOCKED", str(veto_path))

        options_intent_path = (adapter_out / "options_intent.v2.json").resolve()
        adapter_record_path = (adapter_out / "exposure_to_options_adapter_record.v1.json").resolve()
        if not options_intent_path.exists() or not adapter_record_path.exists():
            veto_path = _write_failclosed_veto(
                out_day_dir=out_day_dir,
                day_utc=day_utc,
                eval_time_utc=eval_time_utc,
                intent_hash=intent_hash,
                intent_path=intent_path,
                reason_detail="EXPOSURE_TO_OPTIONS_ADAPTER_OUTPUT_MISSING",
            )
            return ("BLOCKED", str(veto_path))

        map_cmd = [
            "python3",
            "-m",
            "constellation_2.phaseA.tools.c2_map_vertical_v1",
            "--intent",
            str(options_intent_path),
            "--chain",
            str(snap_path.resolve()),
            "--freshness",
            str(cert_path.resolve()),
            "--now-utc",
            str(eval_time_utc),
            "--tick-size",
            tick_size,
            "--run-id",
            run_id,
        ]
        res_map = _run(map_cmd, cwd=phasea_root)
        if res_map.returncode != 0:
            veto_path = _write_failclosed_veto(
                out_day_dir=out_day_dir,
                day_utc=day_utc,
                eval_time_utc=eval_time_utc,
                intent_hash=intent_hash,
                intent_path=intent_path,
                reason_detail=(
                    "OPTIONS_MAP_FAILED:"
                    f"rc={res_map.returncode};"
                    f"tick_size={tick_size!r};"
                    f"stdout={res_map.stdout.strip()!r};"
                    f"stderr={res_map.stderr.strip()!r}"
                ),
            )
            return ("BLOCKED", str(veto_path))

        order_plan = (run_dir / "order_plan.v1.json").resolve()
        mapping = (run_dir / "mapping_ledger_record.v1.json").resolve()
        if not order_plan.exists() or not mapping.exists():
            veto_path = _write_failclosed_veto(
                out_day_dir=out_day_dir,
                day_utc=day_utc,
                eval_time_utc=eval_time_utc,
                intent_hash=intent_hash,
                intent_path=intent_path,
                reason_detail="OPTIONS_MAP_OUTPUT_MISSING",
            )
            return ("BLOCKED", str(veto_path))

        preflight_cmd = [
            "python3",
            str(PREFLIGHT_OPTIONS_V1),
            "--intent",
            str(options_intent_path),
            "--chain_snapshot",
            str(snap_path.resolve()),
            "--freshness_cert",
            str(cert_path.resolve()),
            "--tick_size",
            tick_size,
            "--eval_time_utc",
            str(eval_time_utc),
            "--out_dir",
            str(preflight_out),
        ]
        res_pf = _run(preflight_cmd, cwd=REPO_ROOT)
        if res_pf.returncode != 0:
            veto_path = _write_failclosed_veto(
                out_day_dir=out_day_dir,
                day_utc=day_utc,
                eval_time_utc=eval_time_utc,
                intent_hash=intent_hash,
                intent_path=intent_path,
                reason_detail=(
                    "OPTIONS_PREFLIGHT_FAILED:"
                    f"rc={res_pf.returncode};"
                    f"stdout={res_pf.stdout.strip()!r};"
                    f"stderr={res_pf.stderr.strip()!r}"
                ),
            )
            return ("BLOCKED", str(veto_path))

        veto = (preflight_out / "veto_record.v1.json").resolve()
        allow = (preflight_out / "submit_preflight_decision.v1.json").resolve()
        binding_v1 = (preflight_out / "binding_record.v1.json").resolve()

        if veto.exists():
            veto_dst = (out_day_dir / f"{intent_hash}.veto_record.v1.json").resolve()
            _immutable_copy(veto, veto_dst)
            return ("BLOCKED", str(veto_dst))

        if not allow.exists() or not binding_v1.exists():
            veto_path = _write_failclosed_veto(
                out_day_dir=out_day_dir,
                day_utc=day_utc,
                eval_time_utc=eval_time_utc,
                intent_hash=intent_hash,
                intent_path=intent_path,
                reason_detail="OPTIONS_PREFLIGHT_OUTPUT_MISSING",
            )
            return ("BLOCKED", str(veto_path))

        final_identity_dir = (out_day_dir / intent_hash).resolve()
        final_identity_dir.mkdir(parents=True, exist_ok=True)

        _immutable_copy(order_plan, final_identity_dir / "order_plan.v1.json")
        _immutable_copy(mapping, final_identity_dir / "mapping_ledger_record.v1.json")
        _immutable_copy(binding_v1, final_identity_dir / "binding_record.v1.json")
        _immutable_copy(allow, final_identity_dir / "submit_preflight_decision.v1.json")
        _immutable_copy(options_intent_path, final_identity_dir / "options_intent.v2.json")
        _immutable_copy(adapter_record_path, final_identity_dir / "exposure_to_options_adapter_record.v1.json")
        try:
            _write_execution_identity_record(
                day_utc=day_utc,
                eval_time_utc=eval_time_utc,
                intent_hash=intent_hash,
                intent_path=intent_path,
                intent_obj=intent_obj,
                out_day_dir=out_day_dir,
                final_identity_dir=final_identity_dir,
                order_plan_path=(final_identity_dir / "order_plan.v1.json").resolve(),
                mapping_path=(final_identity_dir / "mapping_ledger_record.v1.json").resolve(),
                binding_path=(final_identity_dir / "binding_record.v1.json").resolve(),
                decision_path=(final_identity_dir / "submit_preflight_decision.v1.json").resolve(),
                options_intent_path=(final_identity_dir / "options_intent.v2.json").resolve(),
                adapter_record_path=(final_identity_dir / "exposure_to_options_adapter_record.v1.json").resolve(),
            )
        except Exception as e:
            veto_path = _write_failclosed_veto(
                out_day_dir=out_day_dir,
                day_utc=day_utc,
                eval_time_utc=eval_time_utc,
                intent_hash=intent_hash,
                intent_path=intent_path,
                reason_detail=f"OPTIONS_EXECUTION_IDENTITY_FAILED:{e}",
            )
            return ("BLOCKED", str(veto_path))

        allow_dst = (out_day_dir / f"{intent_hash}.submit_preflight_decision.v1.json").resolve()
        _immutable_copy(allow, allow_dst)

        return ("RELEASED", str(final_identity_dir))


def main() -> int:
    ap = argparse.ArgumentParser(description="Materialize same-day Phase C options identities from governed truth.")
    ap.add_argument("--day_utc", required=True)
    ap.add_argument("--eval_time_utc", required=True)
    ap.add_argument("--intent_path", required=True)
    ap.add_argument("--out_day_dir", required=True)
    ap.add_argument("--truth_root", default="", help="Absolute truth root; defaults to C2_TRUTH_ROOT or repo resolver")
    args = ap.parse_args()

    truth_root = _resolve_truth_root(args.truth_root)

    try:
        status, path = _materialize(
            truth_root=truth_root,
            day_utc=str(args.day_utc).strip(),
            eval_time_utc=str(args.eval_time_utc).strip(),
            intent_path=Path(str(args.intent_path)).expanduser().resolve(),
            out_day_dir=Path(str(args.out_day_dir)).expanduser().resolve(),
        )
        print(f"{status}: {path}")
        return 0 if status == "RELEASED" else 2
    except ImmutableWriteError as e:
        print(f"FAIL: IMMUTABLE_WRITE: {e}", file=sys.stderr)
        return 2
    except OptionsIdentityError as e:
        print(f"FAIL: {e}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
