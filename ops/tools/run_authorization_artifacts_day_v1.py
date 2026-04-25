#!/usr/bin/env python3
"""
run_authorization_artifacts_day_v1.py

Bundle A (A1): Per-intent Authorization Artifacts (day-scoped, deterministic, fail-closed).

Inputs:
- allocation_v1/capital_authority_allocation_v1/<DAY>/capital_authority_allocation.v1.json
- intents_v1/snapshots/<DAY>/*.exposure_intent.v1.json
- policy manifest (sha in input manifest only)

Outputs:
- engine_activity_v1/authorization_v1/<DAY>/<INTENT_SHA>.authorization.v1.json

Compatibility note:
- authorization_v1 remains the submit-boundary compatibility projection.
- capital_authority_allocation_v1.decision_chain.authorized_trade_intents is the
  canonical Bundle B authority and must be present.

Truth-root selection order:
  1) --truth_root
  2) C2_TRUTH_ROOT
  3) constellation_2.common.truth_root_v1.resolve_truth_root(repo_root=REPO_ROOT)

Note:
- Uses sha256(file bytes) of intent file as the stable intent_hash reference.
- decision_hash is sha256(canonical JSON of authorization block excluding decision_hash).

Governed same-day freshness behavior:
- existence of day-key output is NOT sufficient to reuse
- existing output may be reused only if its recorded input_manifest matches the current authoritative input_manifest
- if input_manifest differs, existing day-key artifact is stale and must be quarantined then replaced
- if input_manifest is identical but bytes differ, fail closed
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import sys
from pathlib import Path
from typing import Any, Dict, List, Optional

_THIS_FILE = Path(__file__).resolve()
REPO_ROOT = _THIS_FILE.parents[2].resolve()
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from constellation_2.common.truth_root_v1 import resolve_truth_root  # noqa: E402
from constellation_2.common.constitutional_authorization_v1 import build_constitutional_authorization_v1  # noqa: E402
from constellation_2.common.constitutional_authorization_v1 import compare_legacy_authorization_to_constitutional_v1  # noqa: E402
from constellation_2.common.constitutional_decision_v1 import evaluate_constitutional_decision_v1  # noqa: E402
from constellation_2.common.constitutional_proposal_v1 import (  # noqa: E402
    build_exposure_intent_proposal_v1,
    proposal_hash_v1,
)
from constellation_2.common.constitutional_review_resolution_v1 import build_review_packet_from_authorization_artifact_v1  # noqa: E402
from constellation_2.common.paper_session_fact_plane_v1 import (  # noqa: E402
    build_constitutional_fact_bundle_v1,
    build_constitutional_fact_record_v1,
)
from constellation_2.common.runtime_contract_v1 import resolve_release_provenance_release_current_first_v1  # noqa: E402
from constellation_2.phaseD.lib.canon_json_v1 import (  # noqa: E402
    CanonicalizationError,
    canonical_hash_excluding_fields_v1,
    canonical_json_bytes_v1,
)
from constellation_2.phaseD.lib.validate_against_schema_v1 import validate_against_repo_schema_v1  # noqa: E402

SCHEMA_RELPATH = "governance/04_DATA/SCHEMAS/C2/ENGINE_ACTIVITY/authorization.v1.schema.json"
NO_INTENTS_SCHEMA = "governance/04_DATA/SCHEMAS/C2/ENGINE_ACTIVITY/no_intents_day.v1.schema.json"
POLICY_PATH = (REPO_ROOT / "governance/02_REGISTRIES/C2_CAPITAL_AUTHORITY_POLICY_V1.json").resolve()
CONSTITUTIONAL_SHADOW_POLICY_VERSION = "constitutional_shadow_v1"


def _require_truth_root_under_repo(truth_root: Path) -> Path:
    pr = truth_root.expanduser().resolve()
    if not pr.is_absolute():
        raise SystemExit(f"FAIL: truth_root must be absolute: {pr}")
    if not pr.exists() or not pr.is_dir():
        raise SystemExit(f"FAIL: truth_root missing or not dir: {pr}")
    return pr


def _resolve_truth_root(truth_root_arg: str) -> Path:
    arg = (truth_root_arg or "").strip()
    if arg:
        return _require_truth_root_under_repo(Path(arg))
    env_root = (os.environ.get("C2_TRUTH_ROOT") or "").strip()
    if env_root:
        return _require_truth_root_under_repo(Path(env_root))
    return _require_truth_root_under_repo(resolve_truth_root(repo_root=REPO_ROOT))


def _parse_day(day: str) -> str:
    s = (day or "").strip()
    if len(s) != 10 or s[4] != "-" or s[7] != "-":
        raise SystemExit(f"FAIL: BAD_DAY_UTC_FORMAT_EXPECTED_YYYY_MM_DD: {s!r}")
    return s


def _git_sha() -> str:
    try:
        value = str(
            resolve_release_provenance_release_current_first_v1(
                caller="ops/tools/run_authorization_artifacts_day_v1.py"
            ).get("git_sha")
            or ""
        ).strip()
    except Exception:
        value = ""
    if len(value) != 40:
        value = "0" * 40
    return value


def _sha256_file(p: Path) -> str:
    h = hashlib.sha256()
    with p.open("rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def _sha256_bytes(b: bytes) -> str:
    return hashlib.sha256(b).hexdigest()


def _read_json_obj(p: Path) -> Dict[str, Any]:
    with p.open("r", encoding="utf-8") as f:
        o = json.load(f)
    if not isinstance(o, dict):
        raise ValueError(f"TOP_LEVEL_NOT_OBJECT: {str(p)}")
    return o


def _canonical_output_bytes(obj: Dict[str, Any]) -> bytes:
    try:
        return canonical_json_bytes_v1(obj) + b"\n"
    except CanonicalizationError as e:
        raise SystemExit(f"FAIL: CANONICALIZATION_FAILED: {e}") from e


def _canonical_manifest_bytes(items: List[Dict[str, Any]]) -> bytes:
    return (json.dumps(items, sort_keys=True, separators=(",", ":")) + "\n").encode("utf-8")


def _input_manifest_hash_from_obj(obj: Dict[str, Any]) -> str:
    im = obj.get("input_manifest")
    if not isinstance(im, list):
        raise SystemExit("FAIL: INPUT_MANIFEST_MISSING_OR_INVALID")
    return _sha256_bytes(_canonical_manifest_bytes(im))


def _replace_stale_existing(out_path: Path, existing_bytes: bytes, candidate_bytes: bytes) -> str:
    out_path.parent.mkdir(parents=True, exist_ok=True)

    existing_sha = _sha256_bytes(existing_bytes)
    candidate_sha = _sha256_bytes(candidate_bytes)

    quarantine = out_path.with_name(f"{out_path.name}.INVALID_{existing_sha}.json")
    if quarantine.exists():
        if not quarantine.is_file():
            raise SystemExit(f"FAIL: QUARANTINE_PATH_NOT_FILE: {quarantine}")
        q_sha = _sha256_file(quarantine)
        if q_sha != existing_sha:
            raise SystemExit(
                "FAIL: QUARANTINE_SHA_MISMATCH "
                f"path={quarantine} expected_sha={existing_sha} actual_sha={q_sha}"
            )
    else:
        tmp_q = quarantine.with_name(f".{quarantine.name}.tmp.{os.getpid()}")
        tmp_q.write_bytes(existing_bytes)
        fdq = os.open(str(tmp_q), os.O_RDONLY)
        try:
            os.fsync(fdq)
        finally:
            os.close(fdq)
        os.replace(str(tmp_q), str(quarantine))

    tmp = out_path.with_name(f".{out_path.name}.tmp.{os.getpid()}")
    tmp.write_bytes(candidate_bytes)
    fd = os.open(str(tmp), os.O_RDONLY)
    try:
        os.fsync(fd)
    finally:
        os.close(fd)

    os.replace(str(tmp), str(out_path))

    dfd = os.open(str(out_path.parent), os.O_RDONLY)
    try:
        os.fsync(dfd)
    finally:
        os.close(dfd)

    return candidate_sha


def _write_daykey_with_freshness(out_path: Path, out_obj: Dict[str, Any]) -> str:
    candidate_bytes = _canonical_output_bytes(out_obj)
    candidate_sha = _sha256_bytes(candidate_bytes)
    candidate_manifest_hash = _input_manifest_hash_from_obj(out_obj)

    if out_path.exists():
        if not out_path.is_file():
            raise SystemExit(f"FAIL: TARGET_NOT_FILE: {out_path}")

        existing_bytes = out_path.read_bytes()
        existing_sha = _sha256_bytes(existing_bytes)
        existing = _read_json_obj(out_path)

        existing_schema_id = str(existing.get("schema_id") or "").strip()
        existing_schema_version = existing.get("schema_version")
        existing_day = str(existing.get("day_utc") or "").strip()

        if existing_schema_id != "C2_AUTHORIZATION_V1":
            raise SystemExit(
                f"FAIL: EXISTING_SCHEMA_ID_MISMATCH path={out_path} schema_id={existing_schema_id!r}"
            )
        if int(existing_schema_version or 0) != 1:
            raise SystemExit(
                f"FAIL: EXISTING_SCHEMA_VERSION_MISMATCH path={out_path} schema_version={existing_schema_version!r}"
            )
        if existing_day != str(out_obj.get("day_utc") or "").strip():
            raise SystemExit(
                f"FAIL: EXISTING_DAY_MISMATCH path={out_path} existing_day={existing_day!r} "
                f"candidate_day={str(out_obj.get('day_utc') or '').strip()!r}"
            )

        existing_manifest_hash = _input_manifest_hash_from_obj(existing)
        if existing_manifest_hash == candidate_manifest_hash:
            if existing_sha != candidate_sha:
                raise SystemExit(
                    "FAIL: SAME_INPUT_MANIFEST_BUT_DIFFERENT_OUTPUT_BYTES "
                    f"path={out_path} manifest_hash={candidate_manifest_hash} "
                    f"existing_sha={existing_sha} candidate_sha={candidate_sha}"
                )
            print(
                f"OK: AUTHORIZATION_ARTIFACT_WRITTEN day_utc={out_obj['day_utc']} "
                f"path={out_path} sha256={existing_sha} action=EXISTS_IDENTICAL"
            )
            return existing_sha

        wrote_sha = _replace_stale_existing(out_path, existing_bytes, candidate_bytes)
        print(
            f"OK: AUTHORIZATION_ARTIFACT_WRITTEN day_utc={out_obj['day_utc']} "
            f"path={out_path} sha256={wrote_sha} action=REPLACED_STALE existing_sha={existing_sha}"
        )
        return wrote_sha

    out_path.parent.mkdir(parents=True, exist_ok=True)
    tmp = out_path.with_name(f".{out_path.name}.tmp.{os.getpid()}")
    tmp.write_bytes(candidate_bytes)
    fd = os.open(str(tmp), os.O_RDONLY)
    try:
        os.fsync(fd)
    finally:
        os.close(fd)
    os.replace(str(tmp), str(out_path))

    dfd = os.open(str(out_path.parent), os.O_RDONLY)
    try:
        os.fsync(dfd)
    finally:
        os.close(dfd)

    print(
        f"OK: AUTHORIZATION_ARTIFACT_WRITTEN day_utc={out_obj['day_utc']} "
        f"path={out_path} sha256={candidate_sha} action=WROTE"
    )
    return candidate_sha


def _require_authority_head_pass_authoritative(day: str, truth_root: Path) -> Dict[str, Any]:
    authority_head_path = (truth_root / "run_pointer_v2" / "canonical_authority_head.v1.json").resolve()
    if not authority_head_path.exists() or not authority_head_path.is_file():
        raise SystemExit(f"FAIL: AUTHORITY_HEAD_MISSING: {str(authority_head_path)}")
    ah = _read_json_obj(authority_head_path)
    schema_id = str(ah.get("schema_id") or "").strip()
    schema_ver = str(ah.get("schema_version") or "").strip()
    status = str(ah.get("status") or "").strip().upper()
    authoritative = bool(ah.get("authoritative") is True)
    day_utc = str(ah.get("day_utc") or "").strip()

    if schema_id != "c2_run_pointer_canonical_authority_head" or schema_ver != "v1":
        raise SystemExit("FAIL: AUTHORITY_HEAD_SCHEMA_MISMATCH")
    if day_utc != day:
        raise SystemExit(f"FAIL: AUTHORITY_HEAD_DAY_MISMATCH head_day={day_utc!r} expected_day={day!r}")
    if status not in ("PASS", "BOOTSTRAP_PASS"):
        raise SystemExit(f"FAIL: AUTHORITY_HEAD_NOT_EXECUTION_AUTHORIZED status={status!r}")
    if not authoritative:
        raise SystemExit("FAIL: AUTHORITY_HEAD_NOT_AUTHORITATIVE")
    points_to = str(ah.get("points_to") or "").strip()
    if ("authorization_gate_verdict_v1" not in points_to) and ("gate_stack_verdict_v1" not in points_to):
        raise SystemExit("FAIL: AUTHORITY_HEAD_NOT_AUTHORIZATION_OR_GATE_STACK_VERDICT")
    return ah


def _alloc_path(truth_root: Path, day: str) -> Path:
    return (truth_root / "allocation_v1" / "capital_authority_allocation_v1" / day / "capital_authority_allocation.v1.json").resolve()


def _intents_dir(truth_root: Path, day: str) -> Path:
    return (truth_root / "intents_v1" / "snapshots" / day).resolve()


def _out_root(truth_root: Path) -> Path:
    return (truth_root / "engine_activity_v1" / "authorization_v1").resolve()


def _authorization_projection_v1(full_authorization: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "proposal_hash": str(full_authorization.get("proposal_hash") or "").strip().lower(),
        "fact_bundle_hash": str(full_authorization.get("fact_bundle_hash") or "").strip().lower(),
        "policy_version": str(full_authorization.get("policy_version") or "").strip(),
        "effective_scope": dict(full_authorization.get("effective_scope") or {}),
        "decision_enum": str(full_authorization.get("decision_enum") or "").strip().upper(),
        "issued_at": str(full_authorization.get("issued_at") or "").strip(),
        "expires_at": full_authorization.get("expires_at"),
        "authorization_source": str(full_authorization.get("authorization_source") or "").strip().upper(),
    }


def _bundle_b_authorized_rows(alloc_obj: Dict[str, Any]) -> Dict[str, Dict[str, Any]]:
    decision_chain = alloc_obj.get("decision_chain")
    if not isinstance(decision_chain, dict):
        raise SystemExit("FAIL: BUNDLE_B_DECISION_CHAIN_MISSING")
    rows = decision_chain.get("authorized_trade_intents")
    if not isinstance(rows, list):
        raise SystemExit("FAIL: BUNDLE_B_AUTHORIZED_TRADE_INTENTS_MISSING")
    out: Dict[str, Dict[str, Any]] = {}
    for row in rows:
        if not isinstance(row, dict):
            continue
        intent_hash = str(row.get("intent_hash") or "").strip()
        if intent_hash:
            out[intent_hash] = row
    return out


def _append_constraint(constraints: List[str], *, key: str, value: int) -> None:
    constraints.append(f"{str(key).strip()}={int(value)}")


def _headroom_constraints_from_decision(decision_row: Dict[str, Any]) -> List[str]:
    reason_codes = {
        str(code).strip().upper()
        for code in list(decision_row.get("reason_codes") or [])
        if str(code).strip()
    }
    if "BUNDLE_B_HEADROOM_REJECTED" not in reason_codes:
        return []

    constraints: List[str] = []
    field_map = {
        "HEADROOM_REQUESTED_QUANTITY": "requested_quantity",
        "HEADROOM_AUTHORIZED_QUANTITY": "authorized_quantity",
        "HEADROOM_REJECTED_QUANTITY": "rejected_quantity",
        "HEADROOM_RISK_PER_UNIT_CENTS": "risk_per_unit_cents",
        "HEADROOM_REQUIRED_RISK_CENTS": "required_risk_cents",
        "HEADROOM_AVAILABLE_CENTS": "headroom_cents",
        "HEADROOM_ALLOWED_CAPITAL_AT_RISK_CENTS": "allowed_capital_at_risk_cents",
        "HEADROOM_AVAILABLE_SLEEVE_CENTS": "available_sleeve_headroom_cents",
        "HEADROOM_AVAILABLE_PORTFOLIO_CENTS": "available_portfolio_headroom_cents",
    }
    for key, field_name in field_map.items():
        raw = decision_row.get(field_name)
        if isinstance(raw, int):
            _append_constraint(constraints, key=key, value=int(raw))
    return constraints


def _select_effective_intents(intents_dir: Path) -> List[Path]:
    # Same-day corrected snapshots may coexist with stale prior snapshots for the
    # same intent_id. Keep exactly one effective file per intent_id using only
    # canonical artifact content: later created_at_utc wins; ties fall back to
    # file sha256, then filename. Never depend on filesystem mtime.
    by_intent_id: Dict[str, tuple[str, str, str, Path]] = {}
    passthrough: List[Path] = []
    for p in sorted(
        [
            p for p in intents_dir.iterdir()
            if p.is_file()
            and p.name.endswith(".json")
            and p.name != "no_intents_day.v1.json"
        ],
        key=lambda p: p.name,
    ):
        try:
            obj = _read_json_obj(p)
        except Exception:
            passthrough.append(p)
            continue
        intent_id = str(obj.get("intent_id") or "").strip()
        if not intent_id:
            passthrough.append(p)
            continue
        created_at_utc = str(obj.get("created_at_utc") or "").strip()
        candidate = (created_at_utc, _sha256_file(p), p.name, p)
        prior = by_intent_id.get(intent_id)
        if prior is None or candidate[:3] >= prior[:3]:
            by_intent_id[intent_id] = candidate
    return sorted(passthrough + [item[3] for item in by_intent_id.values()], key=lambda p: p.name)


def main(argv: Optional[List[str]] = None) -> int:
    ap = argparse.ArgumentParser(prog="run_authorization_artifacts_day_v1")
    ap.add_argument("--day_utc", required=True)
    ap.add_argument("--truth_root", default="", help="Absolute truth root; defaults to C2_TRUTH_ROOT or repo resolver")
    args = ap.parse_args(argv)

    day = _parse_day(args.day_utc)
    produced_utc = f"{day}T00:00:00Z"
    truth_root = _resolve_truth_root(args.truth_root)

    _require_authority_head_pass_authoritative(day, truth_root)

    p_alloc = _alloc_path(truth_root, day)
    if not p_alloc.exists():
        raise SystemExit(f"FAIL: ALLOCATION_AUTHORITY_MISSING: {str(p_alloc)}")
    alloc_sha = _sha256_file(p_alloc)
    alloc_obj = _read_json_obj(p_alloc)

    intents_dir = _intents_dir(truth_root, day)
    if not intents_dir.exists() or not intents_dir.is_dir():
        raise SystemExit(f"FAIL: INTENTS_DAY_DIR_MISSING: {str(intents_dir)}")
    intent_files = _select_effective_intents(intents_dir)

    marker_path = (intents_dir / "no_intents_day.v1.json").resolve()

    if not intent_files:
        if marker_path.exists():
            obj = _read_json_obj(marker_path)
            validate_against_repo_schema_v1(obj, REPO_ROOT, NO_INTENTS_SCHEMA)
            print(f"OK: NO_INTENTS_FOR_DAY_MARKER_PRESENT day_utc={day} path={str(marker_path)}")
            return 0
        raise SystemExit("FAIL: NO_INTENTS_FOR_DAY")

    pol_sha = _sha256_file(POLICY_PATH) if POLICY_PATH.exists() else "0" * 64
    producer_git_sha = _git_sha()
    producer_git_sha_hash = _sha256_bytes((producer_git_sha + "\n").encode("utf-8"))

    decisions = _bundle_b_authorized_rows(alloc_obj)

    out_day_dir = (_out_root(truth_root) / day).resolve()
    out_day_dir.mkdir(parents=True, exist_ok=True)

    wrote = 0
    constitutional_decision_counts: Dict[str, int] = {}
    constitutional_missing_fact_count = 0
    constitutional_dependency_degradation_count = 0
    constitutional_mismatch_count = 0
    constitutional_review_required_count = 0
    for p in intent_files:
        intent_obj = _read_json_obj(p)
        engine_id = str(((intent_obj.get("engine") or {}).get("engine_id") or "")).strip()
        intent_id = str(intent_obj.get("intent_id") or "").strip()
        if not engine_id or not intent_id:
            raise SystemExit(f"FAIL: INTENT_MISSING_ENGINE_OR_ID: {str(p)}")

        intent_sha = _sha256_file(p)
        dec = decisions.get(intent_sha, None)
        if not isinstance(dec, dict):
            raise SystemExit(f"FAIL: ALLOCATION_MISSING_INTENT_HASH: {intent_sha} file={str(p)}")

        authorization_outcome = str(dec.get("authorization_outcome") or "").strip().upper()
        auth_qty = int(dec.get("authorized_quantity") or 0)
        rc = list(dec.get("reason_codes") or ["CAPAUTH_REJECTED", "CAPAUTH_FAIL_CLOSED_REQUIRED"])
        decision = "AUTHORIZED" if auth_qty > 0 else "REJECTED"
        if authorization_outcome == "RESIZED" and auth_qty > 0:
            rc = sorted({*rc, "BUNDLE_B_RESIZED"})
        status = "AUTHORIZED" if auth_qty > 0 and authorization_outcome in {"APPROVED", "RESIZED"} else "REJECTED"

        headroom_constraints = _headroom_constraints_from_decision(dec)
        auth_block: Dict[str, Any] = {
            "decision": decision,
            "authorized_quantity": int(auth_qty),
            "constraints": list(headroom_constraints),
            "decision_hash": None,
        }
        auth_block["decision_hash"] = canonical_hash_excluding_fields_v1(auth_block, fields=("decision_hash",))
        proposal = build_exposure_intent_proposal_v1(
            day_utc=day,
            intent_obj=intent_obj,
            intent_path=p,
            intent_hash=intent_sha,
            policy_path=POLICY_PATH,
            policy_hash=pol_sha,
        )
        proposal_hash = proposal_hash_v1(proposal)
        allocation_fact = build_constitutional_fact_record_v1(
            fact_type="account_state_fact",
            source_system="capital_authority_allocation_v1",
            source_version=str(alloc_obj.get("schema_version") or "v1").strip(),
            observed_at=str(alloc_obj.get("produced_utc") or produced_utc).strip(),
            captured_at=produced_utc,
            freshness_class="CURRENT",
            provenance_class="AUTHORITATIVE_FILE",
            payload={
                "decision": decision,
                "authorized_quantity": int(auth_qty),
                "reason_codes": sorted({str(code).strip() for code in rc if str(code).strip()}),
                "authorization_outcome": authorization_outcome,
            },
            scope_keys={"day_utc": day, "intent_id": intent_id, "engine_id": engine_id},
            content_hash=alloc_sha,
            general_admissibility="VERIFIED_COMPLETE",
            tax_admissibility="ESTIMATED_POSITION_LEVEL",
            dependency_health="HEALTHY" if auth_qty >= 0 else "DEGRADED_BLOCKING",
            state_coherence="COHERENT",
            logical_name="capital_authority_allocation_v1",
            artifact_path=str(p_alloc),
        )
        policy_fact = build_constitutional_fact_record_v1(
            fact_type="policy_binding_fact",
            source_system="governance_registry",
            source_version="v1",
            observed_at=produced_utc,
            captured_at=produced_utc,
            freshness_class="CURRENT" if POLICY_PATH.exists() else "UNKNOWN",
            provenance_class="REGISTRY_BOUND",
            payload={"policy_path": str(POLICY_PATH), "policy_sha256": pol_sha},
            scope_keys={"day_utc": day, "intent_id": intent_id},
            content_hash=pol_sha,
            general_admissibility="VERIFIED_COMPLETE" if POLICY_PATH.exists() else "UNAVAILABLE",
            tax_admissibility="ESTIMATED_POSITION_LEVEL",
            dependency_health="HEALTHY" if POLICY_PATH.exists() else "UNAVAILABLE",
            state_coherence="COHERENT" if POLICY_PATH.exists() else "UNKNOWN",
            logical_name="capital_authority_policy_v1",
            artifact_path=str(POLICY_PATH),
        )
        execution_fact = build_constitutional_fact_record_v1(
            fact_type="execution_capability_fact",
            source_system="authorization_artifacts_day_v1",
            source_version="v1",
            observed_at=produced_utc,
            captured_at=produced_utc,
            freshness_class="CURRENT",
            provenance_class="DERIVED_FROM_AUTHORITATIVE_FILES",
            payload={
                "intent_hash": intent_sha,
                "allocation_present": True,
                "status": status,
            },
            scope_keys={"day_utc": day, "intent_id": intent_id, "engine_id": engine_id},
            content_hash=_sha256_bytes(
                canonical_json_bytes_v1({"intent_hash": intent_sha, "status": status, "authorized_quantity": int(auth_qty)})
            ),
            general_admissibility="VERIFIED_COMPLETE",
            tax_admissibility="ESTIMATED_POSITION_LEVEL",
            dependency_health="HEALTHY" if status == "AUTHORIZED" else "DEGRADED_NON_BLOCKING",
            state_coherence="COHERENT",
            logical_name="execution_capability_shadow_v1",
            artifact_path="engine_activity_v1/authorization_v1",
        )
        fact_bundle = build_constitutional_fact_bundle_v1(
            day_utc=day,
            session_id=intent_id,
            policy_version=CONSTITUTIONAL_SHADOW_POLICY_VERSION,
            required_fact_types=list(proposal.get("required_fact_types") or []),
            fact_records=[allocation_fact, policy_fact, execution_fact],
        )
        constitutional_decision = evaluate_constitutional_decision_v1(
            proposal=proposal,
            proposal_hash=proposal_hash,
            fact_bundle=fact_bundle,
            fact_bundle_hash=str(fact_bundle.get("fact_bundle_hash") or "").strip(),
            policy_version=CONSTITUTIONAL_SHADOW_POLICY_VERSION,
            scope_authorities={
                "global": "REQUIRE_HUMAN_REVIEW",
                "domain": "REQUIRE_HUMAN_REVIEW",
                "account": "REQUIRE_HUMAN_REVIEW",
                "sleeve": "REQUIRE_HUMAN_REVIEW",
                "action_class": "REQUIRE_HUMAN_REVIEW",
            },
            hard_envelope_ok=bool(status == "AUTHORIZED" and auth_qty > 0),
            policy_blockers=[] if status == "AUTHORIZED" and auth_qty > 0 else list(rc),
            persistence_ok=True,
            evaluated_at=produced_utc,
        )
        constitutional_authorization = build_constitutional_authorization_v1(
            proposal_hash=proposal_hash,
            fact_bundle_hash=str(fact_bundle.get("fact_bundle_hash") or "").strip(),
            policy_version=CONSTITUTIONAL_SHADOW_POLICY_VERSION,
            effective_scope=dict(constitutional_decision.get("effective_scope") or {}),
            decision_enum=str(constitutional_decision.get("decision_enum") or "").strip(),
            issued_at=produced_utc,
            expires_at=f"{day}T23:59:59Z",
            issuer_identity={
                "issuer": "run_authorization_artifacts_day_v1",
                "producer_module": "ops/tools/run_authorization_artifacts_day_v1.py",
                "git_sha": producer_git_sha,
            },
        )
        constitutional_authorization_projection = _authorization_projection_v1(constitutional_authorization)
        legacy_constitutional_comparison = compare_legacy_authorization_to_constitutional_v1(
            legacy_status=status,
            legacy_decision=decision,
            legacy_authorized_quantity=int(auth_qty),
            constitutional_decision_enum=str(constitutional_decision.get("decision_enum") or "").strip(),
            constitutional_authorization_issuable=bool(constitutional_decision.get("authorization_issuable") is True),
        )
        constitutional_decision_counts[str(constitutional_decision.get("decision_enum") or "").strip()] = (
            constitutional_decision_counts.get(str(constitutional_decision.get("decision_enum") or "").strip(), 0) + 1
        )
        negative_evidence_rows = list(constitutional_decision.get("negative_evidence") or [])
        constitutional_missing_fact_count += sum(
            1 for row in negative_evidence_rows
            if isinstance(row, dict) and str(row.get("type") or "").strip() == "MISSING_FACT"
        )
        constitutional_dependency_degradation_count += sum(
            1
            for row in (fact_bundle.get("fact_records") or [])
            if isinstance(row, dict)
            and str(row.get("dependency_health") or "").strip().upper() in {"DEGRADED_NON_BLOCKING", "DEGRADED_BLOCKING", "UNAVAILABLE"}
        )
        if str(legacy_constitutional_comparison.get("comparison_status") or "").strip() == "MISMATCH":
            constitutional_mismatch_count += 1

        out_obj: Dict[str, Any] = {
            "schema_id": "C2_AUTHORIZATION_V1",
            "schema_version": 1,
            "produced_utc": produced_utc,
            "day_utc": day,
            "producer": {
                "repo": "constellation_2_runtime",
                "git_sha": producer_git_sha,
                "module": "ops/tools/run_authorization_artifacts_day_v1.py",
            },
            "status": status,
            "reason_codes": rc,
            "input_manifest": [
                {"type": "intent", "path": str(p.resolve()), "sha256": intent_sha, "day_utc": day, "producer": "intents_v1"},
                {"type": "capital_authority_allocation", "path": str(p_alloc), "sha256": alloc_sha, "day_utc": day, "producer": "allocation_v1"},
                {"type": "policy_manifest", "path": str(POLICY_PATH), "sha256": pol_sha, "day_utc": None, "producer": "governance"},
                {"type": "other", "path": "git:HEAD", "sha256": producer_git_sha_hash, "day_utc": None, "producer": "git"},
            ],
            "engine_id": engine_id,
            "intent_id": intent_id,
            "intent_hash": intent_sha,
            "proposal_hash": proposal_hash,
            "fact_bundle_hash": str(fact_bundle.get("fact_bundle_hash") or "").strip(),
            "decision_enum": str(constitutional_decision.get("decision_enum") or "").strip(),
            "effective_scope": dict(constitutional_decision.get("effective_scope") or {}),
            "issued_at": str(constitutional_authorization.get("issued_at") or "").strip(),
            "expires_at": constitutional_authorization.get("expires_at"),
            "authorization": auth_block,
            "legacy_constitutional_comparison": legacy_constitutional_comparison,
            "constitutional_authorization": constitutional_authorization_projection,
            "constitutional_shadow": {
                "policy_version": CONSTITUTIONAL_SHADOW_POLICY_VERSION,
                "proposal": proposal,
                "fact_bundle": fact_bundle,
                "decision": constitutional_decision,
                "constitutional_authorization": constitutional_authorization,
                "legacy_constitutional_comparison": legacy_constitutional_comparison,
            },
        }
        if str(constitutional_decision.get("decision_enum") or "").strip().upper() == "REQUIRE_HUMAN_REVIEW":
            constitutional_review_required_count += 1
            out_obj["constitutional_shadow"]["review_packet"] = build_review_packet_from_authorization_artifact_v1(out_obj)

        validate_against_repo_schema_v1(out_obj, REPO_ROOT, SCHEMA_RELPATH)

        out_path = (out_day_dir / f"{intent_sha}.authorization.v1.json").resolve()
        _write_daykey_with_freshness(out_path, out_obj)
        wrote += 1

    print(
        json.dumps(
            {
                "day_utc": day,
                "wrote": wrote,
                "out_dir": str(out_day_dir),
                "constitutional_shadow_metrics": {
                    "proposal_count": wrote,
                    "decision_counts": dict(sorted(constitutional_decision_counts.items())),
                    "missing_fact_count": constitutional_missing_fact_count,
                    "dependency_degradation_count": constitutional_dependency_degradation_count,
                    "legacy_constitutional_mismatch_count": constitutional_mismatch_count,
                    "review_required_count": constitutional_review_required_count,
                },
            },
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
