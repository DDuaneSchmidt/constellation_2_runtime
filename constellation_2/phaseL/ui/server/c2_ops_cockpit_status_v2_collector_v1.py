#!/usr/bin/env python3
"""
Constellation 2.0 — Ops Cockpit UI V2 — Status Collector (Read-Only, Deterministic)

Contract:
- Reads ONLY canonical truth artifacts under constellation_2/runtime/truth and instance config JSON (if present).
- Produces a single deterministic payload for Operations + Engines.
- Fail-closed: missing/parse errors are explicit, never inferred.
- No nondeterministic ordering: all lists are sorted by stable keys.
- No trading logic changes.
"""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple


# -------------------------
# Deterministic helpers
# -------------------------

def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _is_day_str(s: str) -> bool:
    try:
        datetime.strptime(s, "%Y-%m-%d")
        return True
    except Exception:
        return False


def _safe_read_json(path: Path) -> Tuple[Optional[Any], Optional[str]]:
    try:
        with path.open("r", encoding="utf-8") as f:
            return json.load(f), None
    except FileNotFoundError:
        return None, "FILE_NOT_FOUND"
    except json.JSONDecodeError:
        return None, "JSON_DECODE_ERROR"
    except Exception:
        return None, "READ_ERROR"


def _mtime(path: Path) -> Optional[float]:
    try:
        return path.stat().st_mtime
    except Exception:
        return None


def _sha256_bytes(b: bytes) -> str:
    return hashlib.sha256(b).hexdigest()


def _sha256_file(path: Path) -> Optional[str]:
    try:
        return _sha256_bytes(path.read_bytes())
    except Exception:
        return None


def _stable_json_bytes(obj: Any) -> bytes:
    return json.dumps(obj, sort_keys=True, separators=(",", ":"), ensure_ascii=False).encode("utf-8")


def _coerce_state(s: Optional[str]) -> str:
    if not isinstance(s, str) or not s:
        return "UNKNOWN"
    u = s.upper()
    if u in ("PASS", "DEGRADED", "FAIL", "ABORTED", "UNKNOWN", "MISSING"):
        return u
    if u in ("OK", "PRESENT", "SUCCESS"):
        return "PASS"
    return "UNKNOWN"


def _top2_reason_codes(x: Any) -> List[str]:
    if isinstance(x, list):
        out = [str(a) for a in x if isinstance(a, (str, int, float)) and str(a).strip()]
        return out[:2]
    return []


THIS_FILE = Path(__file__).resolve()
REPO_ROOT = THIS_FILE.parents[4]
SLEEVE_POLICY_REGISTRY = (REPO_ROOT / "governance/02_REGISTRIES/C2_CAPITAL_AUTHORITY_POLICY_V1.json").resolve()
IB_ACCOUNT_REGISTRY = (REPO_ROOT / "governance/02_REGISTRIES/C2_IB_ACCOUNT_REGISTRY_V1.json").resolve()


# -------------------------
# Tile model
# -------------------------

@dataclass(frozen=True)
class Tile:
    tile_id: str
    state: str
    last_updated_utc: Optional[str]
    reason_codes: List[str]
    reason_human: List[str]
    artifact_path: Optional[str]
    artifact_sha256: Optional[str]


def _tile_dict(t: Tile) -> Dict[str, Any]:
    return {
        "tile_id": t.tile_id,
        "state": t.state,
        "last_updated_utc": t.last_updated_utc,
        "reason_codes": t.reason_codes,
        "reason_human": t.reason_human,
        "artifact_ref": {
            "path": t.artifact_path,
            "sha256": t.artifact_sha256,
        },
    }


# -------------------------
# Candidate roots
# -------------------------

def _candidate_replay_roots(truth_root: Path) -> List[Path]:
    return [
        (truth_root / "reports" / "replay_certification_gate_v1"),
        (truth_root / "reports" / "replay_certification_bundle_v1"),
        (truth_root / "reports" / "replay_integrity_v2"),
        (truth_root / "reports" / "replay_integrity_day_v2"),
        (truth_root / "reports" / "replay_integrity_day_v1"),
    ]


# -------------------------
# Attempts (V2) discovery
# -------------------------

def discover_attempts(truth_root: Path, day: str) -> Tuple[List[str], List[str], List[str], Dict[str, float], List[str]]:
    """
    Deterministically list orchestrator v2 attempt directories for day.
    Filters to canonical v2 attempt ids that include "__A" (A0001 etc).
    """
    missing: List[str] = []
    source_paths: List[str] = []
    source_mtimes: Dict[str, float] = {}
    warnings: List[str] = []

    attempts: List[str] = []

    v2_day_dir = (truth_root / "reports" / "orchestrator_run_verdict_v2" / day).resolve()
    if not v2_day_dir.exists() or not v2_day_dir.is_dir():
        missing.append(str(v2_day_dir))
        warnings.append("ATTEMPTS_NOT_FOUND")
        return [], missing, [], {}, sorted(set(warnings))

    source_paths.append(str(v2_day_dir))
    mt = _mtime(v2_day_dir)
    if mt is not None:
        source_mtimes[str(v2_day_dir)] = mt

    for p in sorted([x for x in v2_day_dir.iterdir() if x.is_dir()], key=lambda x: x.name):
        name = p.name.strip()
        if name and "__A" in name:
            attempts.append(name)

    attempts = sorted(set(attempts))
    if not attempts:
        warnings.append("ATTEMPTS_NOT_FOUND")

    return attempts, sorted(set(missing)), sorted(set(source_paths)), source_mtimes, sorted(set(warnings))


def select_latest_attempt(attempts: List[str]) -> Optional[str]:
    return attempts[-1] if attempts else None


def _load_attempt_verdict(truth_root: Path, day: str, attempt_id: str) -> Optional[Dict[str, Any]]:
    p = (truth_root / "reports" / "orchestrator_run_verdict_v2" / day / attempt_id / "orchestrator_run_verdict.v2.json").resolve()
    obj, _err = _safe_read_json(p)
    if isinstance(obj, dict):
        return obj
    return None


def select_preferred_attempt(truth_root: Path, day: str, attempts: List[str]) -> Optional[str]:
    """
    Default attempt selection for auto mode:
    - Use latest attempt normally.
    - If latest is ABORTED but there is an earlier PASS on the same day,
      prefer the latest PASS so canonical healthy proof is not masked by a
      later aborted rerun.
    """
    latest = select_latest_attempt(attempts)
    if not isinstance(latest, str) or not latest:
        return None

    latest_doc = _load_attempt_verdict(truth_root, day, latest)
    latest_status = _coerce_state(str(latest_doc.get("status") or latest_doc.get("state") or "UNKNOWN")) if isinstance(latest_doc, dict) else "UNKNOWN"
    if latest_status != "ABORTED":
        return latest

    pass_attempts: List[Tuple[int, str]] = []
    for aid in attempts:
        doc = _load_attempt_verdict(truth_root, day, aid)
        if not isinstance(doc, dict):
            continue
        st = _coerce_state(str(doc.get("status") or doc.get("state") or "UNKNOWN"))
        if st != "PASS":
            continue
        seq = doc.get("attempt_seq")
        seq_i = int(seq) if isinstance(seq, int) else -1
        pass_attempts.append((seq_i, aid))
    if not pass_attempts:
        return latest

    pass_attempts.sort(key=lambda x: (x[0], x[1]))
    return pass_attempts[-1][1]


# -------------------------
# Orchestrator attempt mode/account
# -------------------------

def _attempt_mode_and_account(truth_root: Path, day: str, attempt_id: Optional[str]) -> Tuple[Optional[str], Optional[str], List[str], List[str]]:
    """
    Reads orchestrator_attempt_manifest.v2.json for selected attempt.
    Returns (mode, ib_account, warnings, missing_paths).
    """
    warnings: List[str] = []
    missing: List[str] = []

    if not isinstance(attempt_id, str) or not attempt_id.strip():
        warnings.append("ATTEMPT_ID_MISSING_FOR_MODE_ACCOUNT")
        return None, None, warnings, missing

    p = (truth_root / "reports" / "orchestrator_run_verdict_v2" / day / attempt_id.strip() / "orchestrator_attempt_manifest.v2.json").resolve()
    if not p.exists():
        missing.append(str(p))
        warnings.append("ATTEMPT_MANIFEST_MISSING_FOR_MODE_ACCOUNT")
        return None, None, warnings, missing

    obj, err = _safe_read_json(p)
    if not isinstance(obj, dict):
        warnings.append(f"ATTEMPT_MANIFEST_UNREADABLE:{err}")
        return None, None, warnings, missing

    mode_raw = obj.get("mode")
    acct_raw = obj.get("ib_account")

    mode = str(mode_raw).upper().strip() if isinstance(mode_raw, str) else None
    acct = str(acct_raw).strip() if isinstance(acct_raw, str) else None

    if mode not in ("PAPER", "LIVE"):
        warnings.append("ATTEMPT_MANIFEST_MODE_INVALID")
        mode = None
    if not acct:
        warnings.append("ATTEMPT_MANIFEST_IB_ACCOUNT_MISSING")
        acct = None

    return mode, acct, warnings, missing


def _attempt_stage_status(doc: Optional[Dict[str, Any]], stage_id: str) -> Optional[str]:
    if not isinstance(doc, dict):
        return None
    stages = doc.get("stages")
    if not isinstance(stages, list):
        return None
    for s in stages:
        if not isinstance(s, dict):
            continue
        if str(s.get("stage_id") or "") != stage_id:
            continue
        st = s.get("status")
        if isinstance(st, str) and st.strip():
            return st.strip().upper()
        return None
    return None


def _load_selected_run_verdict_doc(run_tile: Optional[Tile]) -> Optional[Dict[str, Any]]:
    if run_tile is None or not isinstance(run_tile.artifact_path, str) or not run_tile.artifact_path:
        return None
    obj, _err = _safe_read_json(Path(run_tile.artifact_path))
    if isinstance(obj, dict):
        return obj
    return None


# -------------------------
# Tile readers
# -------------------------

def _parse_simple_gate_tile(path: Path, tile_id: str) -> Tuple[Tile, List[str], List[str]]:
    """
    Deterministic read of a single gate artifact.
    Returns (tile, warnings, missing_paths)
    """
    warnings: List[str] = []
    missing: List[str] = []

    if not path.exists():
        missing.append(str(path))
        return Tile(
            tile_id=tile_id,
            state="MISSING",
            last_updated_utc=None,
            reason_codes=["MISSING_GATE_ARTIFACT"],
            reason_human=[],
            artifact_path=str(path),
            artifact_sha256=None,
        ), warnings, missing

    obj, err = _safe_read_json(path)
    if not isinstance(obj, dict):
        warnings.append(f"GATE_UNREADABLE:{err}")
        return Tile(
            tile_id=tile_id,
            state="UNKNOWN",
            last_updated_utc=None,
            reason_codes=[f"GATE_UNREADABLE:{err}"],
            reason_human=[],
            artifact_path=str(path),
            artifact_sha256=_sha256_file(path),
        ), warnings, missing

    # Common fields
    st = obj.get("state") or obj.get("status") or obj.get("verdict") or obj.get("run_verdict")
    if isinstance(st, dict):
        st = st.get("state") or st.get("status") or st.get("run_verdict")
    state = _coerce_state(str(st) if st is not None else "UNKNOWN")

    rc = obj.get("reason_codes") or obj.get("reason_codes_top") or []
    if isinstance(obj.get("verdict"), dict) and not rc:
        v = obj.get("verdict")
        rc = v.get("reason_codes") or v.get("reason_codes_top") or []

    last = obj.get("generated_at_utc") or obj.get("generated_utc") or obj.get("produced_utc") or obj.get("asof_utc") or None

    return Tile(
        tile_id=tile_id,
        state=state,
        last_updated_utc=str(last) if isinstance(last, str) and last else None,
        reason_codes=_top2_reason_codes(rc),
        reason_human=[],
        artifact_path=str(path),
        artifact_sha256=_sha256_file(path),
    ), warnings, missing


def _parse_gate_stack_verdict_tile(truth_root: Path, day: str) -> Tuple[Optional[Tile], List[str], List[str], Dict[str, float], List[str]]:
    missing: List[str] = []
    source_paths: List[str] = []
    source_mtimes: Dict[str, float] = {}
    warnings: List[str] = []

    p = (truth_root / "reports" / "gate_stack_verdict_v1" / day / "gate_stack_verdict.v1.json").resolve()
    if not p.exists():
        missing.append(str(p))
        warnings.append("GATE_STACK_VERDICT_MISSING")
        return None, missing, [], {}, warnings

    obj, err = _safe_read_json(p)
    source_paths.append(str(p))
    mt = _mtime(p)
    if mt is not None:
        source_mtimes[str(p)] = mt

    if not isinstance(obj, dict):
        warnings.append(f"GATE_STACK_UNREADABLE:{err}")
        return None, missing, source_paths, source_mtimes, warnings

    v = obj.get("verdict") if isinstance(obj.get("verdict"), dict) else obj
    state = _coerce_state(str(v.get("state") or v.get("overall_state") or "UNKNOWN"))
    rc = v.get("reason_codes_top") or v.get("reason_codes") or []
    last = obj.get("generated_at_utc") or obj.get("generated_utc") or obj.get("asof_utc") or None

    tile = Tile(
        tile_id="gate_stack_verdict_v1",
        state=state,
        last_updated_utc=str(last) if isinstance(last, str) and last else None,
        reason_codes=_top2_reason_codes(rc),
        reason_human=[],
        artifact_path=str(p),
        artifact_sha256=_sha256_file(p),
    )
    return tile, sorted(set(missing)), sorted(set(source_paths)), source_mtimes, sorted(set(warnings))


def _parse_orchestrator_run_verdict_v2(truth_root: Path, day: str, attempt_id: Optional[str]) -> Tuple[Optional[Tile], List[str], List[str], Dict[str, float], List[str]]:
    """
    Deterministic best-of selection:
    - Prefer attempt-scoped verdict file under .../day/<attempt_id>/orchestrator_run_verdict.v2.json
    - Else search day directory for any orchestrator_run_verdict.v2.json and choose best by:
      (has_attempt_fields, attempt_seq, produced_utc, path)
    """
    missing: List[str] = []
    source_paths: List[str] = []
    source_mtimes: Dict[str, float] = {}
    warnings: List[str] = []

    day_dir = (truth_root / "reports" / "orchestrator_run_verdict_v2" / day).resolve()
    if not day_dir.exists() or not day_dir.is_dir():
        missing.append(str(day_dir))
        warnings.append("RUN_VERDICT_NOT_FOUND")
        return None, missing, [], {}, warnings

    # 1) exact attempt file if attempt_id present
    if isinstance(attempt_id, str) and attempt_id.strip():
        p = (day_dir / attempt_id.strip() / "orchestrator_run_verdict.v2.json").resolve()
        if p.exists():
            obj, err = _safe_read_json(p)
            source_paths.append(str(p))
            mt = _mtime(p)
            if mt is not None:
                source_mtimes[str(p)] = mt
            if isinstance(obj, dict):
                state = _coerce_state(str(obj.get("status") or obj.get("state") or "UNKNOWN"))
                rc = obj.get("reason_codes") or obj.get("reason_codes_top") or []
                last = obj.get("run_completed_utc") or obj.get("produced_utc") or obj.get("generated_at_utc") or obj.get("generated_utc") or None
                tile = Tile(
                    tile_id="orchestrator_run_verdict_v2",
                    state=state,
                    last_updated_utc=str(last) if isinstance(last, str) and last else None,
                    reason_codes=_top2_reason_codes(rc),
                    reason_human=[],
                    artifact_path=str(p),
                    artifact_sha256=_sha256_file(p),
                )
                return tile, sorted(set(missing)), sorted(set(source_paths)), source_mtimes, sorted(set(warnings))
            warnings.append(f"RUN_VERDICT_UNREADABLE:{err}")

    # 2) scan all attempt dirs for verdict files
    candidates: List[Path] = []
    for d in sorted([x for x in day_dir.iterdir() if x.is_dir()], key=lambda x: x.name):
        fp = (d / "orchestrator_run_verdict.v2.json").resolve()
        if fp.exists():
            candidates.append(fp)

    candidates = sorted(set(candidates), key=lambda p: str(p))

    best_tile: Optional[Tile] = None
    best_key: Optional[Tuple[int, int, str, str]] = None

    for p in candidates:
        obj, err = _safe_read_json(p)
        source_paths.append(str(p))
        mt = _mtime(p)
        if mt is not None:
            source_mtimes[str(p)] = mt

        if not isinstance(obj, dict):
            warnings.append(f"RUN_VERDICT_UNREADABLE:{p}:{err}")
            continue

        attempt_seq = obj.get("attempt_seq")
        attempt_id2 = obj.get("attempt_id")
        produced2 = obj.get("produced_utc")

        has_attempt = isinstance(attempt_seq, int) and attempt_seq > 0 and isinstance(attempt_id2, str) and attempt_id2.strip()
        produced_s = produced2 if isinstance(produced2, str) else ""

        key = (
            1 if has_attempt else 0,
            int(attempt_seq) if isinstance(attempt_seq, int) else -1,
            produced_s,
            str(p),
        )

        state = _coerce_state(str(obj.get("status") or obj.get("state") or "UNKNOWN"))
        rc = obj.get("reason_codes") or obj.get("reason_codes_top") or []
        last = obj.get("run_completed_utc") or obj.get("produced_utc") or obj.get("generated_at_utc") or obj.get("generated_utc") or None

        tile = Tile(
            tile_id="orchestrator_run_verdict_v2",
            state=state,
            last_updated_utc=str(last) if isinstance(last, str) and last else None,
            reason_codes=_top2_reason_codes(rc),
            reason_human=[],
            artifact_path=str(p),
            artifact_sha256=_sha256_file(p),
        )

        if best_key is None or key > best_key:
            best_key = key
            best_tile = tile

    if best_tile is None:
        warnings.append("RUN_VERDICT_NOT_FOUND")
    return best_tile, sorted(set(missing)), sorted(set(source_paths)), source_mtimes, sorted(set(warnings))


def _parse_replay_tile(truth_root: Path, day: str, attempt_id: Optional[str]) -> Tuple[Optional[Tile], List[str], List[str], Dict[str, float], List[str]]:
    missing: List[str] = []
    source_paths: List[str] = []
    source_mtimes: Dict[str, float] = {}
    warnings: List[str] = []

    roots = _candidate_replay_roots(truth_root)
    candidates: List[Path] = []

    for r in roots:
        ddir = (r / day).resolve()
        if not ddir.exists() or not ddir.is_dir():
            missing.append(str(ddir))
            continue

        # attempt-scoped files first
        if isinstance(attempt_id, str) and attempt_id.strip():
            adir = (ddir / attempt_id.strip()).resolve()
            if adir.exists() and adir.is_dir():
                candidates.extend(sorted([p for p in adir.iterdir() if p.is_file() and p.suffix == ".json"], key=lambda p: p.name))

        candidates.extend(sorted([p for p in ddir.iterdir() if p.is_file() and p.suffix == ".json"], key=lambda p: p.name))

    # deterministic unique
    uniq: List[Path] = []
    seen = set()
    for p in candidates:
        sp = str(p)
        if not p.exists() or sp in seen:
            continue
        seen.add(sp)
        uniq.append(p)
    candidates = uniq

    for p in candidates:
        obj, err = _safe_read_json(p)
        source_paths.append(str(p))
        mt = _mtime(p)
        if mt is not None:
            source_mtimes[str(p)] = mt

        if not isinstance(obj, dict):
            warnings.append(f"REPLAY_UNREADABLE:{p}:{err}")
            continue

        st = obj.get("state") or obj.get("status") or obj.get("verdict") or obj.get("run_verdict")
        if isinstance(st, dict):
            st = st.get("state") or st.get("status")
        state = _coerce_state(str(st) if st is not None else "UNKNOWN")

        rc = obj.get("reason_codes") or obj.get("reason_codes_top") or []
        last = obj.get("generated_at_utc") or obj.get("generated_utc") or obj.get("asof_utc") or obj.get("produced_utc") or None

        tile = Tile(
            tile_id="replay_certification",
            state=state,
            last_updated_utc=str(last) if isinstance(last, str) and last else None,
            reason_codes=_top2_reason_codes(rc),
            reason_human=[],
            artifact_path=str(p),
            artifact_sha256=_sha256_file(p),
        )
        return tile, sorted(set(missing)), sorted(set(source_paths)), source_mtimes, sorted(set(warnings))

    warnings.append("REPLAY_NOT_FOUND")
    return None, sorted(set(missing)), sorted(set(source_paths)), source_mtimes, sorted(set(warnings))


# -------------------------
# Engines from truth (no runtime config dependency)
# -------------------------

def _engine_ids_deep_scan(obj: Any) -> List[str]:
    eids: List[str] = []

    def walk(node: Any) -> None:
        if isinstance(node, dict):
            for k, v in node.items():
                if k == "engine_id" and isinstance(v, str) and v.strip():
                    eids.append(v.strip())
                else:
                    walk(v)
        elif isinstance(node, list):
            for v in node:
                walk(v)

    walk(obj)
    return sorted(set(eids))


def _engine_ids_from_active_engine_set(truth_root: Path, day: str) -> Tuple[List[str], List[str], List[str]]:
    """
    Primary engine list source (truth): reports/active_engine_set_v1/<day>/active_engine_set.v1.json
    Returns (engine_ids, missing_paths, warnings)
    """
    missing: List[str] = []
    warnings: List[str] = []

    p = (truth_root / "reports" / "active_engine_set_v1" / day / "active_engine_set.v1.json").resolve()
    if not p.exists():
        missing.append(str(p))
        warnings.append("ACTIVE_ENGINE_SET_MISSING")
        return [], missing, warnings

    obj, err = _safe_read_json(p)
    if not isinstance(obj, dict):
        warnings.append(f"ACTIVE_ENGINE_SET_UNREADABLE:{err}")
        return [], missing, warnings

    # common shapes
    for k in ("engine_ids", "active_engine_ids", "engines"):
        v = obj.get(k)
        if isinstance(v, list):
            out: List[str] = []
            for it in v:
                if isinstance(it, str) and it.strip():
                    out.append(it.strip())
                elif isinstance(it, dict):
                    eid = it.get("engine_id")
                    if isinstance(eid, str) and eid.strip():
                        out.append(eid.strip())
            out = sorted(set(out))
            if out:
                return out, missing, warnings

    # deep scan fallback
    out = _engine_ids_deep_scan(obj)
    if not out:
        warnings.append("ACTIVE_ENGINE_SET_EMPTY")
    return out, missing, warnings


def _engine_ids_from_engine_linkage(truth_root: Path, day: str) -> Tuple[List[str], List[str], List[str]]:
    """
    Fallback engine list source (truth): engine_linkage_v1/snapshots/<day>/engine_linkage.v1.json
    Schema-agnostic deep scan for engine_id.
    """
    missing: List[str] = []
    warnings: List[str] = []

    p = (truth_root / "engine_linkage_v1" / "snapshots" / day / "engine_linkage.v1.json").resolve()
    if not p.exists():
        missing.append(str(p))
        warnings.append("ENGINE_LINKAGE_MISSING")
        return [], missing, warnings

    obj, err = _safe_read_json(p)
    if not isinstance(obj, (dict, list)):
        warnings.append(f"ENGINE_LINKAGE_UNREADABLE:{err}")
        return [], missing, warnings

    out = _engine_ids_deep_scan(obj)
    if not out:
        warnings.append("ENGINE_LINKAGE_EMPTY")
    return out, missing, warnings


def _engine_ids_from_heartbeat(truth_root: Path, day: str) -> Tuple[List[str], List[str], List[str]]:
    """
    Fallback engine list from heartbeat directory names:
    monitoring_v1/engine_heartbeat_v1/<day>/<ENGINE_ID>/...
    """
    missing: List[str] = []
    warnings: List[str] = []
    root = (truth_root / "monitoring_v1" / "engine_heartbeat_v1" / day).resolve()
    if not root.exists() or not root.is_dir():
        missing.append(str(root))
        warnings.append("ENGINE_HEARTBEAT_MISSING")
        return [], missing, warnings
    out: List[str] = []
    for p in sorted([x for x in root.iterdir() if x.is_dir()], key=lambda x: x.name):
        n = p.name.strip()
        if n:
            out.append(n)
    if not out:
        warnings.append("ENGINE_HEARTBEAT_EMPTY")
    return sorted(set(out)), missing, warnings


# -------------------------
# Counts / flow
# -------------------------

def _intents_root(truth_root: Path) -> Path:
    return truth_root / "intents_v1" / "snapshots"


def _submissions_root(truth_root: Path) -> Path:
    return truth_root / "execution_evidence_v1" / "submissions"


def _count_intents(truth_root: Path, day: str) -> Tuple[int, List[str]]:
    root = (_intents_root(truth_root) / day).resolve()
    if not root.exists() or not root.is_dir():
        return 0, [str(root)]
    files = sorted([p for p in root.iterdir() if p.is_file()])
    return len(files), []


def _count_submissions_and_fills(truth_root: Path, day: str) -> Tuple[Dict[str, int], List[str]]:
    miss: List[str] = []
    root = (_submissions_root(truth_root) / day).resolve()
    if not root.exists() or not root.is_dir():
        miss.append(str(root))
        return {"submitted": 0, "filled": 0}, miss

    submitted = 0
    filled = 0

    items = sorted(list(root.rglob("*.json")), key=lambda p: str(p))
    for p in items:
        obj, _ = _safe_read_json(p)
        if not isinstance(obj, dict):
            continue
        sid = str(obj.get("schema_id") or "")
        if "broker_submission_record" in sid:
            submitted += 1
        if "execution_event_record" in sid:
            st = obj.get("status")
            if isinstance(st, str) and "FILL" in st.upper():
                filled += 1

    return {"submitted": submitted, "filled": filled}, miss


def _candidate_activity_rollup_path(truth_root: Path, day: str) -> Path:
    return (truth_root / "monitoring_v1" / "activity_ledger_rollup_v1" / day / "activity_ledger_rollup.v1.json").resolve()


def _load_activity_rollup(truth_root: Path, day: str) -> Tuple[Optional[Dict[str, Any]], List[str]]:
    p = _candidate_activity_rollup_path(truth_root, day)
    obj, err = _safe_read_json(p)
    if not isinstance(obj, dict):
        return None, [str(p)] if err == "FILE_NOT_FOUND" else [str(p)]
    return obj, []


def _extract_flow_from_activity_rollup(doc: Optional[Dict[str, Any]]) -> Dict[str, Optional[int]]:
    out: Dict[str, Optional[int]] = {
        "intents": None,
        "authorized": None,
        "submitted": None,
        "filled": None,
        "reconciled": None,
        "blocked_liquidity": None,
        "blocked_correlation": None,
        "blocked_attestation": None,
        "blocked_convex": None,
        "blocked_capital": None,
    }
    if not isinstance(doc, dict):
        return out

    totals = doc.get("totals") if isinstance(doc.get("totals"), dict) else None
    counts = doc.get("counts") if isinstance(doc.get("counts"), dict) else None
    src = totals or counts or doc

    def _get_int(keys: List[str]) -> Optional[int]:
        for k in keys:
            v = src.get(k) if isinstance(src, dict) else None
            if isinstance(v, int):
                return v
        return None

    out["intents"] = _get_int(["intents_total", "intents_today", "intents"])
    out["submitted"] = _get_int(["submissions_total", "submitted_total", "submitted"])
    out["authorized"] = _get_int(["authorized_total", "authorizations_total", "authorized"])
    out["filled"] = _get_int(["fills_total", "filled_total", "filled"])
    out["reconciled"] = _get_int(["reconciled_total", "reconciled"])

    blocked = doc.get("blocked_by_gate") if isinstance(doc.get("blocked_by_gate"), dict) else None
    if isinstance(blocked, dict):
        mapping = [
            ("liquidity", "blocked_liquidity"),
            ("correlation", "blocked_correlation"),
            ("attestation", "blocked_attestation"),
            ("convex", "blocked_convex"),
            ("capital", "blocked_capital"),
        ]
        for k, field in mapping:
            v = blocked.get(k)
            if isinstance(v, int):
                out[field] = v

    return out


# -------------------------
# Accounting / portfolio
# -------------------------

def _load_nav(truth_root: Path, day: str) -> Tuple[Optional[Dict[str, Any]], List[str], Optional[str], Optional[str]]:
    p = (truth_root / "accounting_v2" / "nav" / day / "nav.v2.json").resolve()
    if not p.exists():
        return None, [str(p)], None, None
    obj, err = _safe_read_json(p)
    if not isinstance(obj, dict):
        return None, [str(p)], str(err), str(p)
    return obj, [], None, str(p)


def _extract_portfolio_metrics(nav_doc: Optional[Dict[str, Any]]) -> Dict[str, Any]:
    out = {
        "nav_total": None,
        "pnl_today": None,
        "pnl_cumulative": None,
        "drawdown_pct": None,
        "cash_pct": None,
        "net_exposure_pct": None,
        "gross_exposure_pct": None,
        "asof_utc": None,
    }
    if not isinstance(nav_doc, dict):
        return out

    out["asof_utc"] = nav_doc.get("asof_utc") or nav_doc.get("generated_at_utc") or nav_doc.get("generated_utc")

    nav = nav_doc.get("nav") if isinstance(nav_doc.get("nav"), dict) else None
    src = nav or nav_doc

    def _get_num(keys: List[str]) -> Optional[float]:
        for k in keys:
            v = src.get(k) if isinstance(src, dict) else None
            if isinstance(v, (int, float)):
                return float(v)
        return None

    out["nav_total"] = _get_num(["nav_total", "nav_end", "nav"])
    out["pnl_today"] = _get_num(["pnl_today", "pnl_day", "pnl_1d"])
    out["pnl_cumulative"] = _get_num(["pnl_cumulative", "pnl_total", "pnl_cum"])
    out["drawdown_pct"] = _get_num(["drawdown_pct", "dd_pct"])
    out["cash_pct"] = _get_num(["cash_pct"])
    out["net_exposure_pct"] = _get_num(["net_exposure_pct", "net_pct"])
    out["gross_exposure_pct"] = _get_num(["gross_exposure_pct", "gross_pct"])
    return out


def _load_positions_snapshot(truth_root: Path, day: str) -> Tuple[Optional[Dict[str, Any]], List[str], Optional[str], Optional[str]]:
    p = (truth_root / "positions_v1" / "snapshots" / day / "positions_snapshot.v2.json").resolve()
    if not p.exists():
        return None, [str(p)], None, None
    obj, err = _safe_read_json(p)
    if not isinstance(obj, dict):
        return None, [str(p)], str(err), str(p)
    return obj, [], None, str(p)


def _load_exposure_net(truth_root: Path, day: str) -> Tuple[Optional[Dict[str, Any]], List[str], Optional[str], Optional[str]]:
    p = (truth_root / "risk_v1" / "exposure_net_v1" / day / "exposure_net.v1.json").resolve()
    if not p.exists():
        return None, [str(p)], None, None
    obj, err = _safe_read_json(p)
    if not isinstance(obj, dict):
        return None, [str(p)], str(err), str(p)
    return obj, [], None, str(p)


def _extract_positions_exposure(positions_doc: Optional[Dict[str, Any]], exposure_doc: Optional[Dict[str, Any]]) -> Dict[str, Any]:
    out: Dict[str, Any] = {
        "asof_utc": None,
        "summary": {
            "positions_total": 0,
            "open_positions": 0,
            "portfolio_net_notional_usd": None,
            "portfolio_gross_notional_usd": None,
            "capital_at_risk_cents": None,
            "symbol_count": None,
        },
        "positions": [],
        "exposure_by_engine": [],
        "sources": {"positions_path": None, "exposure_path": None},
    }

    if isinstance(positions_doc, dict):
        out["asof_utc"] = positions_doc.get("produced_utc") if isinstance(positions_doc.get("produced_utc"), str) else None
        pos = positions_doc.get("positions") if isinstance(positions_doc.get("positions"), dict) else {}
        items = pos.get("items") if isinstance(pos.get("items"), list) else []
        rows: List[Dict[str, Any]] = []
        open_cnt = 0
        for it in items:
            if not isinstance(it, dict):
                continue
            status = str(it.get("status") or "UNKNOWN")
            qty = it.get("qty")
            if status.upper() == "OPEN":
                open_cnt += 1
            rows.append(
                {
                    "position_id": it.get("position_id"),
                    "engine_id": it.get("engine_id"),
                    "qty": qty if isinstance(qty, (int, float)) else None,
                    "status": status,
                    "market_exposure_type": it.get("market_exposure_type"),
                }
            )
        out["positions"] = rows
        out["summary"]["positions_total"] = len(rows)
        out["summary"]["open_positions"] = open_cnt

    if isinstance(exposure_doc, dict):
        portfolio = exposure_doc.get("portfolio") if isinstance(exposure_doc.get("portfolio"), dict) else {}
        out["summary"]["portfolio_net_notional_usd"] = portfolio.get("net_notional_usd")
        out["summary"]["portfolio_gross_notional_usd"] = portfolio.get("gross_notional_usd")
        out["summary"]["capital_at_risk_cents"] = portfolio.get("capital_at_risk_cents")
        out["summary"]["symbol_count"] = portfolio.get("symbol_count")

        per_engine = exposure_doc.get("per_engine") if isinstance(exposure_doc.get("per_engine"), list) else []
        e_rows: List[Dict[str, Any]] = []
        for it in per_engine:
            if not isinstance(it, dict):
                continue
            e_rows.append(
                {
                    "engine_id": it.get("engine_id"),
                    "net_notional_usd": it.get("net_notional_usd"),
                    "gross_notional_usd": it.get("gross_notional_usd"),
                    "capital_at_risk_cents": it.get("capital_at_risk_cents"),
                }
            )
        e_rows.sort(key=lambda x: str(x.get("engine_id") or ""))
        out["exposure_by_engine"] = e_rows

    return out


def _flow_drilldown(truth_root: Path, day: str, counts: Dict[str, Any]) -> Dict[str, Any]:
    intents_dir = (truth_root / "intents_v1" / "snapshots" / day).resolve()
    auth_dir = (truth_root / "engine_activity_v1" / "authorization_v1" / day).resolve()
    veto_dir = (truth_root / "phaseC_preflight_v1" / day).resolve()
    sub_dir = (truth_root / "execution_evidence_v1" / "submissions" / day).resolve()
    recon_path = (truth_root / "reports" / "execution_reconciliation_v1" / day / "execution_reconciliation.v1.json").resolve()

    def top_paths(paths: List[str]) -> List[str]:
        return sorted(paths)[:20]

    intent_paths = top_paths([str(p) for p in intents_dir.glob("*.json")]) if intents_dir.exists() else []
    auth_paths = top_paths([str(p) for p in auth_dir.glob("*.authorization.v1.json")]) if auth_dir.exists() else []
    rejected_paths: List[str] = []
    authorized_paths: List[str] = []
    for p in sorted([Path(x) for x in auth_paths], key=lambda x: str(x)):
        obj, _err = _safe_read_json(p)
        if not isinstance(obj, dict):
            continue
        st = str(obj.get("status") or obj.get("decision") or "").upper()
        if st == "REJECTED":
            rejected_paths.append(str(p))
        elif st == "AUTHORIZED":
            authorized_paths.append(str(p))

    veto_paths = top_paths([str(p) for p in veto_dir.rglob("*.veto_record.v1.json")]) if veto_dir.exists() else []

    submitted_paths: List[str] = []
    filled_paths: List[str] = []
    if sub_dir.exists():
        for p in sorted(sub_dir.rglob("*.json"), key=lambda x: str(x)):
            obj, _err = _safe_read_json(p)
            if not isinstance(obj, dict):
                continue
            sid = str(obj.get("schema_id") or "")
            if "broker_submission_record" in sid:
                submitted_paths.append(str(p))
            if "execution_event_record" in sid:
                st = str(obj.get("status") or "").upper()
                if "FILL" in st:
                    filled_paths.append(str(p))

    return {
        "intents": {
            "count": counts.get("intents"),
            "summary": "Intent artifacts emitted for selected day.",
            "evidence_paths": top_paths(intent_paths),
        },
        "rejected_or_vetoed": {
            "count": counts.get("rejected"),
            "summary": "Rejected authorizations and/or submit vetoes blocked before broker submission.",
            "evidence_paths": top_paths(rejected_paths + veto_paths),
        },
        "authorized": {
            "count": counts.get("authorized"),
            "summary": "Authorization records allowed by capital authority.",
            "evidence_paths": top_paths(authorized_paths),
        },
        "submitted": {
            "count": counts.get("submitted"),
            "summary": "Broker submission records written.",
            "evidence_paths": top_paths(submitted_paths),
        },
        "filled": {
            "count": counts.get("filled"),
            "summary": "Execution records with fill state.",
            "evidence_paths": top_paths(filled_paths),
        },
        "reconciled": {
            "count": counts.get("reconciled"),
            "summary": "Execution reconciliation status for day.",
            "evidence_paths": [str(recon_path)] if recon_path.exists() else [],
        },
        "vetoed": {
            "count": counts.get("vetoed"),
            "summary": "PhaseC submit veto records observed for day.",
            "evidence_paths": top_paths(veto_paths),
        },
    }


def _count_authorization_rejected(truth_root: Path, day: str) -> Tuple[int, List[str]]:
    root = (truth_root / "engine_activity_v1" / "authorization_v1" / day).resolve()
    if not root.exists() or not root.is_dir():
        return 0, [str(root)]
    seen = set()
    cnt = 0
    for p in sorted(root.glob("*.authorization.v1.json"), key=lambda x: x.name):
        obj, _err = _safe_read_json(p)
        if not isinstance(obj, dict):
            continue
        ih = str(obj.get("intent_hash") or "").strip() or p.name.split(".")[0]
        if ih in seen:
            continue
        seen.add(ih)
        st = str(obj.get("status") or obj.get("decision") or "").strip().upper()
        if st == "REJECTED":
            cnt += 1
    return cnt, []


def _count_phasec_veto_records(truth_root: Path, day: str) -> Tuple[int, List[str]]:
    root = (truth_root / "phaseC_preflight_v1" / day).resolve()
    if not root.exists() or not root.is_dir():
        return 0, [str(root)]
    seen = set()
    cnt = 0
    for p in sorted(root.rglob("*.veto_record.v1.json"), key=lambda x: str(x)):
        ih = p.name.split(".")[0]
        if ih in seen:
            continue
        seen.add(ih)
        cnt += 1
    return cnt, []


def _active_engine_ids_for_day(truth_root: Path, day: str) -> List[str]:
    eids = set()

    # Heartbeat surface
    hb_day = (truth_root / "monitoring_v1" / "engine_heartbeat_v1" / day).resolve()
    if hb_day.exists() and hb_day.is_dir():
        for p in hb_day.iterdir():
            if p.is_dir() and p.name.strip():
                eids.add(p.name.strip())

    # Intent surface
    intents_day = (truth_root / "intents_v1" / "snapshots" / day).resolve()
    if intents_day.exists() and intents_day.is_dir():
        for p in sorted([x for x in intents_day.iterdir() if x.is_file() and x.suffix == ".json"], key=lambda x: x.name):
            obj, _err = _safe_read_json(p)
            if not isinstance(obj, dict):
                continue
            eng = obj.get("engine") if isinstance(obj.get("engine"), dict) else None
            eid = eng.get("engine_id") if isinstance(eng, dict) else None
            if isinstance(eid, str) and eid.strip():
                eids.add(eid.strip())

    # Authorization surface
    auth_day = (truth_root / "engine_activity_v1" / "authorization_v1" / day).resolve()
    if auth_day.exists() and auth_day.is_dir():
        for p in sorted(auth_day.glob("*.authorization.v1.json"), key=lambda x: x.name):
            obj, _err = _safe_read_json(p)
            if not isinstance(obj, dict):
                continue
            eid = obj.get("engine_id")
            if isinstance(eid, str) and eid.strip():
                eids.add(eid.strip())

    return sorted(eids)


def _load_sleeve_policy() -> List[Dict[str, Any]]:
    obj, _err = _safe_read_json(SLEEVE_POLICY_REGISTRY)
    if not isinstance(obj, dict):
        return []
    sleeves = obj.get("sleeves")
    if not isinstance(sleeves, list):
        return []
    out: List[Dict[str, Any]] = []
    for s in sleeves:
        if not isinstance(s, dict):
            continue
        sid = str(s.get("sleeve_id") or "").strip()
        engine_ids = s.get("engine_ids") if isinstance(s.get("engine_ids"), list) else []
        limits = s.get("limits") if isinstance(s.get("limits"), dict) else {}
        if not sid or not engine_ids:
            continue
        out.append(
            {
                "sleeve_id": sid,
                "display_name": str(s.get("display_name") or sid),
                "engine_ids": [str(e).strip() for e in engine_ids if isinstance(e, str) and str(e).strip()],
                "priority_rank": int(s.get("priority_rank")) if isinstance(s.get("priority_rank"), int) else 9999,
                "max_capital_at_risk_cents": int(limits.get("max_capital_at_risk_cents")) if isinstance(limits.get("max_capital_at_risk_cents"), int) else 0,
            }
        )
    out.sort(key=lambda x: (x.get("priority_rank", 9999), x.get("sleeve_id", "")))
    return out


def _resolve_secondary_paper_account(primary_account: Optional[str]) -> Optional[str]:
    obj, _err = _safe_read_json(IB_ACCOUNT_REGISTRY)
    if not isinstance(obj, dict):
        return None
    accts = obj.get("accounts")
    if not isinstance(accts, list):
        return None
    primary = str(primary_account or "").strip()
    cands: List[str] = []
    for a in accts:
        if not isinstance(a, dict):
            continue
        env = str(a.get("environment") or "").strip().upper()
        enabled = bool(a.get("enabled_for_submission"))
        aid = str(a.get("account_id") or "").strip()
        if env == "PAPER" and enabled and aid:
            cands.append(aid)
    cands = sorted(set(cands))
    for aid in cands:
        if aid != primary:
            return aid
    return None


def _build_sleeve_strip_rows(
    *,
    truth_root: Path,
    day: str,
    mode_from_attempt: Optional[str],
    primary_account: Optional[str],
    fallback_rows: List[Dict[str, Any]],
) -> Tuple[List[Dict[str, Any]], List[str]]:
    warnings: List[str] = []
    sleeves = _load_sleeve_policy()
    if not sleeves:
        warnings.append("SLEEVE_POLICY_MISSING")
        return fallback_rows, warnings

    active_engines = set(_active_engine_ids_for_day(truth_root, day))
    secondary_account = _resolve_secondary_paper_account(primary_account)
    primary = str(primary_account or "").strip() or None
    mode = (mode_from_attempt or "UNKNOWN")

    out: List[Dict[str, Any]] = []
    for s in sleeves:
        engine_ids = s.get("engine_ids") or []
        is_active = any(e in active_engines for e in engine_ids)
        acct: Optional[str]
        if is_active:
            acct = primary
        else:
            acct = secondary_account or primary
        if acct is None:
            warnings.append(f"SLEEVE_ACCOUNT_UNRESOLVED:{s.get('sleeve_id')}")
        out.append(
            {
                "sleeve_id": s.get("sleeve_id"),
                "name": s.get("display_name"),
                "mode": mode,
                "ib_account_id": acct,
                "entries_allowed": None,
                "flatten_only": None,
                "engine_ids": engine_ids,
                "active_today": bool(is_active),
            }
        )
    out.sort(key=lambda x: (x.get("sleeve_id") or "", x.get("name") or ""))
    return out, warnings


def _build_attempt_summaries(truth_root: Path, day: str, attempts: List[str]) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    for aid in attempts:
        doc = _load_attempt_verdict(truth_root, day, aid)
        if not isinstance(doc, dict):
            out.append(
                {
                    "attempt_id": aid,
                    "attempt_seq": None,
                    "status": "UNKNOWN",
                    "produced_utc": None,
                    "reason_codes": [],
                }
            )
            continue
        out.append(
            {
                "attempt_id": str(doc.get("attempt_id") or aid),
                "attempt_seq": doc.get("attempt_seq") if isinstance(doc.get("attempt_seq"), int) else None,
                "status": _coerce_state(str(doc.get("status") or doc.get("state") or "UNKNOWN")),
                "produced_utc": doc.get("produced_utc") if isinstance(doc.get("produced_utc"), str) else None,
                "reason_codes": _top2_reason_codes(doc.get("reason_codes") or []),
                "producer_git_sha": doc.get("producer", {}).get("git_sha") if isinstance(doc.get("producer"), dict) else None,
            }
        )
    out.sort(key=lambda x: (int(x.get("attempt_seq")) if isinstance(x.get("attempt_seq"), int) else -1, str(x.get("attempt_id") or "")))
    return out


# -------------------------
# Deterministic diff (server memory)
# -------------------------

_LAST_HASH: Optional[str] = None
_LAST_KEY_FIELDS: Optional[Dict[str, Any]] = None


def _compute_key_fields(payload: Dict[str, Any]) -> Dict[str, Any]:
    meta = payload.get("meta") if isinstance(payload.get("meta"), dict) else {}
    ops = payload.get("ops_health") if isinstance(payload.get("ops_health"), dict) else {}
    flow = payload.get("trade_flow_today") if isinstance(payload.get("trade_flow_today"), dict) else {}
    port = payload.get("portfolio") if isinstance(payload.get("portfolio"), dict) else {}

    tiles = ops.get("tiles") if isinstance(ops.get("tiles"), list) else []
    tile_k: List[Dict[str, Any]] = []
    for t in tiles:
        if not isinstance(t, dict):
            continue
        tid = t.get("tile_id")
        if isinstance(tid, str):
            tile_k.append({"tile_id": tid, "state": t.get("state"), "last_updated_utc": t.get("last_updated_utc")})
    tile_k.sort(key=lambda x: x["tile_id"])

    engines = payload.get("engines") if isinstance(payload.get("engines"), list) else []
    eng_k: List[Dict[str, Any]] = []
    for e in engines:
        if not isinstance(e, dict):
            continue
        eid = e.get("engine_id")
        if isinstance(eid, str):
            eng_k.append({"engine_id": eid, "status": e.get("status"), "mode": e.get("mode"), "ib_account_id": e.get("ib_account_id")})
    eng_k.sort(key=lambda x: x["engine_id"])

    return {
        "selected_day": meta.get("selected_day"),
        "selected_attempt_id": meta.get("selected_attempt_id"),
        "tiles": tile_k,
        "flow": flow.get("counts"),
        "blocked": flow.get("blocked_by_gate"),
        "portfolio": {
            "nav_total": port.get("nav_total"),
            "pnl_today": port.get("pnl_today"),
            "pnl_cumulative": port.get("pnl_cumulative"),
            "asof_utc": port.get("asof_utc"),
        },
        "engines": eng_k,
    }


def _diff_key_fields(prev: Optional[Dict[str, Any]], cur: Dict[str, Any]) -> List[Dict[str, str]]:
    if prev is None:
        return [{"code": "FIRST_LOAD", "summary": "First load"}]

    out: List[Dict[str, str]] = []

    if prev.get("selected_attempt_id") != cur.get("selected_attempt_id"):
        out.append({"code": "ATTEMPT_CHANGED", "summary": "Attempt changed"})

    prev_tiles = {t["tile_id"]: t for t in (prev.get("tiles") or []) if isinstance(t, dict) and "tile_id" in t}
    cur_tiles = {t["tile_id"]: t for t in (cur.get("tiles") or []) if isinstance(t, dict) and "tile_id" in t}
    for tid in sorted(set(prev_tiles.keys()) | set(cur_tiles.keys())):
        a = prev_tiles.get(tid, {})
        b = cur_tiles.get(tid, {})
        if a.get("state") != b.get("state"):
            out.append({"code": "TILE_STATE_CHANGED", "summary": f"{tid}: {a.get('state')} → {b.get('state')}"})

    if prev.get("portfolio") != cur.get("portfolio"):
        out.append({"code": "PORTFOLIO_UPDATED", "summary": "Portfolio metrics updated"})

    prev_eng = {e["engine_id"]: e for e in (prev.get("engines") or []) if isinstance(e, dict) and "engine_id" in e}
    cur_eng = {e["engine_id"]: e for e in (cur.get("engines") or []) if isinstance(e, dict) and "engine_id" in e}
    for eid in sorted(set(prev_eng.keys()) | set(cur_eng.keys())):
        a = prev_eng.get(eid, {})
        b = cur_eng.get(eid, {})
        if a.get("status") != b.get("status"):
            out.append({"code": "ENGINE_STATUS_CHANGED", "summary": f"{eid}: {a.get('status')} → {b.get('status')}"})
        if a.get("mode") != b.get("mode"):
            out.append({"code": "ENGINE_MODE_CHANGED", "summary": f"{eid}: {a.get('mode')} → {b.get('mode')}"})

    return out[:8] if out else [{"code": "NO_CHANGE", "summary": "No change"}]


# -------------------------
# Main builder
# -------------------------

def build_status_v2(
    truth_root: Path,
    instance_config_path: Path,
    day: str,
    attempt_id: Optional[str],
    c3_status: Optional[Dict[str, Any]],
) -> Dict[str, Any]:
    global _LAST_HASH, _LAST_KEY_FIELDS

    # Attempts
    attempts, miss_a, sp_a, sm_a, warn_a = discover_attempts(truth_root, day)
    raw_attempt = attempt_id.strip() if isinstance(attempt_id, str) else ""
    if raw_attempt in ("", "attempts", "latest"):
        sel_attempt = select_preferred_attempt(truth_root, day, attempts)
    else:
        sel_attempt = raw_attempt

    # Run verdict
    run_tile, miss_rv, sp_rv, sm_rv, warn_rv = _parse_orchestrator_run_verdict_v2(truth_root, day, sel_attempt)
    run_doc = _load_selected_run_verdict_doc(run_tile)

    # Gate stack verdict (optional)
    gate_tile, miss_gs, sp_gs, sm_gs, warn_gs = _parse_gate_stack_verdict_tile(truth_root, day)

    # Gate tiles (authoritative artifacts you proved exist)
    attest_path = (truth_root / "reports" / "feed_attestation_gate_v1" / day / "feed_attestation_gate.v1.json").resolve()
    liquidity_path = (truth_root / "reports" / "liquidity_slippage_gate_v1" / day / "liquidity_slippage_gate.v1.json").resolve()
    correlation_path = (truth_root / "reports" / "correlation_envelope_gate_v1" / day / "correlation_envelope_gate.v1.json").resolve()
    convex_path = (truth_root / "reports" / "convex_risk_assessment_v1" / day / "convex_risk_assessment.v1.json").resolve()

    attest_tile, warn_att, miss_att = _parse_simple_gate_tile(attest_path, "feed_attestation")
    liquidity_tile, warn_liq, miss_liq = _parse_simple_gate_tile(liquidity_path, "liquidity_gate")
    corr_tile, warn_cor, miss_cor = _parse_simple_gate_tile(correlation_path, "correlation_gate")
    convex_tile, warn_cvx, miss_cvx = _parse_simple_gate_tile(convex_path, "convex_gate")

    # Replay
    replay_tile, miss_rep, sp_rep, sm_rep, warn_rep = _parse_replay_tile(truth_root, day, sel_attempt)

    # Safety breach tile
    safety_state = "UNKNOWN"
    safety_rc: List[str] = []
    safety_last: Optional[str] = None
    safety_path: Optional[str] = None
    safety_sha: Optional[str] = None

    if run_tile is not None:
        safety_last = run_tile.last_updated_utc
        if run_tile.state == "ABORTED":
            safety_state = "ABORTED"
            safety_rc = run_tile.reason_codes
            safety_path = run_tile.artifact_path
            safety_sha = run_tile.artifact_sha256
        else:
            safety_state = "PASS"
    elif gate_tile is not None:
        safety_last = gate_tile.last_updated_utc
        safety_state = "ABORTED" if gate_tile.state == "ABORTED" else "PASS"
        safety_path = gate_tile.artifact_path
        safety_sha = gate_tile.artifact_sha256

    safety_tile = Tile(
        tile_id="safety_breach",
        state=_coerce_state(safety_state),
        last_updated_utc=safety_last,
        reason_codes=safety_rc,
        reason_human=[],
        artifact_path=safety_path,
        artifact_sha256=safety_sha,
    )

    # Broker connection/observer
    broker_path_v2 = (truth_root / "reports" / "broker_reconciliation_v2" / day / "broker_reconciliation.v2.json").resolve()
    broker_path_v1 = (truth_root / "reports" / "broker_reconciliation_v1" / day / "broker_reconciliation.v1.json").resolve()

    if broker_path_v2.exists():
        broker_tile, warn_broker, miss_broker = _parse_simple_gate_tile(broker_path_v2, "broker_connection_observer")
    elif broker_path_v1.exists():
        broker_tile, warn_broker, miss_broker = _parse_simple_gate_tile(broker_path_v1, "broker_connection_observer")
    else:
        broker_state = "UNKNOWN"
        broker_last = None
        if isinstance(c3_status, dict):
            br = c3_status.get("broker_reconciliation") if isinstance(c3_status.get("broker_reconciliation"), dict) else None
            if isinstance(br, dict):
                broker_state = _coerce_state(str(br.get("state") or "UNKNOWN"))
                broker_last = br.get("generated_at_utc") or br.get("generated_utc") or None
        # Fallback to selected orchestrator attempt stage status when broker artifact is absent.
        if broker_state in ("UNKNOWN", "MISSING"):
            s = _attempt_stage_status(run_doc, "A1_BROKER_RECONCILIATION_GATE_V2_CHECK")
            if s in ("OK", "SKIP", "PASS"):
                broker_state = "PASS"
            elif s in ("FAIL", "ABORTED"):
                broker_state = "ABORTED"

        broker_tile = Tile(
            tile_id="broker_connection_observer",
            state=_coerce_state(broker_state),
            last_updated_utc=str(broker_last) if isinstance(broker_last, str) and broker_last else None,
            reason_codes=[],
            reason_human=[],
            artifact_path=None,
            artifact_sha256=None,
        )
        warn_broker = []
        miss_broker = [str(broker_path_v2), str(broker_path_v1)]

    # Flow
    rollup_doc, miss_roll = _load_activity_rollup(truth_root, day)
    flow = _extract_flow_from_activity_rollup(rollup_doc)
    intents_cnt, miss_int = _count_intents(truth_root, day)
    subs_cnt, miss_sub = _count_submissions_and_fills(truth_root, day)
    rejected_cnt, miss_auth = _count_authorization_rejected(truth_root, day)
    veto_cnt, miss_veto = _count_phasec_veto_records(truth_root, day)

    counts = {
        "intents": int(flow["intents"]) if isinstance(flow.get("intents"), int) else intents_cnt,
        "authorized": int(flow["authorized"]) if isinstance(flow.get("authorized"), int) else None,
        "submitted": int(flow["submitted"]) if isinstance(flow.get("submitted"), int) else subs_cnt["submitted"],
        "filled": int(flow["filled"]) if isinstance(flow.get("filled"), int) else subs_cnt["filled"],
        "reconciled": int(flow["reconciled"]) if isinstance(flow.get("reconciled"), int) else None,
        "rejected": rejected_cnt if rejected_cnt > 0 else None,
        "vetoed": veto_cnt if veto_cnt > 0 else None,
    }
    # Fallback derivations from orchestrator stage truth when day rollup is missing.
    auth_stage = _attempt_stage_status(run_doc, "A6B_AUTHORIZATION_ARTIFACTS_DAY_V1")
    recon_stage = _attempt_stage_status(run_doc, "B2_EXECUTION_RECONCILIATION_V1")
    if counts["authorized"] is None and auth_stage in ("OK", "SKIP", "PASS"):
        counts["authorized"] = counts["submitted"] if isinstance(counts["submitted"], int) else None
    if counts["reconciled"] is None and recon_stage in ("OK", "SKIP", "PASS"):
        counts["reconciled"] = counts["submitted"] if isinstance(counts["submitted"], int) else None
    blocked_by_gate = {
        "liquidity": flow.get("blocked_liquidity"),
        "correlation": flow.get("blocked_correlation"),
        "attestation": flow.get("blocked_attestation"),
        "convex": flow.get("blocked_convex"),
        "capital": flow.get("blocked_capital"),
    }

    # Portfolio
    nav_doc, miss_nav, nav_err, nav_path = _load_nav(truth_root, day)
    portfolio = _extract_portfolio_metrics(nav_doc)
    if nav_doc is None:
        portfolio["note_if_missing"] = "PnL unavailable (missing accounting/nav)"
        portfolio["missing"] = True
    else:
        portfolio["missing"] = False
    portfolio["nav_path"] = nav_path

    # Positions / Exposure
    positions_doc, miss_pos, pos_err, pos_path = _load_positions_snapshot(truth_root, day)
    exposure_doc, miss_exp, exp_err, exp_path = _load_exposure_net(truth_root, day)
    positions_exposure = _extract_positions_exposure(positions_doc, exposure_doc)
    positions_exposure["sources"]["positions_path"] = pos_path
    positions_exposure["sources"]["exposure_path"] = exp_path

    # Engine mode/account defaults from attempt manifest
    mode_from_attempt, acct_from_attempt, warn_ma, miss_ma = _attempt_mode_and_account(truth_root, day, sel_attempt)

    # Engines list from truth
    engines_out: List[Dict[str, Any]] = []
    miss_eng: List[str] = []
    warn_eng: List[str] = []

    eids, miss_ae, warn_ae = _engine_ids_from_active_engine_set(truth_root, day)
    miss_eng.extend(miss_ae)
    warn_eng.extend(warn_ae)

    if not eids:
        eids2, miss_el, warn_el = _engine_ids_from_engine_linkage(truth_root, day)
        miss_eng.extend(miss_el)
        warn_eng.extend(warn_el)
        eids = eids2
    if not eids:
        eids3, miss_hb, warn_hb = _engine_ids_from_heartbeat(truth_root, day)
        miss_eng.extend(miss_hb)
        warn_eng.extend(warn_hb)
        eids = eids3

    # Deterministic normalization
    eids = [str(x) for x in eids if isinstance(x, str) and str(x).strip()]
    eids = sorted(set(eids))

    for eid in eids:
        engines_out.append(
            {
                "engine_id": eid,
                "engine_name": eid,
                "mode": (mode_from_attempt or "UNKNOWN"),
                "ib_account_id": acct_from_attempt,
                "entries_allowed": None,
                "flatten_only": None,
                "status": "ACTIVE" if counts["intents"] and safety_tile.state != "ABORTED" else ("FAIL" if safety_tile.state == "ABORTED" else "UNKNOWN"),
                "today": {"intents": None, "authorized": None, "submitted": None, "filled": None},
                "positions": {"open_count": None},
                "exposure": {"net_pct": None, "gross_pct": None, "asof_utc": portfolio.get("asof_utc")},
                "pnl": {
                    "today": None if portfolio.get("missing") else portfolio.get("pnl_today"),
                    "cumulative": None if portfolio.get("missing") else portfolio.get("pnl_cumulative"),
                    "currency": "USD",
                    "asof_utc": portfolio.get("asof_utc"),
                    "note_if_missing": "PnL unavailable (missing accounting/nav)" if portfolio.get("missing") else None,
                },
                "applied_risk": {
                    "base_risk_pct": None,
                    "vol_adjusted_weight": None,
                    "liquidity_scalar": None,
                    "correlation_scalar": None,
                    "convex_scalar": None,
                    "final_authorized_weight": None,
                    "cash_authority_cap": None,
                },
                "details_collapsed": {
                    "top_reason_codes": [],
                    "attempt_ids": [sel_attempt] if isinstance(sel_attempt, str) and sel_attempt else [],
                    "replay_bundle": {
                        "state": replay_tile.state if replay_tile else "MISSING",
                        "summary": None,
                        "artifact_path": replay_tile.artifact_path if replay_tile else None,
                    },
                    "stage_timestamps": {},
                },
            }
        )

    engines_out.sort(key=lambda x: (x.get("engine_id") or "", x.get("engine_name") or ""))

    fallback_sleeves: List[Dict[str, Any]] = []
    for e in engines_out:
        fallback_sleeves.append(
            {
                "sleeve_id": e["engine_id"],
                "name": e["engine_name"],
                "mode": e["mode"],
                "ib_account_id": e.get("ib_account_id"),
                "entries_allowed": e.get("entries_allowed"),
                "flatten_only": e.get("flatten_only"),
            }
        )
    fallback_sleeves.sort(key=lambda x: (x.get("sleeve_id") or "", x.get("name") or ""))
    sleeves_out, warn_sleeves = _build_sleeve_strip_rows(
        truth_root=truth_root,
        day=day,
        mode_from_attempt=mode_from_attempt,
        primary_account=acct_from_attempt,
        fallback_rows=fallback_sleeves,
    )

    attempt_summaries = _build_attempt_summaries(truth_root, day, attempts)

    # Tiles (fixed layout)
    tiles: List[Tile] = []
    tiles.append(run_tile if run_tile else Tile("orchestrator_run_verdict_v2", "MISSING", None, ["RUN_VERDICT_NOT_FOUND"], [], None, None))
    tiles.append(safety_tile)
    tiles.append(broker_tile)
    tiles.append(attest_tile)
    tiles.append(liquidity_tile)
    tiles.append(corr_tile)
    tiles.append(convex_tile)
    tiles.append(replay_tile if replay_tile else Tile("replay_certification", "MISSING", None, ["REPLAY_NOT_FOUND"], [], None, None))

    # Provenance aggregation
    missing_paths = sorted(
        set(
            miss_a
            + miss_rv
            + miss_gs
            + miss_rep
            + miss_roll
            + miss_int
            + miss_sub
            + miss_auth
            + miss_veto
            + miss_pos
            + miss_exp
            + miss_nav
            + miss_att
            + miss_liq
            + miss_cor
            + miss_cvx
            + miss_eng
            + miss_ma
            + miss_broker
        )
    )

    source_paths = sorted(
        set(
            sp_a
            + sp_rv
            + sp_gs
            + sp_rep
            + ([nav_path] if isinstance(nav_path, str) and nav_path else [])
            + [str(instance_config_path)]
        )
    )

    source_mtimes: Dict[str, float] = {}
    for dct in (sm_a, sm_rv, sm_gs, sm_rep):
        source_mtimes.update({k: v for k, v in dct.items() if isinstance(v, (int, float))})

    warnings = sorted(
        set(
            warn_a
            + warn_rv
            + warn_gs
            + warn_rep
            + warn_att
            + warn_liq
            + warn_cor
            + warn_cvx
            + warn_broker
            + warn_ma
            + warn_eng
            + warn_sleeves
            + (["POSITIONS_UNREADABLE"] if pos_err else [])
            + (["EXPOSURE_UNREADABLE"] if exp_err else [])
            + (["NAV_UNREADABLE"] if nav_err else [])
        )
    )

    payload: Dict[str, Any] = {
        "meta": {
            "server_time_utc": _utc_now_iso(),
            "selected_day": day,
            "selected_attempt_id": sel_attempt,
            "attempts": attempts,
            "attempt_summaries": attempt_summaries,
            "canonical_pointer": {
                "exists": (truth_root / "run_pointer_v2" / "canonical_authority_head.v1.json").resolve().exists(),
                "points_to_attempt_id": None,
                "last_updated_utc": None,
                "note": "Canonical authority head presence only; attempt linkage not yet derived by this collector.",
            },
        },
        "ops_health": {"tiles": [_tile_dict(t) for t in tiles]},
        "sleeves": sleeves_out,
        "trade_flow_today": {
            "counts": counts,
            "blocked_by_gate": blocked_by_gate,
            "semantics": {
                "intents": "Intents emitted for selected day.",
                "rejected": "Authorization records with status REJECTED.",
                "authorized": "Intents authorized by capital authority.",
                "submitted": "Broker submission records written.",
                "filled": "Execution event records with fill status.",
                "reconciled": "Execution reconciliation completed for day.",
                "vetoed": "PhaseC submit veto records observed for day.",
            },
            "drilldown": _flow_drilldown(truth_root, day, counts),
        },
        "engines": engines_out,
        "portfolio": portfolio,
        "positions_exposure": positions_exposure,
        "provenance": {
            "warnings": warnings,
            "missing_paths": missing_paths,
            "source_paths": source_paths,
            "source_mtimes": source_mtimes,
        },
        "errors": [],
        "ok": True,
    }

    # Server-side deterministic diff summary
    key_fields = _compute_key_fields(payload)
    cur_hash = _sha256_bytes(_stable_json_bytes(key_fields))
    diffs = _diff_key_fields(_LAST_KEY_FIELDS, key_fields)
    payload["meta"]["what_changed"] = {"diff_from_prev_poll": diffs, "key_fields_sha256": cur_hash}

    _LAST_HASH = cur_hash
    _LAST_KEY_FIELDS = key_fields

    return payload
