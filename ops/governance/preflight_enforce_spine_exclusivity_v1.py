#!/usr/bin/env python3
from __future__ import annotations

import json
import re
from pathlib import Path
from typing import Dict, List, Tuple

REPO_ROOT = Path("/home/node/constellation_2_runtime").resolve()
TRUTH_ROOT = (REPO_ROOT / "constellation_2/runtime/truth").resolve()

RUNPTR_IDX = (TRUTH_ROOT / "run_pointer_v1/canonical_pointer_index.v1.jsonl").resolve()
REG = (REPO_ROOT / "governance/02_REGISTRIES/C2_SPINE_AUTHORITY_V1.json").resolve()

DAY_RE = re.compile(r"^[0-9]{4}-[0-9]{2}-[0-9]{2}$")


def _read_json_obj(p: Path) -> Dict:
    if not p.exists() or not p.is_file():
        raise SystemExit(f"FAIL: missing file: {p}")
    obj = json.loads(p.read_text(encoding="utf-8"))
    if not isinstance(obj, dict):
        raise SystemExit(f"FAIL: JSON_NOT_OBJECT: {p}")
    return obj


def _day_ge(a: str, b: str) -> bool:
    return a >= b


def _split_pattern(pat: str) -> Tuple[str, str]:
    if "{DAY}" not in pat:
        raise SystemExit(f"FAIL: day_path_pattern missing {{DAY}} token: {pat!r}")
    pre, post = pat.split("{DAY}", 1)
    return pre, post


def _discover_days_for_pattern(repo: Path, pat: str) -> List[Tuple[str, Path]]:
    pre, post = _split_pattern(pat)
    pre_path = (repo / pre).resolve()

    try:
        pre_path.relative_to(repo)
    except Exception:
        raise SystemExit(f"FAIL: pattern escapes repo: {pat!r} -> {pre_path}")

    out: List[Tuple[str, Path]] = []
    if not pre_path.exists() or not pre_path.is_dir():
        return out

    for child in sorted(pre_path.iterdir()):
        if not child.is_dir():
            continue
        day = child.name
        if not DAY_RE.match(day):
            continue
        full = (child / post.lstrip("/")).resolve()
        try:
            full.relative_to(repo)
        except Exception:
            raise SystemExit(f"FAIL: pattern resolves outside repo: {pat!r} -> {full}")
        if full.exists():
            out.append((day, full))
    return out


def _read_run_day_from_run_pointer_authority_head(idx_path: Path) -> str:
    if not idx_path.exists() or not idx_path.is_file():
        raise SystemExit(f"FAIL: missing run pointer index: {idx_path}")
    lines = [x.strip() for x in idx_path.read_text(encoding="utf-8").splitlines() if x.strip()]
    if not lines:
        raise SystemExit(f"FAIL: empty run pointer index: {idx_path}")

    best_seq = -1
    best_day = ""
    best_ok = False

    for ln in lines:
        try:
            o = json.loads(ln)
        except Exception:
            raise SystemExit(f"FAIL: invalid JSONL line in run pointer index: {idx_path}")
        if not isinstance(o, dict):
            continue
        try:
            seq = int(o.get("pointer_seq"))
        except Exception:
            continue
        day = str(o.get("day_utc") or "").strip()
        authoritative = bool(o.get("authoritative") is True)
        status = str(o.get("status") or "").strip().upper()

        # Authority head rule: authoritative == true AND status == PASS
        if authoritative and status == "PASS" and DAY_RE.match(day):
            if seq > best_seq:
                best_seq = seq
                best_day = day
                best_ok = True

    if not best_ok:
        raise SystemExit(
            "FAIL: no authoritative PASS pointer entry found in run pointer index "
            f"(cannot determine run day). path={idx_path}"
        )

    return best_day


def main() -> int:
    run_day = _read_run_day_from_run_pointer_authority_head(RUNPTR_IDX)

    cfg = _read_json_obj(REG)
    spines = cfg.get("spines", [])
    if not isinstance(spines, list):
        raise SystemExit(f"FAIL: registry malformed (spines not list): {REG}")

    any_fail = False
    checked_days_total = 0
    checked_spines = 0

    for s in spines:
        if not isinstance(s, dict):
            continue

        name = str(s.get("spine", "")).strip()
        active = str(s.get("active", "")).strip()
        enforce = str(s.get("enforce_from_day_utc", "")).strip()
        exclusive = bool(s.get("exclusive", False))
        patterns = s.get("day_path_patterns", [])
        versions = s.get("versions", [])

        if not name:
            continue
        if not exclusive:
            continue

        checked_spines += 1

        if enforce and not DAY_RE.match(enforce):
            raise SystemExit(f"FAIL: spine {name} has invalid enforce_from_day_utc: {enforce!r}")

        if not isinstance(patterns, list) or not patterns:
            raise SystemExit(f"FAIL: spine {name} missing day_path_patterns")
        if not isinstance(versions, list) or not versions:
            raise SystemExit(f"FAIL: spine {name} missing versions list")
        if active not in versions:
            raise SystemExit(f"FAIL: spine {name} active version {active!r} not in versions {versions!r}")

        per_day: Dict[str, Dict[str, int]] = {}

        for pat in patterns:
            if not isinstance(pat, str) or not pat.strip():
                raise SystemExit(f"FAIL: spine {name} has invalid pattern entry: {pat!r}")
            found = _discover_days_for_pattern(REPO_ROOT, pat)

            pat_version = None
            for v in versions:
                token = f"_{v}/"
                if token in pat:
                    pat_version = v
                    break
            if pat_version is None:
                raise SystemExit(f"FAIL: spine {name} pattern does not contain version token _vN/: {pat!r}")

            for (day, _fullpath) in found:
                if enforce and not _day_ge(day, enforce):
                    continue
                if day not in per_day:
                    per_day[day] = {v: 0 for v in versions}
                per_day[day][pat_version] += 1

        for day, counts in sorted(per_day.items()):
            checked_days_total += 1
            present_versions = [v for v, c in counts.items() if c > 0]

            if len(present_versions) == 0:
                continue

            if len(present_versions) > 1:
                print(
                    f"FAIL: spine split-brain detected spine={name} day={day} "
                    f"active={active} present_versions={present_versions} counts={counts}"
                )
                any_fail = True
                continue

            only = present_versions[0]
            if only != active:
                print(
                    f"FAIL: spine wrong active version spine={name} day={day} "
                    f"active={active} present={only} counts={counts}"
                )
                any_fail = True

    if any_fail:
        raise SystemExit(2)

    print(
        f"[c2-preflight] PASS: spine exclusivity holds "
        f"(checked_spines={checked_spines} checked_days={checked_days_total} run_day={run_day})"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
