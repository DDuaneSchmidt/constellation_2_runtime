#!/usr/bin/env bash
set -euo pipefail

REPO_ROOT="/home/node/constellation_2_runtime"
TRUTH_ROOT="${REPO_ROOT}/constellation_2/runtime/truth"
RUN_PTR="${TRUTH_ROOT}/latest.json"
REG="${REPO_ROOT}/governance/02_REGISTRIES/C2_SPINE_AUTHORITY_V1.json"

echo "[c2-preflight] enforcing spine exclusivity (day-scoped) via ${REG}"

if [[ ! -f "${RUN_PTR}" ]]; then
  echo "FAIL: missing run pointer: ${RUN_PTR}" >&2
  exit 2
fi
if [[ ! -f "${REG}" ]]; then
  echo "FAIL: missing spine authority registry: ${REG}" >&2
  exit 2
fi

DAY="$(python3 -c 'import json;print(json.load(open("'"${RUN_PTR}"'","r",encoding="utf-8")).get("day_utc",""))')"
if [[ -z "${DAY}" ]]; then
  echo "FAIL: run pointer missing day_utc: ${RUN_PTR}" >&2
  exit 2
fi

python3 - <<'PY'
import json
from pathlib import Path

repo = Path("/home/node/constellation_2_runtime")
truth = repo / "constellation_2/runtime/truth"
run_ptr = truth / "latest.json"
reg = repo / "governance/02_REGISTRIES/C2_SPINE_AUTHORITY_V1.json"

run = json.load(run_ptr.open("r", encoding="utf-8"))
day = run.get("day_utc","").strip()
if not day:
    raise SystemExit(f"FAIL: run pointer missing day_utc: {run_ptr}")

cfg = json.load(reg.open("r", encoding="utf-8"))
spines = cfg.get("spines", [])
if not isinstance(spines, list):
    raise SystemExit(f"FAIL: registry malformed (spines not list): {reg}")

def day_ge(a: str, b: str) -> bool:
    # YYYY-MM-DD lexical compare is valid
    return a >= b

fail = False
for s in spines:
    if not isinstance(s, dict):
        continue
    name = str(s.get("spine","")).strip()
    active = str(s.get("active","")).strip()
    enforce = str(s.get("enforce_from_day_utc","")).strip()
    exclusive = bool(s.get("exclusive", False))
    patterns = s.get("day_path_patterns", [])
    versions = s.get("versions", [])
    if not name or not exclusive:
        continue
    if enforce and not day_ge(day, enforce):
        continue
    if not isinstance(patterns, list) or not patterns:
        raise SystemExit(f"FAIL: spine {name} missing day_path_patterns")
    if not isinstance(versions, list) or not versions:
        raise SystemExit(f"FAIL: spine {name} missing versions list")
    if active not in versions:
        raise SystemExit(f"FAIL: spine {name} active version {active} not in versions {versions}")

    # Determine per-version presence by checking if any pattern resolves to an existing path under repo
    present = {v: 0 for v in versions}
    for pat in patterns:
        if not isinstance(pat, str):
            continue
        p = pat.replace("{DAY}", day)
        ap = (repo / p).resolve()
        # only count if path exists AND is under repo
        try:
            ap.relative_to(repo)
        except Exception:
            raise SystemExit(f"FAIL: pattern escapes repo: spine={name} path={ap}")
        # bucket by version substring
        for v in versions:
            if f"_{v}/" in p:
                if ap.exists():
                    present[v] += 1

    nonzero = [v for v,c in present.items() if c > 0]
    if len(nonzero) == 0:
        # No data for any version: allowed (upstream may be missing); other gates will fail closed if required.
        continue
    if len(nonzero) > 1:
        print(f"FAIL: spine split-brain detected for day={day} spine={name} present_versions={nonzero} counts={present}")
        fail = True
    else:
        # If the only present version is not the active version, fail closed
        only = nonzero[0]
        if only != active:
            print(f"FAIL: spine wrong active version for day={day} spine={name} active={active} present={only} counts={present}")
            fail = True

if fail:
    raise SystemExit(2)

print(f"[c2-preflight] PASS: spine exclusivity holds for day={day}")
PY
