from __future__ import annotations

import sys
from pathlib import Path


SOURCE_ROOT = Path("/home/node/constellation").resolve()
if str(SOURCE_ROOT) not in sys.path:
    sys.path.insert(0, str(SOURCE_ROOT))


ACTIVE_RUNTIME_FILES = (
    SOURCE_ROOT / "ops/run/c2_paper_day_orchestrator_systemd_entry_v1.sh",
    SOURCE_ROOT / "ops/tools/run_paper_session_bootstrap_v1.py",
    SOURCE_ROOT / "ops/tools/run_pre_open_materializer_v1.py",
    SOURCE_ROOT / "ops/tools/run_session_authority_v1.py",
    SOURCE_ROOT / "ops/tools/run_day_open_attempt_v1.py",
    SOURCE_ROOT / "ops/tools/run_c2_multi_sleeve_orchestrator_v1.py",
    SOURCE_ROOT / "ops/tools/run_c2_paper_day_orchestrator_v2.py",
    SOURCE_ROOT / "constellation_2/common/runtime_contract_v1.py",
    SOURCE_ROOT / "constellation_2/common/runtime_path_authority_v1.py",
    SOURCE_ROOT / "constellation_2/common/runtime_identity_v1.py",
    SOURCE_ROOT / "constellation_2/common/truth_root_v1.py",
)

FORBIDDEN_REPO_LOCAL_TRUTH_PATTERNS = (
    "constellation_2/runtime/truth",
    "constellation_2/runtime/truth_sleeves",
    "/home/node/constellation/constellation_2/runtime/truth",
    "/home/node/constellation/constellation_2/runtime/truth_sleeves",
    "/home/node/constellation_2_runtime/constellation_2/runtime/truth",
    "/home/node/constellation_2_runtime/constellation_2/runtime/truth_sleeves",
)


def test_no_repo_local_truth_path_appears_in_active_runtime_files() -> None:
    offenders: list[str] = []
    for path in ACTIVE_RUNTIME_FILES:
        text = path.read_text(encoding="utf-8")
        for pattern in FORBIDDEN_REPO_LOCAL_TRUTH_PATTERNS:
            if pattern in text:
                offenders.append(f"{path.relative_to(SOURCE_ROOT)} -> {pattern}")
    assert not offenders, "repo-local truth path literal found in active runtime files:\n" + "\n".join(offenders)
