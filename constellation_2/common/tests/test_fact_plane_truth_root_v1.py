from __future__ import annotations

import sys
from pathlib import Path

SOURCE_ROOT = Path("/home/node/constellation").resolve()
if str(SOURCE_ROOT) not in sys.path:
    sys.path.insert(0, str(SOURCE_ROOT))

from constellation_2.common.paper_session_fact_plane_v1 import resolve_fact_plane_truth_root_v1
from constellation_2.common.runtime_contract_v1 import resolve_canonical_truth_root


def test_fact_plane_truth_root_defaults_to_canonical_runtime_truth() -> None:
    assert resolve_fact_plane_truth_root_v1() == resolve_canonical_truth_root()


def test_fact_plane_truth_root_allows_explicit_override() -> None:
    override = Path("/tmp/c2-test-truth").resolve()
    assert resolve_fact_plane_truth_root_v1(str(override)) == override
