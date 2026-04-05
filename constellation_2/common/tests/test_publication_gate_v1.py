from pathlib import Path
import sys
import pytest

ROOT = Path(__file__).resolve().parents[3]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))
if '/home/node/constellation' not in sys.path:
    sys.path.insert(0, '/home/node/constellation')

from constellation_2.common.advisor_kernel.publication_gate import evaluate_publication


def test_publication_gate_blocks_view_as_decision() -> None:
    meta = {'produced_utc': '2030-01-20T00:00:00Z', 'day_utc': '2030-01-20', 'mode': 'PAPER', 'source_artifact_refs': ['artifact_ref:x']}
    with pytest.raises(ValueError):
        evaluate_publication(artifact_family='decision_memo_v1', artifact_ref='x', authority_class='view_authority', support_status='fully_supported', semantic_status='pass', upstream_publication_passed=True, lane='decision_lane', metadata=meta)
