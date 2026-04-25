from __future__ import annotations

import sys
from pathlib import Path

import pytest

SOURCE_ROOT = Path('/home/node/constellation')
if str(SOURCE_ROOT) not in sys.path:
    sys.path.insert(0, str(SOURCE_ROOT))

from constellation_2.phaseD.lib.submit_boundary_paper_v1 import run_submit_boundary_paper_v1  # noqa: E402
from constellation_2.phaseD.lib.submit_boundary_paper_v2 import run_submit_boundary_paper_v2  # noqa: E402
from constellation_2.phaseD.lib.submit_boundary_paper_v3 import run_submit_boundary_paper_v3  # noqa: E402
from constellation_2.phaseD.tools import c2_submit_paper_v1, c2_submit_paper_v2, c2_submit_paper_v3, c2_submit_paper_v4  # noqa: E402


def test_legacy_submit_boundaries_fail_closed() -> None:
    with pytest.raises(RuntimeError, match='LEGACY_EXECUTION_SUBMISSION_PATH_DISABLED'):
        run_submit_boundary_paper_v1(
            SOURCE_ROOT,
            phasec_out_dir=SOURCE_ROOT,
            phased_out_dir=SOURCE_ROOT,
            submissions_root=SOURCE_ROOT,
            eval_time_utc='2026-04-14T00:00:00Z',
            risk_budget_path=SOURCE_ROOT,
            engine_id=None,
            ib_host='127.0.0.1',
            ib_port=4002,
            ib_client_id=7,
        )

    with pytest.raises(RuntimeError, match='LEGACY_EXECUTION_SUBMISSION_PATH_DISABLED'):
        run_submit_boundary_paper_v2(
            repo_root=SOURCE_ROOT,
            eval_time_utc='2026-04-14T00:00:00Z',
            phasec_out_dir=SOURCE_ROOT,
            ib_host='127.0.0.1',
            ib_port=4002,
            ib_client_id=7,
            ib_account='DUO847203',
        )

    with pytest.raises(RuntimeError, match='LEGACY_EXECUTION_SUBMISSION_PATH_DISABLED'):
        run_submit_boundary_paper_v3(
            repo_root=SOURCE_ROOT,
            eval_time_utc='2026-04-14T00:00:00Z',
            phasec_out_dir=SOURCE_ROOT,
            ib_host='127.0.0.1',
            ib_port=4002,
            ib_client_id=7,
            ib_account='DUO847203',
        )


def test_legacy_submit_tools_fail_closed() -> None:
    for module in (c2_submit_paper_v1, c2_submit_paper_v2, c2_submit_paper_v3, c2_submit_paper_v4):
        with pytest.raises(SystemExit, match='LEGACY_EXECUTION_SUBMISSION_PATH_DISABLED'):
            module.main()
