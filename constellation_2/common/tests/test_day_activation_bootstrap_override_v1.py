from __future__ import annotations

from pathlib import Path
import sys

SOURCE_ROOT = Path("/home/node/constellation")
if str(SOURCE_ROOT) not in sys.path:
    sys.path.insert(0, str(SOURCE_ROOT))

from constellation_2.common.day_activation_authority_v1 import (  # noqa: E402
    DayActivationContext,
    STATUS_PRESENT,
    _evaluate_dependencies,
)


def _write_json(path: Path, payload: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(payload + "\n", encoding="utf-8")


def test_day_activation_bootstrap_override_relaxes_authority_head_and_authorization(monkeypatch, tmp_path: Path) -> None:
    canonical_truth_root = (tmp_path / "truth").resolve()
    execution_truth_root = (tmp_path / "truth_sleeves" / "PRIMARY" / "PAPER").resolve()
    day_utc = "2026-04-20"

    admission_path = canonical_truth_root / "target_day_admission_v1" / f"{day_utc}.json"
    _write_json(
        admission_path,
        '{"admission_status":"ADMIT","binding":true,"target_day":"2026-04-20","mode":"PAPER_BOOTSTRAP"}',
    )
    authority_head_path = canonical_truth_root / "run_pointer_v2" / "canonical_authority_head.v1.json"
    _write_json(
        authority_head_path,
        '{"status":"PASS","authoritative":true,"day_utc":"2026-04-17"}',
    )
    auth_verdict_path = execution_truth_root / "reports" / "authorization_gate_verdict_v1" / day_utc / "authorization_gate_verdict.v1.json"
    _write_json(auth_verdict_path, '{"status":"FAIL"}')

    ctx = DayActivationContext(
        repo_root=SOURCE_ROOT,
        canonical_truth_root=canonical_truth_root,
        truth_sleeves_root=(tmp_path / "truth_sleeves").resolve(),
        execution_truth_root=execution_truth_root,
        day_utc=day_utc,
        sleeve_id="PRIMARY",
        environment="PAPER",
        ib_account="DUO847203",
        operation_type="fresh_paper_entry_v1",
        context_hash="ctx",
    )
    manifest = {
        "dependencies": [
            {
                "dependency_id": "target_day_admission_v1",
                "path_pattern": "{canonical_truth_root}/target_day_admission_v1/{day_utc}.json",
                "required": True,
                "advisory_only": False,
                "post_submit_only": False,
                "owner_ref": "owner",
                "upstream_dependency_ids": [],
            },
            {
                "dependency_id": "canonical_authority_head_v1",
                "path_pattern": "{canonical_truth_root}/run_pointer_v2/canonical_authority_head.v1.json",
                "required": True,
                "advisory_only": False,
                "post_submit_only": False,
                "owner_ref": "owner",
                "upstream_dependency_ids": ["target_day_admission_v1"],
            },
            {
                "dependency_id": "authorization_gate_verdict_v1",
                "path_pattern": "{execution_truth_root}/reports/authorization_gate_verdict_v1/{day_utc}/authorization_gate_verdict.v1.json",
                "required": True,
                "advisory_only": False,
                "post_submit_only": False,
                "owner_ref": "owner",
                "upstream_dependency_ids": ["target_day_admission_v1", "canonical_authority_head_v1"],
            },
        ]
    }

    results = _evaluate_dependencies(ctx, manifest)

    assert results["target_day_admission_v1"]["status"] == STATUS_PRESENT
    assert results["canonical_authority_head_v1"]["status"] == STATUS_PRESENT
    assert results["canonical_authority_head_v1"]["detail"] == "BOOTSTRAP_OVERRIDE:canonical_authority_head_v1"
    assert results["authorization_gate_verdict_v1"]["status"] == STATUS_PRESENT
    assert results["authorization_gate_verdict_v1"]["detail"] == "BOOTSTRAP_OVERRIDE:authorization_gate_verdict_v1"

