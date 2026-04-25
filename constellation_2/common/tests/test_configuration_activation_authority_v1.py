from __future__ import annotations

from pathlib import Path

from constellation_2.common.configuration_activation_authority_v1 import (
    CONFIGURATION_STATE_SCHEMA,
    POLICY_SNAPSHOT_ARTIFACT_ID,
    POLICY_SNAPSHOT_SCHEMA,
    REQUIRED_GOVERNANCE_UTILITY_RELPATHS,
    WRITER_ID,
    run_configuration_activation_authority_v1,
)
from constellation_2.common.constitutional_runtime_v1 import (
    FINALITY_FINALIZED,
    assert_constitutional_writer_allowed_v1,
    build_artifact_dependency_declaration_v1,
    build_governed_artifact_lineage_v1,
    resolve_constitutional_artifact_path_v1,
    validate_governed_artifact_payload_v1,
)
from constellation_2.common.paper_session_fact_plane_v1 import (
    atomic_write_validated_json_v1,
    read_validated_surface_v1,
    repo_git_sha_v1,
    sha256_file_v1,
)
from constellation_2.phaseD.lib.canon_json_v1 import canonical_hash_for_c2_artifact_v1


REPO_ROOT = Path(__file__).resolve().parents[3]


def _governance_utility_refs(*, include_all: bool = True) -> list[dict[str, str]]:
    relpaths = list(REQUIRED_GOVERNANCE_UTILITY_RELPATHS)
    if not include_all:
        relpaths = relpaths[:-1]
    rows: list[dict[str, str]] = []
    for relpath in relpaths:
        path = (REPO_ROOT / relpath).resolve()
        rows.append(
            {
                "logical_name": relpath,
                "path": str(path),
                "sha256": sha256_file_v1(path),
            }
        )
    return rows


def _source_document_ref(path: Path, *, logical_name: str) -> dict[str, str]:
    return {
        "logical_name": logical_name,
        "path": str(path.resolve()),
        "sha256": sha256_file_v1(path),
        "document_class": "configuration_source",
    }


def _write_policy_snapshot(
    *,
    truth_root: Path,
    generated_at_utc: str,
    effective_at_utc: str,
    logical_name: str,
    document_text: str,
    include_all_utilities: bool = True,
) -> Path:
    contract = assert_constitutional_writer_allowed_v1(REPO_ROOT, POLICY_SNAPSHOT_ARTIFACT_ID, WRITER_ID)
    source_dir = (truth_root / "inputs").resolve()
    source_dir.mkdir(parents=True, exist_ok=True)
    source_path = (source_dir / f"{logical_name}.json").resolve()
    source_path.write_text(document_text, encoding="utf-8")
    source_refs = [_source_document_ref(source_path, logical_name=logical_name)]
    utility_refs = _governance_utility_refs(include_all=include_all_utilities)
    policy_snapshot_id = canonical_hash_for_c2_artifact_v1(
        {
            "artifact_id": POLICY_SNAPSHOT_ARTIFACT_ID,
            "generated_at_utc": generated_at_utc,
            "effective_at_utc": effective_at_utc,
            "source_document_refs": source_refs,
            "governance_utility_refs": utility_refs,
        }
    )
    dependency_decl = build_artifact_dependency_declaration_v1(
        artifact_type=POLICY_SNAPSHOT_ARTIFACT_ID,
        artifact_class=str(contract.get("artifact_class") or "").strip(),
        authority_id=POLICY_SNAPSHOT_ARTIFACT_ID,
        declared_dependency_artifacts=list(contract.get("required_upstream_dependencies") or []),
        dependency_refs=[],
    )
    lineage = build_governed_artifact_lineage_v1(
        artifact_type=POLICY_SNAPSHOT_ARTIFACT_ID,
        artifact_version="v1",
        artifact_class=str(contract.get("artifact_class") or "").strip(),
        authority_id=POLICY_SNAPSHOT_ARTIFACT_ID,
        producer_id=WRITER_ID,
        generated_at_utc=generated_at_utc,
        effective_at_utc=effective_at_utc,
        finality_state=FINALITY_FINALIZED,
        input_artifact_refs=[],
        policy_snapshot_refs=[],
        code_version=repo_git_sha_v1(),
        run_id=f"policy_snapshot:{policy_snapshot_id}",
    )
    payload = {
        "schema_id": "configuration_policy_snapshot.v1",
        "schema_version": "v1",
        "policy_snapshot_id": policy_snapshot_id,
        "authority_owner": WRITER_ID,
        "generated_at_utc": generated_at_utc,
        "effective_at_utc": effective_at_utc,
        "activation_scope": "runtime",
        "source_document_refs": source_refs,
        "governance_utility_refs": utility_refs,
        "policy_digest_sha256": canonical_hash_for_c2_artifact_v1(
            {
                "source_document_refs": source_refs,
                "governance_utility_refs": utility_refs,
            }
        ),
        "closure_state": "COMPLETE",
        "blocked_reason_codes": [],
        "constitutional_dependency_declaration": dependency_decl,
        "constitutional_lineage": lineage,
    }
    path = resolve_constitutional_artifact_path_v1(
        repo_root=REPO_ROOT,
        artifact_id=POLICY_SNAPSHOT_ARTIFACT_ID,
        day_utc=effective_at_utc[:10],
        canonical_truth_root=truth_root,
        extra_variables={"policy_snapshot_id": policy_snapshot_id},
    )
    atomic_write_validated_json_v1(
        path=path,
        payload=payload,
        schema_relpath=POLICY_SNAPSHOT_SCHEMA,
    )
    validate_governed_artifact_payload_v1(
        repo_root=REPO_ROOT,
        artifact_id=POLICY_SNAPSHOT_ARTIFACT_ID,
        payload=payload,
        required_finality_states=[FINALITY_FINALIZED],
    )
    return path


def test_configuration_activation_happy_path_first_and_superseding(tmp_path: Path) -> None:
    truth_root = (tmp_path / "truth").resolve()
    first_policy_path = _write_policy_snapshot(
        truth_root=truth_root,
        generated_at_utc="2026-04-18T13:00:00Z",
        effective_at_utc="2026-04-18T13:00:00Z",
        logical_name="config_alpha",
        document_text='{"mode":"alpha","risk":"low"}\n',
    )

    first_result = run_configuration_activation_authority_v1(
        policy_snapshot_path=first_policy_path,
        truth_root=truth_root,
    )

    assert first_result.validation_result_ref.payload["validation_status"] == "PASS"
    assert first_result.compiled_active_config_ref is not None
    assert first_result.compile_result_ref is not None
    assert first_result.review_diff_ref is not None
    assert first_result.activation_transaction_ref is not None
    assert first_result.configuration_state_ref is not None
    assert first_result.review_diff_ref.payload["review_kind"] == "FIRST_ACTIVATION"
    assert first_result.activation_transaction_ref.payload["activation_kind"] == "FIRST_ACTIVATION"
    assert first_result.configuration_state_ref.payload["activation_kind"] == "FIRST_ACTIVATION"
    assert first_result.configuration_state_ref.payload["compiled_active_config_ref"]["path"] == str(first_result.compiled_active_config_ref.path)
    assert first_result.configuration_state_ref.payload["compiled_active_config_ref"]["sha256"] == first_result.compiled_active_config_ref.sha256
    assert first_result.configuration_state_ref.payload["configuration_policy_snapshot_ref"]["path"] == str(first_policy_path.resolve())
    assert first_result.configuration_state_ref.payload["configuration_activation_transaction_ref"]["path"] == str(first_result.activation_transaction_ref.path)

    for ref, artifact_id, finality_states in (
        (first_result.compiled_active_config_ref, "compiled_active_config_v1", [FINALITY_FINALIZED]),
        (first_result.compile_result_ref, "configuration_compile_result_v1", [FINALITY_FINALIZED]),
        (first_result.review_diff_ref, "configuration_review_diff_v1", [FINALITY_FINALIZED]),
        (first_result.activation_transaction_ref, "configuration_activation_transaction_v1", [FINALITY_FINALIZED]),
        (first_result.configuration_state_ref, "configuration_state_v1", ["provisional", "finalized"]),
    ):
        assert ref is not None
        validate_governed_artifact_payload_v1(
            repo_root=REPO_ROOT,
            artifact_id=artifact_id,
            payload=ref.payload,
            required_finality_states=finality_states,
        )
        assert "constitutional_dependency_declaration" in ref.payload
        assert "constitutional_lineage" in ref.payload

    second_policy_path = _write_policy_snapshot(
        truth_root=truth_root,
        generated_at_utc="2026-04-18T14:00:00Z",
        effective_at_utc="2026-04-18T14:00:00Z",
        logical_name="config_alpha",
        document_text='{"mode":"beta","risk":"high"}\n',
    )
    second_result = run_configuration_activation_authority_v1(
        policy_snapshot_path=second_policy_path,
        truth_root=truth_root,
    )

    assert second_result.validation_result_ref.payload["validation_status"] == "PASS"
    assert second_result.compiled_active_config_ref is not None
    assert second_result.activation_transaction_ref is not None
    assert second_result.configuration_state_ref is not None
    assert second_result.review_diff_ref is not None
    assert second_result.review_diff_ref.payload["review_kind"] == "SUPERSEDING_ACTIVATION"
    assert second_result.review_diff_ref.payload["prior_configuration_state_ref"]["path"] == str(first_result.configuration_state_ref.path)
    assert second_result.review_diff_ref.payload["prior_compiled_active_config_ref"]["path"] == str(first_result.compiled_active_config_ref.path)
    assert second_result.activation_transaction_ref.payload["activation_kind"] == "SUPERSEDING_ACTIVATION"
    assert second_result.activation_transaction_ref.payload["prior_configuration_state_ref"]["path"] == str(first_result.configuration_state_ref.path)
    assert second_result.configuration_state_ref.payload["prior_configuration_state_ref"]["path"] == str(first_result.configuration_state_ref.path)
    assert second_result.configuration_state_ref.payload["compiled_active_config_ref"]["path"] == str(second_result.compiled_active_config_ref.path)
    assert second_result.configuration_state_ref.payload["compiled_active_config_ref"]["sha256"] == second_result.compiled_active_config_ref.sha256
    assert second_result.configuration_state_ref.payload["configuration_policy_snapshot_ref"]["path"] == str(second_policy_path.resolve())
    assert second_result.configuration_state_ref.payload["configuration_activation_transaction_ref"]["path"] == str(second_result.activation_transaction_ref.path)

    current_ref = read_validated_surface_v1(
        path=second_result.configuration_state_ref.path,
        schema_relpath=CONFIGURATION_STATE_SCHEMA,
    )
    assert current_ref.payload == second_result.configuration_state_ref.payload


def test_configuration_activation_blocks_without_required_governance_utility(tmp_path: Path) -> None:
    truth_root = (tmp_path / "truth").resolve()
    policy_path = _write_policy_snapshot(
        truth_root=truth_root,
        generated_at_utc="2026-04-18T15:00:00Z",
        effective_at_utc="2026-04-18T15:00:00Z",
        logical_name="config_blocked",
        document_text='{"mode":"blocked"}\n',
        include_all_utilities=False,
    )

    result = run_configuration_activation_authority_v1(
        policy_snapshot_path=policy_path,
        truth_root=truth_root,
    )

    assert result.validation_result_ref.payload["validation_status"] == "FAIL"
    assert "GOVERNANCE_UTILITY_SET_MISMATCH" in result.validation_result_ref.payload["blocking_reason_codes"]
    assert result.compiled_active_config_ref is None
    assert result.compile_result_ref is None
    assert result.review_diff_ref is None
    assert result.activation_transaction_ref is None
    assert result.configuration_state_ref is None

    current_path = resolve_constitutional_artifact_path_v1(
        repo_root=REPO_ROOT,
        artifact_id="configuration_state_v1",
        day_utc="2026-04-18",
        canonical_truth_root=truth_root,
    )
    compiled_root = (truth_root / "compiled_active_config_v1").resolve()
    activation_root = (truth_root / "reports" / "configuration_activation_transaction_v1").resolve()
    assert not compiled_root.exists()
    assert not activation_root.exists()
    assert not current_path.exists()
