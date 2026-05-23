from __future__ import annotations

from pathlib import Path
from typing import Any

from constellation_2.common.global_context_authority_v1 import run_global_context_authority_v1
from ops.aegis.event_append_transaction_v1 import contract_input_hashes_for_paths_v1, emit_artifact_evidence_transaction_v1, sha256_file_v1

PRODUCER_ID = "ops/tools/build_global_context_package_v1.py"
PRODUCER_VERSION = "v1"


def build_and_write_global_context_package_v1(
    *,
    truth_root: Path | str,
    day_utc: str,
    operation_type: str = "fresh_paper_entry_v1",
    sleeve_id: str = "PRIMARY",
    environment: str = "PAPER",
    ib_account: str = "DUO847203",
    emit_events: bool = True,
) -> dict[str, Any]:
    repo_root = Path(__file__).resolve().parents[2]
    result = run_global_context_authority_v1(
        repo_root=repo_root,
        operation_type=operation_type,
        day_utc=day_utc,
        sleeve_id=sleeve_id,
        environment=environment,
        ib_account=ib_account,
        materialize=True,
        emit_package=True,
    )
    build_path = Path(result["build_path"])
    package_path = Path(result["package_path"]) if result.get("package_path") is not None else None
    validation_status = "VALID" if result["build_obj"].get("closure_status") == "COMPLETE" and package_path is not None else "REJECTED"
    root = Path(truth_root).expanduser().resolve()
    if emit_events:
        emit_artifact_evidence_transaction_v1(
            truth_root=root,
            day_utc=day_utc,
            artifact_path=build_path,
            payload=result["build_obj"],
            producer_id=PRODUCER_ID,
            producer_version=PRODUCER_VERSION,
            created_at_utc=str(result["build_obj"].get("generated_utc") or f"{day_utc}T00:00:00Z"),
            input_hashes=contract_input_hashes_for_paths_v1([build_path], extra={"context_hash": result["build_obj"].get("context_hash", "")}),
            validation_status=validation_status,
        )
        if package_path is not None and result.get("package_obj") is not None:
            emit_artifact_evidence_transaction_v1(
                truth_root=root,
                day_utc=day_utc,
                artifact_path=package_path,
                payload=result["package_obj"],
                producer_id=PRODUCER_ID,
                producer_version=PRODUCER_VERSION,
                created_at_utc=str(result["package_obj"].get("sealed_utc") or f"{day_utc}T00:00:00Z"),
                input_hashes=contract_input_hashes_for_paths_v1([build_path], extra={"context_hash": result["build_obj"].get("context_hash", "")}),
                validation_status=validation_status,
            )
    return {
        "validation_status": validation_status,
        "closure_status": result["build_obj"].get("closure_status"),
        "build_path": str(build_path),
        "build_hash": sha256_file_v1(build_path),
        "package_path": str(package_path) if package_path is not None else "",
        "package_hash": sha256_file_v1(package_path) if package_path is not None and package_path.exists() else "",
        "first_real_blocker": result["build_obj"].get("first_real_blocker"),
        "broker_execution_allowed": False,
        "autonomous_execution_allowed": False,
        "trade_advice_allowed": False,
    }
