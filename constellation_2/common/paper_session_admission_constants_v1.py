from __future__ import annotations

from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]

PAPER_SESSION_DEFINITION_SCHEMA_RELPATH_V1 = (
    "governance/04_DATA/SCHEMAS/C2/REPORTS/paper_session_definition.v1.schema.json"
)
PAPER_SESSION_DEPENDENCY_GRAPH_SCHEMA_RELPATH_V1 = (
    "governance/04_DATA/SCHEMAS/C2/REPORTS/paper_session_dependency_graph.v1.schema.json"
)
PAPER_SESSION_BLOCKER_LEDGER_SCHEMA_RELPATH_V1 = (
    "governance/04_DATA/SCHEMAS/C2/REPORTS/paper_session_blocker_ledger.v1.schema.json"
)
PAPER_SESSION_CLOSURE_SCHEMA_RELPATH_V1 = (
    "governance/04_DATA/SCHEMAS/C2/REPORTS/paper_session_closure.v1.schema.json"
)
PAPER_SESSION_PRODUCER_ATTEMPTS_SCHEMA_RELPATH_V1 = (
    "governance/04_DATA/SCHEMAS/C2/REPORTS/paper_session_producer_attempts.v1.schema.json"
)
PAPER_SESSION_ENVELOPE_SCHEMA_RELPATH_V1 = (
    "governance/04_DATA/SCHEMAS/C2/REPORTS/paper_session_envelope.v1.schema.json"
)
PAPER_SESSION_ADMISSION_CERTIFICATE_SCHEMA_RELPATH_V1 = (
    "governance/04_DATA/SCHEMAS/C2/REPORTS/paper_session_admission_certificate.v1.schema.json"
)
PAPER_SESSION_DIVERGENCE_SCHEMA_RELPATH_V1 = (
    "governance/04_DATA/SCHEMAS/C2/REPORTS/paper_session_divergence.v1.schema.json"
)

PAPER_SESSION_CONTRACT_PATHS_V1 = (
    "governance/05_CONTRACTS/C2/paper_session_definition_v1.contract.md",
    "governance/05_CONTRACTS/C2/paper_session_dependency_graph_v1.contract.md",
    "governance/05_CONTRACTS/C2/paper_session_closure_v1.contract.md",
    "governance/05_CONTRACTS/C2/paper_session_blocker_ledger_v1.contract.md",
    "governance/05_CONTRACTS/C2/paper_session_admission_producer_map_v1.contract.md",
    "governance/05_CONTRACTS/C2/paper_session_producer_attempts_v1.contract.md",
    "governance/05_CONTRACTS/C2/paper_session_envelope_v1.contract.md",
    "governance/05_CONTRACTS/C2/paper_session_admission_certificate_v1.contract.md",
    "governance/05_CONTRACTS/C2/paper_session_divergence_v1.contract.md",
    "governance/05_CONTRACTS/C2/paper_day_operator_entrypoint_v1.contract.md",
)

PAPER_SESSION_REPORT_FAMILIES_V1 = (
    "paper_session_definition_v1",
    "paper_session_dependency_graph_v1",
    "paper_session_blocker_ledger_v1",
    "paper_session_closure_v1",
    "paper_session_producer_attempts_v1",
    "paper_session_envelope_v1",
    "paper_session_admission_certificate_v1",
    "paper_session_divergence_v1",
)


def canonical_paper_session_report_path(*, truth_root: Path, artifact_family: str, day_utc: str, filename: str) -> Path:
    return (truth_root / "reports" / artifact_family / day_utc / filename).resolve()
