from __future__ import annotations

from pathlib import Path

DOMAIN_NAME = "legacy_holdings"
DOMAIN_SCHEMA_DIR = "LEGACY_HOLDINGS"
AUTHORITY_STATUS_CANDIDATE = "candidate"
AUTHORITY_STATUS_AUTHORITATIVE = "authoritative"

REPO_ROOT = Path(__file__).resolve().parents[3]
SCHEMA_RELPATH_NORMALIZED_HOLDINGS_ARTIFACT_V1 = (
    "governance/04_DATA/SCHEMAS/C2/LEGACY_HOLDINGS/normalized_holdings_artifact.v1.schema.json"
)
SCHEMA_RELPATH_LEGACY_BOND_RISK_SUMMARY_V1 = (
    "governance/04_DATA/SCHEMAS/C2/LEGACY_HOLDINGS/legacy_bond_risk_summary.v1.schema.json"
)
