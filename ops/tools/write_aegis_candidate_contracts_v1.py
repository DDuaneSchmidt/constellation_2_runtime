#!/usr/bin/env python3
from __future__ import annotations

import argparse
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from ops.aegis.entry_reference_price_certification_v1 import build_entry_reference_price_certification_v1, write_entry_reference_price_certification_v1  # noqa: E402
from ops.aegis.signal_evidence_graph_v1 import build_signal_evidence_graph_v1, write_signal_evidence_graph_v1  # noqa: E402
from ops.aegis.candidate_contracts_v1 import build_candidate_contracts_v1, write_candidate_contracts_v1  # noqa: E402


def main() -> int:
    parser = argparse.ArgumentParser(prog="write_aegis_candidate_contracts_v1")
    parser.add_argument("--truth-root", dest="truth_root", required=True)
    parser.add_argument("--day", dest="day_utc", required=True)
    args = parser.parse_args()
    cert_payload = build_entry_reference_price_certification_v1(truth_root=Path(args.truth_root), day_utc=str(args.day_utc))
    write_entry_reference_price_certification_v1(truth_root=Path(args.truth_root), day_utc=str(args.day_utc), payload=cert_payload)
    graph_payload = build_signal_evidence_graph_v1(truth_root=Path(args.truth_root), day_utc=str(args.day_utc), repo_root=REPO_ROOT)
    write_signal_evidence_graph_v1(truth_root=Path(args.truth_root), day_utc=str(args.day_utc), payload=graph_payload)
    payload = build_candidate_contracts_v1(truth_root=Path(args.truth_root), day_utc=str(args.day_utc), repo_root=REPO_ROOT)
    paths = write_candidate_contracts_v1(truth_root=Path(args.truth_root), day_utc=str(args.day_utc), payload=payload)
    print(paths["json"])
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
