from __future__ import annotations

import json
import sys
from pathlib import Path
from types import SimpleNamespace

import pytest

SOURCE_ROOT = Path("/home/node/constellation")
if str(SOURCE_ROOT) not in sys.path:
    sys.path.insert(0, str(SOURCE_ROOT))

from constellation_2.phaseD.lib.submit_boundary_paper_v4 import (  # noqa: E402
    RC_FINAL_SNAPSHOT_LINEAGE_MISMATCH,
    SubmitBoundaryV4Error,
    _combo_preview_blocker,
    _enforce_final_snapshot_lineage_gate,
    _sha256_file,
    _write_ib_order_payload_artifact,
)


DAY = "2026-04-29"


def _write_json(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, sort_keys=True, separators=(",", ":")) + "\n", encoding="utf-8")


def _seed_lineage(tmp_path: Path, *, structure_snapshot: Path | None = None) -> tuple[Path, Path, Path, Path, dict, dict, list[str]]:
    truth = tmp_path / "truth"
    execution = tmp_path / "truth_sleeves" / "PRIMARY" / "PAPER"
    phasec = execution / "phaseC_preflight_v1" / DAY / "attempt" / "intent"
    snapshot = execution / "options_chain_snapshot_v1" / DAY / "capture" / "options_chain_snapshot.v1.json"
    cert = snapshot.parent / "freshness_certificate.v1.json"
    _write_json(snapshot, {"schema_id": "options_chain_snapshot", "schema_version": "v1", "day_utc": DAY})
    _write_json(cert, {"schema_id": "freshness_certificate", "schema_version": "v1", "day_utc": DAY})
    _write_json(
        truth / "reports" / "market_open_data_gate_v1" / DAY / "market_open_data_gate.v1.json",
        {
            "status": "PASS",
            "snapshot_path": str(snapshot),
            "freshness_certificate_path": str(cert),
        },
    )
    structure_snapshot = structure_snapshot or snapshot
    structure = {
        "status": "PASS",
        "market_open_data": {
            "snapshot_path": str(structure_snapshot),
            "freshness_certificate_path": str(cert),
        },
        "structure_decisions": [
            {
                "pricing_inputs": {
                    "snapshot_path": str(structure_snapshot),
                    "freshness_certificate_path": str(cert),
                }
            }
        ],
    }
    _write_json(truth / "reports" / "structure_decision_supply_v1" / DAY / "structure_decision_supply.v1.json", structure)
    _write_json(phasec / "structure_decision_supply.v1.json", structure)
    mapping = {"chain_snapshot_hash": _sha256_file(snapshot), "freshness_cert_hash": _sha256_file(cert)}
    binding = {"freshness_cert_hash": _sha256_file(cert)}
    return truth, execution, phasec, snapshot, mapping, binding, []


def test_matching_snapshot_lineage_allows_submit_readiness_to_proceed(tmp_path: Path) -> None:
    truth, execution, phasec, snapshot, mapping, binding, pointers = _seed_lineage(tmp_path)

    payload = _enforce_final_snapshot_lineage_gate(
        canonical_truth_root=truth,
        execution_truth_root=execution,
        day_utc=DAY,
        phasec_out_dir=phasec,
        mapping_obj=mapping,
        binding_obj=binding,
        pointers=pointers,
    )

    assert payload["status"] == "PASS"
    assert payload["latest_accepted_snapshot_path"] == str(snapshot.resolve())


def test_snapshot_mismatch_blocks_submit(tmp_path: Path) -> None:
    wrong_snapshot = tmp_path / "truth_sleeves" / "PRIMARY" / "PAPER" / "options_chain_snapshot_v1" / DAY / "old" / "options_chain_snapshot.v1.json"
    _write_json(wrong_snapshot, {"schema_id": "options_chain_snapshot", "schema_version": "v1", "day_utc": DAY})
    truth, execution, phasec, _snapshot, mapping, binding, pointers = _seed_lineage(tmp_path, structure_snapshot=wrong_snapshot)

    with pytest.raises(SubmitBoundaryV4Error, match=RC_FINAL_SNAPSHOT_LINEAGE_MISMATCH):
        _enforce_final_snapshot_lineage_gate(
            canonical_truth_root=truth,
            execution_truth_root=execution,
            day_utc=DAY,
            phasec_out_dir=phasec,
            mapping_obj=mapping,
            binding_obj=binding,
            pointers=pointers,
        )


def test_combo_preview_rejects_ib_error_201_before_transmit() -> None:
    whatif = SimpleNamespace(
        ok=False,
        detail="IB_ERROR_201_RISKLESS_COMBINATION",
        raw={
            "payload": {"routing": {"smart_combo_routing_params": [{"tag": "NonGuaranteed", "value": "1"}]}},
            "error": "Error 201: Riskless combination orders are not allowed.",
        },
    )

    assert _combo_preview_blocker(whatif) == "IB_ERROR_201_RISKLESS_COMBINATION"


def test_combo_preview_requires_smart_routing_params() -> None:
    whatif = SimpleNamespace(ok=True, detail="WHATIF_OK", raw={"payload": {"routing": {}}})

    assert _combo_preview_blocker(whatif) == "SMART_COMBO_ROUTING_PARAMS_MISSING"


def test_exact_ib_payload_is_persisted_before_transmit(tmp_path: Path) -> None:
    payload_path = _write_ib_order_payload_artifact(
        submission_dir=tmp_path / "submission",
        day_utc=DAY,
        ib_account="DUO847203",
        ib_host="127.0.0.1",
        ib_port=4002,
        ib_client_id=7,
        payload={
            "bag": {"secType": "BAG"},
            "order": {"action": "BUY"},
            "routing": {"smart_combo_routing_params": [{"tag": "NonGuaranteed", "value": "1"}]},
        },
        final_lineage_gate={"status": "PASS"},
    )

    payload = json.loads(payload_path.read_text(encoding="utf-8"))
    assert payload["payload"]["bag"]["secType"] == "BAG"
    assert payload["payload"]["order"]["action"] == "BUY"
    assert payload["canonical_json_hash"]
