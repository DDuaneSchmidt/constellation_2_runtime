"""
test_phaseD_identity_inputs_written_v1.py

Acceptance (Phase D Evidence Writer):
- When identity inputs (order_plan, binding_record, mapping_ledger_record) are provided to the
  Phase D evidence writer, they are written immutably into the submission directory.
- Refuses overwrite / non-empty out_dir on rerun.

Execution:
  python3 -m constellation_2.phaseD.tests.test_phaseD_identity_inputs_written_v1
"""

from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path

from constellation_2.phaseC.tools.c2_submit_preflight_offline_v1 import main as phasec_main
from constellation_2.phaseD.lib.canon_json_v1 import canonical_json_bytes_v1
from constellation_2.phaseD.lib.evidence_writer_v1 import EvidenceWriteError, write_phased_success_outputs_v1
from constellation_2.phaseD.lib.validate_against_schema_v1 import validate_against_repo_schema_v1

REPO_ROOT = Path(__file__).resolve().parents[3]
SAMPLES = REPO_ROOT / "constellation_2" / "acceptance" / "samples"


def _load_json(path: Path) -> dict:
    with path.open("r", encoding="utf-8") as f:
        obj = json.load(f)
    assert isinstance(obj, dict)
    return obj


class TestPhaseDIdentityInputsWrittenV1(unittest.TestCase):
    def test_identity_inputs_written_and_overwrite_refused(self) -> None:
        # Step 1: Generate schema-valid Phase C outputs (order_plan, mapping_ledger_record, binding_record).
        with tempfile.TemporaryDirectory() as td:
            td_path = Path(td).resolve()
            phasec_out = td_path / "phasec_out"
            phasec_out.mkdir(parents=True, exist_ok=False)

            base_args = [
                "--intent",
                str(SAMPLES / "sample_options_intent.v2.json"),
                "--chain_snapshot",
                str(SAMPLES / "sample_chain_snapshot.v1.json"),
                "--freshness_cert",
                str(SAMPLES / "sample_freshness_certificate.v1.json"),
                "--eval_time_utc",
                "2026-02-13T21:52:00Z",
                "--tick_size",
                "0.01",
                "--out_dir",
                str(phasec_out),
            ]
            rc = phasec_main(base_args)
            self.assertEqual(rc, 0)

            order_plan = _load_json(phasec_out / "order_plan.v1.json")
            mapping_ledger_record = _load_json(phasec_out / "mapping_ledger_record.v1.json")
            binding_record = _load_json(phasec_out / "binding_record.v1.json")

            # Sanity: these must validate against their schemas (fail closed if not).
            validate_against_repo_schema_v1(order_plan, REPO_ROOT, "constellation_2/schemas/order_plan.v1.schema.json")
            validate_against_repo_schema_v1(mapping_ledger_record, REPO_ROOT, "constellation_2/schemas/mapping_ledger_record.v1.schema.json")
            validate_against_repo_schema_v1(binding_record, REPO_ROOT, "constellation_2/schemas/binding_record.v1.schema.json")

            # Step 2: Minimal schema-valid BrokerSubmissionRecord v2 and ExecutionEventRecord v1.
            bsr = {
                "schema_id": "broker_submission_record",
                "schema_version": "v2",
                "submission_id": "1" * 64,
                "submitted_at_utc": "2026-02-14T00:00:00Z",
                "binding_hash": binding_record.get("canonical_json_hash") or ("2" * 64),
                "broker": {"name": "INTERACTIVE_BROKERS", "environment": "PAPER"},
                "status": "SUBMITTED",
                "broker_ids": {"order_id": 1, "perm_id": 1},
                "error": None,
                "canonical_json_hash": "3" * 64,
            }
            validate_against_repo_schema_v1(bsr, REPO_ROOT, "constellation_2/schemas/broker_submission_record.v2.schema.json")

            evt = {
                "schema_id": "execution_event_record",
                "schema_version": "v1",
                "created_at_utc": "2026-02-14T00:00:00Z",
                "event_time_utc": "2026-02-14T00:00:00Z",
                "binding_hash": bsr["binding_hash"],
                "broker_submission_hash": bsr["canonical_json_hash"],
                "broker_order_id": "1",
                "perm_id": "1",
                "status": "SUBMITTED",
                "filled_qty": 0,
                "avg_price": "0",
                "raw_broker_status": None,
                "raw_payload_digest": None,
                "sequence_num": None,
                "canonical_json_hash": "4" * 64,
                "upstream_hash": None,
            }
            validate_against_repo_schema_v1(evt, REPO_ROOT, "constellation_2/schemas/execution_event_record.v1.schema.json")

            # Step 3: Write submission evidence into a fresh out_dir.
            out_dir = td_path / "submission_out"
            write_phased_success_outputs_v1(
                out_dir,
                broker_submission_record=bsr,
                execution_event_record=evt,
                order_plan=order_plan,
                binding_record=binding_record,
                mapping_ledger_record=mapping_ledger_record,
            )

            # Step 4: Proof files exist.
            p_sub = out_dir / "broker_submission_record.v2.json"
            p_evt = out_dir / "execution_event_record.v1.json"
            p_plan = out_dir / "order_plan.v1.json"
            p_bind = out_dir / "binding_record.v1.json"
            p_map = out_dir / "mapping_ledger_record.v1.json"

            for p in (p_sub, p_evt, p_plan, p_bind, p_map):
                self.assertTrue(p.exists() and p.is_file())

            # Step 5: Determinism proof: each file is JSON-object and canon-json encodable.
            for p in (p_sub, p_evt, p_plan, p_bind, p_map):
                obj = _load_json(p)
                _ = canonical_json_bytes_v1(obj)

            # Step 6: Overwrite/partial rerun must hard fail because out_dir is not empty.
            with self.assertRaises(EvidenceWriteError):
                write_phased_success_outputs_v1(
                    out_dir,
                    broker_submission_record=bsr,
                    execution_event_record=evt,
                    order_plan=order_plan,
                    binding_record=binding_record,
                    mapping_ledger_record=mapping_ledger_record,
                )


if __name__ == "__main__":
    unittest.main()
