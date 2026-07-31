# Aegis Generated Hypothesis Paper Construction Repair Design V1

The repair places deterministic stop derivation at the candidate-contract boundary, before paper construction. Paper construction remains fail-closed and validates that `stop_price` exists before producing a constructed paper trade. Candidate-to-paper lifecycle remains the only path that materializes auto-promoted paper-position events, and `aegis_paper_position_ledger_v1` remains the authoritative source for paper observation.

Package 017 adds a read-only proof artifact that reports success or the exact deterministic blocker. It does not fabricate paper observations or downstream outcomes; it only verifies whether the normal lifecycle wrote the ledger.
