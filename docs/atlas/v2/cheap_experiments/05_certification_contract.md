# Experience Loop Certification Contract

The graph-facing canonical evidence artifact for aegis_experience_loop_v1 is:

reports/aegis_experience_loop_v1/{day}/experience_loop_certification.v1.json

The supporting runtime data ledger is:

events/aegis_experience_loop_v1/{day}/experience_events.jsonl

Both may exist for the same day. The report JSON is the verified graph evidence contract declared by the runtime_truth_kernel module. The JSONL event ledger is append-only supporting runtime data used to build or audit the certification.

Do not delete either artifact during reconciliation. If both are present, preserve both and treat the report JSON as canonical for graph readiness.
