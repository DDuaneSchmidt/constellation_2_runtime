# Aegis Lite Operational Spine

Aegis Lite is the canonical manual PAPER trading path.

The operator-facing source of truth is the current day Aegis Lite EOD artifact set under runtime truth:

- `reports/aegis_lite_eod_report_v1/<day>/<run_id>/aegis_lite_eod_report.v1.json`
- `reports/operator_execution_queue_v1/<day>/<run_id>/operator_execution_queue.v1.json`
- `reports/edge_cluster_v1/<day>/<run_id>/edge_cluster.v1.json`
- `reports/sleeve_edge_overlap_review_v1/<day>/<run_id>/sleeve_edge_overlap_review.v1.json`
- `reports/aegis_lite_operating_status_v1/<day>/aegis_lite_operating_status.v1.json`

The safe manual run command is:

```bash
python3 ops/tools/run_aegis_lite_eod_pipeline_v1.py --day_utc YYYY-MM-DD --environment PAPER --manual-only --allow-not-ready-exit-zero
```

Release integrity should be checked before relying on the UI:

```bash
python3 ops/tools/check_aegis_release_integrity_v1.py
```

This path never submits broker orders, never touches transmit control, and does not require IB state. If no approved promoted candidates are available, it still writes an advisory or not-ready report with no executable queue items.

Initial supervised PAPER usage is capped at 1 trade/day until the first manual feedback artifacts prove the operator loop is clean.
