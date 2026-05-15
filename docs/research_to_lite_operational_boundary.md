# Research To Lite Operational Boundary

Research Lab artifacts are advisory only.

A Research Lab idea cannot enter the operational Lite EOD report unless its sleeve is present in the promoted sleeve library with:

- `promotion_status=promoted`
- `approved_by_human=true` or human approval status of `approved` / `human_approved`
- `approved_for_lite_implementation=true` or implementation status of `approved`, `implemented`, `active`, or `production_ready`
- `archived=false`

The operational producer filters candidate inputs through the promoted sleeve library before building the Lite report. Unapproved research candidates are excluded and cannot appear in the executable queue.

If no approved promoted candidates remain after filtering, Aegis Lite writes an advisory report with no executable queue items.
