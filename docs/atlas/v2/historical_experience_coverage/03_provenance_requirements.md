# Provenance Requirements

Every parser output must preserve `source_artifact` as the repository-relative source path and `provenance_reference` as `source_artifact#anchor`. Parser metadata may be preserved on `HistoricalExperienceRecord` as `source_family`, `parser_name`, and `parser_extracted_fields`.

Parser metadata is not authority. It is reportable extraction provenance only. Converted `ExperienceEvent` records continue to reference the original source artifact, historical record, source type, and provenance reference.

Coverage reports must include candidate count, eligible count, converted count, incomplete count, source-type distribution, and parser contribution by source family when available. Incomplete records are evidence of missing provenance, not permission to infer missing fields.
