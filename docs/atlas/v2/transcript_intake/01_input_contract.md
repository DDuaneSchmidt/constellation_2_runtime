# Input Contract

Transcript Intake V1 input is manual text.

Required fields:

- `source_title`
- `transcript_text`

Optional metadata:

- `source_url`
- `source_channel`

`source_url` is stored as provenance metadata only. It must not be fetched. `source_channel` is stored as provenance metadata only. It must not be crawled.

The request object is `TranscriptIntakeRequest` with status `RECEIVED`, `PARSED`, or `FAILED`. A parsed request produces `TranscriptSegment` and `TranscriptClaimCandidate` records.
