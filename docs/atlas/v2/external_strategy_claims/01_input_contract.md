# Input Contract

Required operator input for V1:

- `source_title`: pasted title or manual title
- `source_type`: one of `YOUTUBE_TRANSCRIPT`, `YOUTUBE_MANUAL_NOTES`, `BLOG`, `PAPER`, `OTHER`
- `input_text`: pasted transcript text or manual notes

Optional input:

- `source_url`: pasted URL for provenance only
- `source_channel`: pasted channel or publisher name

If `input_text` is empty, Atlas records `TRANSCRIPT_REQUIRED`. The extractor must not fetch the transcript itself.

The source record stores `input_text_hash` so repeated manual inputs can be identified without relying on a crawler.
