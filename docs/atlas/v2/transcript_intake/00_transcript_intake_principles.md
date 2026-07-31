# Transcript Intake Principles

Transcript Intake V1 converts manually supplied YouTube transcript text into Atlas V2 external strategy objects.

The intake path is deliberately narrow:

- Accept pasted transcript text or an explicitly supplied local text file.
- Segment the text into rule-like chunks.
- Preserve performance claims as separate non-evidence segments.
- Create claim candidates only from supplied rule detail.
- Feed eligible candidates into existing ExternalStrategy objects.

The feature is an intake and interpretation surface only. It does not discover videos, fetch URLs, download captions, validate strategies, recommend trades, allocate capital, create sleeves, create candidates, create paper positions, or execute anything.
