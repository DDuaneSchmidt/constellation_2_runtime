# Circularity Detection

The label integrity audit detects circularity by checking shared source fields, shared provenance, direct copies, deterministic transformations, perfect correlation, low label variance, homogeneous distributions, and duplicated label provenance.

A perfect correlation warning is necessary but not sufficient. The audit treats perfect correlation as most severe when combined with shared fields, copied values, or shared provenance.

The output status is one of `INDEPENDENT`, `PARTIALLY_SHARED`, `HIGHLY_SHARED`, or `CIRCULAR`.
