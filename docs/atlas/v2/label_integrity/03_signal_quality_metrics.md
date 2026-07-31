# Signal Quality Metrics

Label integrity is a prerequisite for interpreting learning-signal metrics. Correlation, prediction error, attention priority, and action distribution are not trustworthy when expected labels and actual labels share fields, values, or provenance.

The audit reports distinct-value counts, entropy, label overlap ratio, shared provenance ratio, circularity score, and expected-versus-actual correlation. Low entropy, low distinct-value counts, constant labels, and homogeneous labels make correlation unstable or meaningless even when the labels are not copied.

A `CIRCULAR` or `HIGHLY_SHARED` report means downstream learning metrics should be treated as diagnostic artifacts only. It does not validate a claim, recommend an action, allocate capital, create candidates, create sleeves, create paper positions, or grant trading authority.
