# Aegis Sleeve Throughput Diagnostics Design V1

The diagnostics builder is a read-only aggregator. It does not call producers, repair tools, sleeve logic, or allocation logic. It reads already-emitted truth reports and links rows by `sleeve_id`, `engine_id`, `hypothesis_id`, and serialized evidence references.

The implementation keeps the core invariant that consumers do not invent truth. Counts and blocker states come from existing artifacts. When no downstream artifact exists, the diagnostic reports that absence as a blocker instead of fabricating a reason.

The artifact is added to the operator portal manifest as read-only evidence so the verified runtime graph can expose it as an explainability surface.
