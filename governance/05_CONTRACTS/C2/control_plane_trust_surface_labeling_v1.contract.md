# control_plane_trust_surface_labeling_v1

This contract governs authority labels and anti-pattern bans for Bundle 6 human-facing trust surfaces.

Allowed labels:
- `authoritative`
- `governed_derived`
- `advisory`
- `diagnostic`
- `non_authoritative`

Required labeling law:
- every human-facing trust-plane surface MUST declare its authority label explicitly
- every human-facing trust-plane surface MUST expose governing refs and freshness state explicitly

Bans:
- direct truth-root resolution in CLIs, dashboards, or report renderers
- per-surface semantic recomputation
- advisory logic that bypasses `control_plane_trust_projection_kernel_v1`
- explanation generation from raw logs without certified refs
- unstated authority claims

