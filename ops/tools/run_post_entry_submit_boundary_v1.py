#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
from pathlib import Path

from constellation_2.common.post_entry_action_request_v1 import seal_post_entry_action_request_v1
from constellation_2.common.post_entry_boundary_operator_surface_v1 import derive_post_entry_operator_surface_v1
from constellation_2.common.post_entry_boundary_snapshot_binding_v1 import build_post_entry_boundary_snapshot_binding_v1
from constellation_2.common.post_entry_core4_shared_v1 import read_json_object_v1
from constellation_2.common.post_entry_submit_boundary_v1 import evaluate_post_entry_submit_boundary_v1


REPO_ROOT = Path(__file__).resolve().parents[2]


def main() -> int:
    parser = argparse.ArgumentParser(prog="run_post_entry_submit_boundary_v1")
    parser.add_argument("--request_json", required=True)
    parser.add_argument("--core2_snapshot_json", required=True)
    parser.add_argument("--core3_projection_json", required=True)
    parser.add_argument("--identity_snapshot_json", default="")
    parser.add_argument("--truth_root", default="")
    parser.add_argument("--evaluated_at_utc", default="")
    parser.add_argument("--json", action="store_true")
    args = parser.parse_args()

    request_payload = read_json_object_v1(Path(args.request_json))
    core2_snapshot = read_json_object_v1(Path(args.core2_snapshot_json))
    core3_projection = read_json_object_v1(Path(args.core3_projection_json))
    identity_snapshot = read_json_object_v1(Path(args.identity_snapshot_json)) if str(args.identity_snapshot_json).strip() else None

    request = seal_post_entry_action_request_v1(request_payload)
    binding, bound_core2, bound_core3, bound_identity = build_post_entry_boundary_snapshot_binding_v1(
        request=request,
        core2_snapshot=core2_snapshot,
        core2_artifact_path=str(Path(args.core2_snapshot_json).resolve()),
        core3_projection=core3_projection,
        execution_identity_snapshot=identity_snapshot,
        repo_root=REPO_ROOT,
        evaluated_at_utc=str(args.evaluated_at_utc or request.created_at_utc),
    )
    bundle = evaluate_post_entry_submit_boundary_v1(
        request=request,
        binding=binding,
        core2_snapshot=bound_core2,
        core3_projection=bound_core3,
        execution_identity_snapshot=bound_identity,
        evaluated_at_utc=str(args.evaluated_at_utc or binding.evaluated_at_utc),
        truth_root=(str(args.truth_root).strip() or None),
    )
    operator_surface = derive_post_entry_operator_surface_v1([bundle.boundary.to_dict()])
    out = {
        "sealed_request": bundle.request.to_dict(),
        "snapshot_binding": bundle.binding.to_dict(),
        "post_entry_submit_boundary": bundle.boundary.to_dict(),
        "authorized_post_entry_payload": None if bundle.authorized_payload is None else bundle.authorized_payload.to_dict(),
        "post_entry_boundary_provenance": bundle.provenance.to_dict(),
        "operator_surface": operator_surface,
    }
    if args.json:
        print(json.dumps(out, sort_keys=True))
    else:
        print(
            "OK: POST_ENTRY_SUBMIT_BOUNDARY_V1 "
            f"request_id={bundle.request.request_id} "
            f"status={bundle.boundary.transmission_authorization_status} "
            f"boundary_verdict_id={bundle.boundary.boundary_verdict_id}"
        )
    return 0 if bundle.boundary.transmission_authorization_status == "AUTHORIZED" else 2


if __name__ == "__main__":
    raise SystemExit(main())
