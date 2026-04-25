---
id: C2_POST_ENTRY_ACTION_REQUEST_CONTRACT_V1
title: "C2 Post-Entry Action Request Contract V1"
status: DRAFT
version: 1
created_utc: 2026-04-11
owner: Constellation
authority: governance+git+runtime_truth
scope: constellation_2_0_paper_post_entry_request_contract
---

# C2 Post-Entry Action Request Contract V1

## Purpose

`post_entry_action_request.v1` is the first-class sealed request artifact for Core 4.

It answers:

- what exact post-entry instruction is being requested

## Required identity

Every request artifact must carry:

- `request_id`
- immutable `request_seal_id`
- `created_at_utc`
- `schema_version`

`request_seal_id` must deterministically seal the exact admitted request shape.

## Required request fields

Every request must carry:

- `action_class`
- `trade_identity_id`
- execution identity:
  - `environment`
  - `sleeve_id`
  - `account_id`
  - `client_id_orders`
  - `execution_root_ref`

## Action-class requirements

### `CLOSE_TRADE`

Must include:

- `requested_quantity`
- `order_parameters`

Must not rely on implicit full-close inference.

### `REDUCE_TRADE`

Must include:

- `requested_quantity`
- `order_parameters`

### `AMEND_PROTECTION`

Must include:

- `requested_quantity`
- `order_parameters`
- `target_order_linkage`

### `CANCEL_ORDER`

Must include:

- `target_order_linkage`

It must not rely on implicit latest-working-order selection.

## Quantity and parameter semantics

- quantity must be explicit when required by action class
- quantity must be syntactically valid and positive
- order parameters must remain request-owned inputs and must not be synthesized by Core 4

## Linkage semantics

`target_order_linkage` is the explicit lineage/linkage reference set used to bind protection amendments or cancel requests to the intended working order.

Unresolved, ambiguous, or empty linkage is invalid.

## Invalid request conditions

The request is invalid if any of the following are true:

- missing request identity
- missing execution identity field
- unsupported `action_class`
- required quantity missing
- required quantity invalid
- required order parameters missing
- required linkage missing
- non-governed environment or routing identity mismatch inside the request itself

## Immutability

After admission to Core 4 evaluation, the request artifact is immutable.

Later stages must consume the sealed artifact, not mutable caller input.
