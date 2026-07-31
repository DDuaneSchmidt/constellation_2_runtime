# Aegis Operator Shell Navigation Requirements

## Purpose

Define the stable route and navigation contract for the Aegis Operator Shell so route identity, rendered page identity, active navigation state, and requested day cannot diverge.

## Core Principle

The operator shell must have one source of truth for route metadata and navigation selection.

A route may render only through the central route registry. Navigation active state must be derived from the resolved route contract, not from page-specific or section-specific logic.

## Route Contract

Every operator shell route must resolve to a route contract containing:

* `route_id`
* `path`
* `nav_item_id`
* `title`
* `workspace_id`
* `parent_nav_item_id` when the route is a child route
* `canonical_path` when the requested path is an alias

The rendered page title, active navigation item, and workspace identity must all come from this contract.

## Central Route Registry

The canonical route registry must live with operator shell route metadata.

The registry owns:

* route path normalization
* legacy alias resolution
* route ID resolution
* navigation item selection
* workspace selection
* parent/child route relationships
* unknown-route fallback
* day-query preservation policy

No page renderer may define its own active navigation state.

## Deterministic Route Resolution

Route resolution must follow this order:

1. Preserve exact registered route paths.
2. Apply explicit dynamic route normalization where required.
3. Apply legacy aliases only when the requested path is not already registered.
4. Return a safe not-found route when no route is registered.

Unknown routes must not silently render Command Center.

## Deterministic Active Navigation

Active navigation selection must be derived from the resolved route contract.

Rules:

* If `nav_item_id` matches a visible top-level item, that item is active.
* If `nav_item_id` matches a child item, the child is active and its parent is active.
* If the route belongs to Engineering, Engineering is active and Command Center must not remain highlighted.
* If a route is unknown, no normal workspace is active.

## Parent And Child Routes

Parent and child routes must be explicit in the route contract.

Examples:

* Performance → Position Review
* Performance → Sleeve Analytics
* Research → Queue
* Research → Review
* Research → Diagnostics
* Positions → Positions Diagnostics

## Day Consistency

When a user navigates between operator shell routes and the current URL has an explicit `?day=YYYY-MM-DD`, that day must be preserved unless the destination route explicitly provides a different day.

APIs and diagnostics shown on a page must use the same requested day or clearly disclose a different source day.

## Ask Aegis

Ask Aegis must remain discoverable from Command Center and must be scoped to the same displayed route/day context.

## Not Found

An unknown route must render a safe not-found page that explains the route is not registered. It must not fall back to Command Center or any other operational page.

## Required Regression Tests

Tests must prove:

* every registered route resolves through the central route contract
* every contract includes `route_id`, `path`, `nav_item_id`, `title`, and `workspace_id`
* every top-level and child nav item has a route and deterministic active state
* Engineering Dashboard activates Engineering and not Command Center
* Ask Aegis remains visible on Command Center after navigation changes
* explicit `?day=` is preserved across applicable navigation
* unknown routes render the safe not-found page

## Governance Rule

Any new operator shell route, navigation item, UI surface, or route alias must update this contract and its tests. Route metadata, navigation schema, rendered page identity, and active state must not be maintained independently.
## Operator UI Architecture Authority

This document is subordinate to `AEGIS_OPERATOR_UI_ARCHITECTURE_REQUIREMENTS.md` for rendering architecture. Pages must consume the Operator Surface Contract through shared templates before rendering page-specific content.

