# Aegis Operator Empty State Requirements

## Purpose

Define how empty, unavailable, blocked, degraded, historical, and inconsistent operator surfaces render.

## Core Rule

Empty states belong to the Operator Surface Contract, not individual pages.

Pages must render:

* status
* reason
* impact
* next step
* Ask Aegis
* diagnostics collapsed when allowed

## Forbidden

Page-local empty states that infer truth from missing arrays or zero counts are forbidden as primary content.

## Required

Use the shared contract renderer or shared template for every operator-facing empty state.
## Operator UI Architecture Authority

This document is subordinate to `AEGIS_OPERATOR_UI_ARCHITECTURE_REQUIREMENTS.md` for rendering architecture. Pages must consume the Operator Surface Contract through shared templates before rendering page-specific content.

