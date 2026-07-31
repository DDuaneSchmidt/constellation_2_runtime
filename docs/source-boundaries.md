# Canonical source boundaries

This repository is the authoritative AEGIS source tree. Runtime execution copies and mutable operational data are not source.

## Versioned source

The following are committed and must reproduce from a clean checkout:

- `aegis/`, `constellation_2/`, `ops/`, `runtime/`, `src/`, and `tests/` application source and tests;
- `governance/` registries, contracts, schemas, and policies;
- `docs/` authored documentation, excluding generated screenshots;
- package and repository configuration; and
- explicit fixtures and static application assets. Generated data required by tests must be copied into a purpose-specific `tests/fixtures/` path and injected explicitly; tests must not depend on ignored `reports/` state.

The top-level `runtime/` directory is a Python source package. It is not runtime state and must remain versioned.

## Versioned evidence inputs

`research_journal/` contains schemas, observations, knowledge, failures, design records, and human-readable research evidence required by module contracts and reproducible tests. It is versioned evidence, not mutable runtime output.

Historical controlled operator inputs already committed under `constellation_2/operator_inputs/` remain versioned evidence/configuration. New live credentials or secrets are forbidden there.

## Runtime state and local data

Mutable truth and operational state live outside this repository under `/home/node/constellation_runtime_data`. `/home/node/constellation_2_runtime` is a deployed runtime copy and has no source authority.

Repository-local `constellation_2/runtime/`, `data/`, `artifacts/`, Phase D outputs, Phase F live inputs, and research execution stores are ignored. They may be used locally but are never authoritative source.

## Generated artifacts and evidence outputs

`reports/`, `docs/screenshots/`, UI proof directories, root research exports, and root image captures are generated or captured outputs. They are excluded from Git and preserved by ticket-specific archives when required.

## Ignore governance

`.gitignore` is the executable boundary. Broad patterns must not hide source packages, tests, static assets, fixtures, or `research_journal/`. Any new generated-output path must be documented here before it is ignored.

Secrets, `.env` files, private keys, local environments, caches, and package build outputs are always excluded. Example environment files may be committed explicitly.
