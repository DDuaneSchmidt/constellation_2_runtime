# FCC-SOURCE-001 recovery record

## Identity

- Canonical repository: `/home/node/constellation`
- Repository role: `authoritative_source`
- Starting branch: `agent/trading_readiness_controller_v1`
- Starting commit: `4b0e435a9adbc91addacac69534c35bafdc81a5e`
- Starting status: 99 modified tracked files and 4,131 untracked files
- Existing remote: `git@github.com:DDuaneSchmidt/constellation_2_runtime.git`
- Remote default branch before recovery: `main` at `32d422ade77aa2e4c40c01601a94b77787154a16`
- Lineage: remote `main` is an ancestor of the starting commit; the source lineage was 389 commits ahead before recovery.

No Production, deployment, DNS, schedule, broker, or runtime mutation is part of this recovery.

## Pre-change archive

Archive root: `/home/node/constellation_source_archives/FCC-SOURCE-001/20260731T204608Z`

- Full tree including `.git`, tracked, untracked, ignored, stash metadata, worktree metadata, and dangling objects: `constellation-full-tree.tar.zst`
- Full-tree SHA-256: `d7da9eb4727d626e39e3bf5d947548aecbc886c00aa4b35ce394a19cd8f67765`
- Full-tree uncompressed bytes verified by Zstandard: `2354196480`
- Tar entry count: `31473`
- All-refs Git bundle: `constellation-all-refs.bundle`
- Bundle SHA-256: `8aaded045148679ec37783fb341cb1ac8337260d01c7925ab8f29039e1629d10`
- Bundle refs: `282`
- Bundle verification: complete history, SHA-1 object format, PASS

The full-tree archive is the lossless authority for dirty files and unreachable Git objects. The bundle is the portable authority for reachable branches, remote refs, stash, and history.

## Restore procedure

Restore only into an empty recovery parent; never extract over an active repository.

1. Verify both SHA-256 values above.
2. Run `zstd -t constellation-full-tree.tar.zst`.
3. List and inspect the archive before extraction.
4. Extract with `tar --zstd -xpf constellation-full-tree.tar.zst -C <empty-parent>`.
5. Run `git fsck --full` in the restored tree.
6. Verify the portable history independently with `git bundle verify constellation-all-refs.bundle`.

## Classification

- Source: application code, governance, tests, authored docs, configuration, static assets, and fixtures.
- Versioned evidence: `research_journal/` and existing controlled operator evidence/configuration.
- Runtime: external runtime truth plus ignored local runtime/input directories.
- Generated artifacts: reports, screenshots, proof captures, local datasets, and research execution stores.
- Ignored files: credentials, environments, caches, build outputs, runtime state, generated artifacts, and archived local data.

The final tracked/ignored consistency audit found 32 historical quarantine records under `constellation_2/runtime/`. They were removed from Git tracking only; all 1,871 files in that local runtime directory remained on disk, and the pre-change archive preserves their original state. No runtime data was deleted or mutated.

See `docs/source-boundaries.md` and `.gitignore` for the governing boundary.

## Mandatory AEGIS truth checks

The latest graph read before recovery was `/home/node/constellation_runtime_data/truth/reports/aegis_verified_runtime_graph_v1/2026-07-01/verified_runtime_graph.v1.json`, SHA-256 `ed133db6a359f7f41f27e2195af58c324399f227cfef4af4bb06fd455d83b277`.

It identifies commit `4b0e435a9adbc91addacac69534c35bafdc81a5e`, reports `graph_status: BLOCKED`, disables autonomous execution and broker transmission, and forbids readiness claims from source alone.

The mandatory pre-change command `npm run aegis:audit` returned `PASS_AEGIS_PORTFOLIO_CONTROL_PACKET_VERSIONED_V2`. Its rewritten control-packet files are generated report output and remain excluded from source.

## Validation during recovery

- `npm run aegis:research-journal-validate`: PASS.
- `npm run aegis:kernel:test`: 20 passed.
- `pytest -q tests`: 73 passed.
- Fresh-clone validation initially exposed four tests coupled to ignored generated `reports/` data. The three required data inputs were preserved as explicit versioned fixtures under `tests/fixtures/research_reports/`, and the tests now inject those fixtures without changing application defaults or business logic.
- Fresh-clone `pytest -q tests`: 73 passed after the fixture boundary correction.
- Fresh-clone `npm run aegis:kernel:test`: 20 passed.
- Fresh-clone Python compile validation: PASS.
- Python compile validation for `aegis`, `constellation_2`, `ops`, `runtime`, `src`, and `tests`: PASS with bytecode redirected to `/tmp/fcc-source-001-pycache` because legacy ignored `__pycache__` directories include unwritable ownership.
- Broad historical `pytest -q`: interrupted at 4 percent after 400 passed and 22 failed in 140.17 seconds. Failures were existing application/runtime contract divergences, including retired advisor bridge expectations, sleeve-set drift, evidence-gateway assumptions, AI inventory classification, and candidate diagnostics. They are outside this source-recovery ticket and remain fail-closed; no business logic was changed to conceal them.

Recovery acceptance relies on source-boundary validation, focused reproducibility checks, mandatory AEGIS audits, Git integrity, and a clean clone. It does not claim full application regression health.
