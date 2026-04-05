# Implemented Slice Notes (AGENTIC-CLI)

This note documents what is currently implemented in `Meridian-HUB` after Phase 4 test/typecheck pass.

## What exists now

- ORC guardrails and orchestration helpers in `src/orc`
- ORC/HUB bridge helper in `src/orc/hal_hub_pubsub.ts`
- HUB replay runtime + cursor state helpers in `src/hub`
- Hub proto contract in `proto/hub.proto`
- Critical API schemas, route factories, and OpenAPI path generation in `src/api`
- Nine critical test suites in `tests` (`m1`..`m9`)
- CI workflow hooks in `.github/workflows/ci.yml` for typecheck + M1..M9

## Seed + trim provenance

- Seeded from `Opencode` and trimmed for this isolated phase slice
- Copy trace is maintained in `.implementation/copy-trace.md`

## Current limitations (intentional for this slice)

- No full server bootstrap / network transport integration
- No persistent datastore or external bus integration
- No runtime execution of SDK contract/chaos suites (markers only in CI conditions step)
