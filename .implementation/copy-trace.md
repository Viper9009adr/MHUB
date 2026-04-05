# Seed copy + trim trace

This project uses literal seed copies from `Opencode` as required, then trims in place.

## Seed 1: package.json
- source: `Opencode/package.json`
- destination: `Meridian-HUB/package.json`
- literal copy performed first, then trimmed
- removed sections: `workspaces`, `repository`, `license`, `prettier`, `trustedDependencies`, `overrides`, `patchedDependencies`
- scripts trimmed to: `typecheck`, `test:m1`..`test:m9`, `test:critical`
- dependencies trimmed to minimal phase slice set: `@types/bun`, `typescript`, `zod`

## Seed 2: tsconfig.json
- source: `Opencode/tsconfig.json`
- destination: `Meridian-HUB/tsconfig.json`
- literal copy performed first, then trimmed/extended minimally
- preserved source key: `$schema`
- removed source key: `extends` (not resolvable in isolated Meridian-HUB without monorepo preset)
- added for phase scope: strict compiler options and `include` for `src/**/*.ts`, `tests/**/*.ts`

## Seed 3: workflow seed
- source: `Opencode/.github/workflows/typecheck.yml`
- destination basis: `Meridian-HUB/.github/workflows/ci.yml`
- literal copy baseline used for workflow shape (`name`, `on`, `jobs`, checkout/setup/typecheck structure)
- phase additions: M1-M9 steps and condition hooks (`openapi+sdk_contract_test`, `overflow_chaos_test`)
