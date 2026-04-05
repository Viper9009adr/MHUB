# Runtime Evidence Artifacts

- `../artifacts/audit/runtime-evidence.md`
- `../artifacts/audit/batch3-evidence.md`
- `../artifacts/audit/batch10-evidence.md`

## Batch 9 additions (M19 + M20)

- TST validated `m19_pubsub_replay_sla.test.ts` and `m20_dedupe_key_stability.test.ts` at `3/3` pass.
- TST validated typecheck pass (`bun run typecheck`).
- CRT approved after remediation for bare-catch removal evidence in source (`src/orc/hal_hub_pubsub.ts`: `catch (_error)`).

## Batch 10 additions (M21 + M22)

- M21 scope: out-of-order replay stress coverage for replay-floor resume when cursor `after` id is absent.
- M22 scope: duplicate finalize key dedupe coverage while converging the cumulative `done_agents` set.
- Replay-floor remediation evidence is documented in `../artifacts/audit/batch10-evidence.md` with implementation references to `src/hub/streams_runtime.ts` and `src/hub/replay_cursor.ts`.
- Final validation status is recorded in the same artifact.
