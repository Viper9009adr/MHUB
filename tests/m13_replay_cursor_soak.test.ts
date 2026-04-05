import { describe, expect, it } from "bun:test"
import { ReplayCursor } from "../src/hub/replay_cursor"
import { RedisStreamsRuntime, replay_with_cursor } from "../src/hub/streams_runtime"

describe("M13 replay-cursor gap/dup soak", () => {
  it("replays all events exactly once across mixed batch sizes", () => {
    const rt = new RedisStreamsRuntime()
    const cursor = new ReplayCursor()
    const hub = "m13"
    const total = 64
    const ids: string[] = []

    for (let i = 0; i < total; i += 1) {
      ids.push(rt.append(hub, { event: `e-${i}` }))
    }

    const seen: string[] = []
    const limits = [1, 3, 2, 5, 4]
    let rounds = 0

    while (rounds < 1000) {
      const batch = replay_with_cursor(rt, cursor, hub, limits[rounds % limits.length] ?? 1)
      if (batch.length === 0) break
      seen.push(...batch.map((x) => x.values.event))
      rounds += 1
    }

    expect(seen.length).toBe(total)
    expect(new Set(seen).size).toBe(total)
    expect(seen).toEqual(Array.from({ length: total }, (_, i) => `e-${i}`))
    expect(cursor.read(hub)).toBe(ids[ids.length - 1])
  })
})
