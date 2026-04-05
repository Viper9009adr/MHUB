import { describe, expect, it } from "bun:test"
import { ReplayCursor } from "../src/hub/replay_cursor"
import { RedisStreamsRuntime, replay_with_cursor } from "../src/hub/streams_runtime"

describe("M21 out-of-order chunk replay stress contract", () => {
  it("resumes from cursor floor when after id is missing", () => {
    const rt = new RedisStreamsRuntime()
    const cursor = new ReplayCursor()
    const hub = "m21"
    const total = 40

    const real_now = Date.now
    Date.now = () => 1000
    try {
      for (let i = 0; i < total; i += 1) {
        rt.append(hub, { event: `e-${i}` })
      }
    } finally {
      Date.now = real_now
    }

    cursor.mark(hub, "1000-9.5")

    const seen: string[] = []
    const limits = [3, 1, 4, 2]
    let rounds = 0
    while (rounds < 200) {
      const batch = replay_with_cursor(rt, cursor, hub, limits[rounds % limits.length] ?? 1)
      if (batch.length === 0) break
      seen.push(...batch.map((x) => x.values.event))
      rounds += 1
    }

    const expected = Array.from({ length: 30 }, (_, i) => `e-${i + 10}`)
    expect(seen).toEqual(expected)
    expect(new Set(seen).size).toBe(expected.length)
  })
})
