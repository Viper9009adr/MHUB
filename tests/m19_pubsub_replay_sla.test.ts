import { describe, expect, it } from "bun:test"
import { ReplayCursor } from "../src/hub/replay_cursor"
import { RedisStreamsRuntime, replay_with_cursor } from "../src/hub/streams_runtime"

describe("M19 pubsub replay SLA contract", () => {
  it("replays a bounded backlog within SLA", () => {
    const rt = new RedisStreamsRuntime()
    const cursor = new ReplayCursor()
    const hub = "m19"
    const total = 5000

    for (let i = 0; i < total; i += 1) {
      rt.append(hub, { event: `e-${i}` })
    }

    const started = Date.now()
    let seen = 0
    while (true) {
      const batch = replay_with_cursor(rt, cursor, hub, 250)
      if (batch.length === 0) break
      seen += batch.length
    }
    const elapsed_ms = Date.now() - started

    expect(seen).toBe(total)
    expect(elapsed_ms).toBeLessThanOrEqual(2000)
  })

  it("keeps replay payloads stable after source mutation", () => {
    const rt = new RedisStreamsRuntime()
    const cursor = new ReplayCursor()
    const hub = "m19-stable"
    const payload = { event: "sync", key: "k-1" }

    rt.append(hub, payload)
    payload.key = "k-1-mutated"

    const batch = replay_with_cursor(rt, cursor, hub, 10)
    expect(batch[0]?.values).toEqual({ event: "sync", key: "k-1" })
  })
})
