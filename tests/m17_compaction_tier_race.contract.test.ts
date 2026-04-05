import { describe, expect, it } from "bun:test"
import { ReplayCursor } from "../src/hub/replay_cursor"
import { RedisStreamsRuntime, replay_with_cursor } from "../src/hub/streams_runtime"

describe("M17 compaction-tier race contract", () => {
  it("keeps replay cursor forward-only during clock rollback", () => {
    const rt = new RedisStreamsRuntime()
    const cursor = new ReplayCursor()
    const hub = "m17"
    const real_now = Date.now
    const values = [100, 90, 90]
    let i = 0

    Date.now = () => values[i++] ?? values[values.length - 1]
    try {
      rt.append(hub, { event: "a" })
      rt.append(hub, { event: "b" })
      rt.append(hub, { event: "c" })
    } finally {
      Date.now = real_now
    }

    const first = replay_with_cursor(rt, cursor, hub, 2)
    expect(first.map((x) => x.values.event)).toEqual(["a", "b"])

    const second = replay_with_cursor(rt, cursor, hub, 2)
    expect(second.map((x) => x.values.event)).toEqual(["c"])

    const none = replay_with_cursor(rt, cursor, hub, 2)
    expect(none).toEqual([])
  })
})
