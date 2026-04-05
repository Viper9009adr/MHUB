import { describe, expect, it } from "bun:test"
import { ReplayCursor } from "../src/hub/replay_cursor"
import { RedisStreamsRuntime, replay_with_cursor } from "../src/hub/streams_runtime"

describe("M8 replay cursor", () => {
  it("replays incrementally using cursor", () => {
    const rt = new RedisStreamsRuntime()
    const cursor = new ReplayCursor()
    const hub = "h1"

    rt.append(hub, { event: "a" })
    rt.append(hub, { event: "b" })
    rt.append(hub, { event: "c" })

    const first = replay_with_cursor(rt, cursor, hub, 2)
    expect(first.length).toBe(2)
    const at = cursor.read(hub)
    expect(typeof at).toBe("string")

    const second = replay_with_cursor(rt, cursor, hub, 2)
    expect(second.length).toBe(1)
    expect(second[0]?.values.event).toBe("c")

    const none = replay_with_cursor(rt, cursor, hub, 2)
    expect(none.length).toBe(0)
  })
})
