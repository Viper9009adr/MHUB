import { describe, expect, it } from "bun:test"
import { ReplayCursor } from "../src/hub/replay_cursor"
import { RedisStreamsRuntime, replay_with_cursor } from "../src/hub/streams_runtime"

describe("replay fallback contract", () => {
  it("falls back to stream head when stored cursor is missing", () => {
    const rt = new RedisStreamsRuntime()
    const cursor = new ReplayCursor()
    const hub = "hub-replay-fallback"

    rt.append(hub, { event: "a" })
    rt.append(hub, { event: "b" })
    rt.append(hub, { event: "c" })

    cursor.mark(hub, "999999-1")

    const first = replay_with_cursor(rt, cursor, hub, 2)
    expect(first.map((x) => x.values.event)).toEqual(["a", "b"])
    expect(cursor.read(hub)).toBe(first[1]?.id)

    const second = replay_with_cursor(rt, cursor, hub, 2)
    expect(second.map((x) => x.values.event)).toEqual(["c"])
  })
})
