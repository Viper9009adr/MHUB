import { describe, expect, it } from "bun:test"
import { MemoryBus } from "../src/orc/hal_orc_pubsub"
import { HalHubPubSub } from "../src/orc/hal_hub_pubsub"

const sleep = (ms: number) => new Promise<void>((resolve) => setTimeout(resolve, ms))

describe("M20 dedupe key stability contract", () => {
  it("keeps dropped and delivered keys stable against source mutation", async () => {
    const bus = new MemoryBus()
    const pubsub = new HalHubPubSub(bus)
    const recv: unknown[] = []
    const dropped: unknown[] = []

    const off = await pubsub.subscribe(
      "hub-m20",
      async (payload) => {
        recv.push(payload)
      },
      {
        debounce_ms: 10,
        on_drop: async (payload) => {
          dropped.push(payload)
        },
      },
    )

    const first = { signal: "sync", key: "k-1" }
    await pubsub.publish("hub-m20", first)
    first.key = "k-1-mutated"

    const second = { signal: "sync", key: "k-2" }
    await pubsub.publish("hub-m20", second)
    second.key = "k-2-mutated"

    await sleep(35)
    off()

    expect(dropped).toEqual([{ signal: "sync", key: "k-1" }])
    expect(recv).toEqual([{ signal: "sync", key: "k-2" }])
  })
})
