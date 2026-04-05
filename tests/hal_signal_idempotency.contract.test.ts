import { describe, expect, it } from "bun:test"
import { HalHubPubSub } from "../src/orc/hal_hub_pubsub"
import { MemoryBus } from "../src/orc/hal_orc_pubsub"

const sleep = (ms: number) => new Promise<void>((resolve) => setTimeout(resolve, ms))

describe("HAL signal idempotency contract", () => {
  it("coalesces repeated identical signals into one delivery", async () => {
    const bus = new MemoryBus()
    const pubsub = new HalHubPubSub(bus)
    const recv: unknown[] = []
    const dropped: unknown[] = []

    const off = await pubsub.subscribe(
      "hub-hal-idem",
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

    await pubsub.publish("hub-hal-idem", { signal: "sync", key: "k-1" })
    await pubsub.publish("hub-hal-idem", { signal: "sync", key: "k-1" })

    await sleep(35)
    off()

    expect(dropped).toEqual([{ signal: "sync", key: "k-1" }])
    expect(recv).toEqual([{ signal: "sync", key: "k-1" }])
  })
})
