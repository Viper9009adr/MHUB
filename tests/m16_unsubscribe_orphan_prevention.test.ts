import { describe, expect, it } from "bun:test"
import { HalOrcPubSub, MemoryBus } from "../src/orc/hal_orc_pubsub"
import { HalHubPubSub } from "../src/orc/hal_hub_pubsub"

const sleep = (ms: number) => new Promise<void>((resolve) => setTimeout(resolve, ms))

describe("M16 unsubscribe orphan-prevention contract", () => {
  it("prevents pending debounce delivery after unsubscribe in HalHubPubSub", async () => {
    const bus = new MemoryBus()
    const pubsub = new HalHubPubSub(bus)
    const recv: unknown[] = []

    const off = await pubsub.subscribe(
      "hub-m16",
      async (payload) => {
        recv.push(payload)
      },
      { debounce_ms: 20 },
    )

    await pubsub.publish("hub-m16", { seq: 1 })
    off()
    await sleep(35)

    expect(recv).toEqual([])
  })

  it("prevents pending debounce delivery after unsubscribe in HalOrcPubSub", async () => {
    const bus = new MemoryBus()
    const pubsub = new HalOrcPubSub(bus)
    const recv: unknown[] = []

    const off = await pubsub.subscribe_from_hub(
      "hub-m16-2",
      async (message) => {
        recv.push(message.payload)
      },
      { debounce_ms: 20 },
    )

    await pubsub.publish_to_hub("hub-m16-2", { seq: 1 })
    off()
    await sleep(35)

    expect(recv).toEqual([])
  })
})
