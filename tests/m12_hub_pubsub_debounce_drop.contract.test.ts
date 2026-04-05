import { describe, expect, it } from "bun:test"
import { MemoryBus } from "../src/orc/hal_orc_pubsub"
import { HalHubPubSub } from "../src/orc/hal_hub_pubsub"

const sleep = (ms: number) => new Promise<void>((resolve) => setTimeout(resolve, ms))

describe("M12 hub pubsub debounce-drop contract", () => {
  it("drops superseded payloads and delivers only the latest payload", async () => {
    const bus = new MemoryBus()
    const pubsub = new HalHubPubSub(bus)
    const recv: unknown[] = []
    const dropped: unknown[] = []

    const off = await pubsub.subscribe(
      "hub-m12",
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

    await pubsub.publish("hub-m12", { seq: 1 })
    await pubsub.publish("hub-m12", { seq: 2 })
    await pubsub.publish("hub-m12", { seq: 3 })

    await sleep(35)
    off()

    expect(dropped).toEqual([{ seq: 1 }, { seq: 2 }])
    expect(recv).toEqual([{ seq: 3 }])
  })
})
