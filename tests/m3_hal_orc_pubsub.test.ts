import { describe, expect, it } from "bun:test"
import { HalOrcPubSub, MemoryBus } from "../src/orc/hal_orc_pubsub"

describe("M3 HAL ORC pubsub plumbing", () => {
  it("publishes from HAL to ORC channel", async () => {
    const pubsub = new HalOrcPubSub(new MemoryBus())
    const recv: unknown[] = []
    const off = await pubsub.subscribe_from_hal((m) => {
      recv.push(m.payload)
    })
    await pubsub.publish_to_orc({ ping: true })
    off()
    expect(recv).toEqual([{ ping: true }])
  })
})
