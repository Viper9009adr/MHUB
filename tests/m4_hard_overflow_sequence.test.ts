import { describe, expect, it } from "bun:test"
import { runHardOverflowSequence } from "../src/orc/hard_overflow_sequence"
import { ReconciliationMarker } from "../src/orc/reconciliation_marker"

describe("M4 hard overflow deterministic sequence", () => {
  it("uses marker fallback when wait_for times out", async () => {
    const marker = new ReconciliationMarker()
    const result = await runHardOverflowSequence({
      id: "ov1",
      wait_ms: 1,
      shield: async () => true,
      wait_for: async () => {
        await new Promise((done) => setTimeout(done, 5))
        return true
      },
      marker,
    })
    expect(result.path).toBe("marker_fallback")
    expect(typeof marker.get("ov1")).toBe("number")
  })

  it("uses marker fallback when shield times out via knob", async () => {
    const marker = new ReconciliationMarker()
    const result = await runHardOverflowSequence({
      id: "ov2",
      wait_ms: 25,
      shield_timeout_ms: 1,
      shield: async () => new Promise<boolean>(() => {}),
      wait_for: async () => true,
      marker,
    })
    expect(result.path).toBe("marker_fallback")
    expect(typeof marker.get("ov2")).toBe("number")
  })
})
