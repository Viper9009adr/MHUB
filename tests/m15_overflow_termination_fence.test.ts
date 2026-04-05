import { describe, expect, it } from "bun:test"
import { runHardOverflowSequence } from "../src/orc/hard_overflow_sequence"
import { ReconciliationMarker } from "../src/orc/reconciliation_marker"

describe("M15 overflow termination-fence contract", () => {
  it("short-circuits on non-positive shield_timeout_ms", async () => {
    const marker = new ReconciliationMarker()
    let shieldCalled = false
    let waitCalled = false

    const result = await runHardOverflowSequence({
      id: "m15-shield-fence",
      wait_ms: 25,
      shield_timeout_ms: 0,
      shield: async () => {
        shieldCalled = true
        return true
      },
      wait_for: async () => {
        waitCalled = true
        return true
      },
      marker,
    })

    expect(result.path).toBe("marker_fallback")
    expect(shieldCalled).toBe(false)
    expect(waitCalled).toBe(false)
  })

  it("short-circuits on non-positive wait_ms", async () => {
    const marker = new ReconciliationMarker()
    let waitCalled = false

    const result = await runHardOverflowSequence({
      id: "m15-wait-fence",
      wait_ms: 0,
      shield: async () => true,
      wait_for: async () => {
        waitCalled = true
        return true
      },
      marker,
    })

    expect(result.path).toBe("marker_fallback")
    expect(waitCalled).toBe(false)
  })
})
