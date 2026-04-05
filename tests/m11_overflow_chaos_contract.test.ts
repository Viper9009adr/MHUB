import { describe, expect, it } from "bun:test"
import { runHardOverflowSequence } from "../src/orc/hard_overflow_sequence"
import { ReconciliationMarker } from "../src/orc/reconciliation_marker"

describe("M11 overflow chaos contract", () => {
  it("returns wait_for path when shield passes and wait succeeds", async () => {
    const marker = new ReconciliationMarker()
    const result = await runHardOverflowSequence({
      id: "m11-wait",
      wait_ms: 25,
      shield: async () => true,
      wait_for: async () => true,
      marker,
    })

    expect(result).toEqual({ id: "m11-wait", path: "wait_for" })
    expect(marker.get("m11-wait")).toBeUndefined()
  })

  it("falls back when wait_for exceeds wait_ms", async () => {
    const marker = new ReconciliationMarker()
    const result = await runHardOverflowSequence({
      id: "m11-wait-timeout",
      wait_ms: 1,
      shield: async () => true,
      wait_for: async () => new Promise<boolean>(() => {}),
      marker,
    })

    expect(result.path).toBe("marker_fallback")
    expect(typeof result.marker_at).toBe("number")
    expect(typeof marker.get("m11-wait-timeout")).toBe("number")
  })

  it("falls back when shield exceeds shield_timeout_ms", async () => {
    const marker = new ReconciliationMarker()
    let waitCalled = false
    const result = await runHardOverflowSequence({
      id: "m11-shield-timeout",
      wait_ms: 25,
      shield_timeout_ms: 1,
      shield: async () => new Promise<boolean>(() => {}),
      wait_for: async () => {
        waitCalled = true
        return true
      },
      marker,
    })

    expect(result.path).toBe("marker_fallback")
    expect(waitCalled).toBe(false)
    expect(typeof marker.get("m11-shield-timeout")).toBe("number")
  })
})
