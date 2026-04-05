import { describe, expect, it } from "bun:test"
import { create_critical_routes } from "../src/api/routes/critical_endpoints"
import { ReconciliationMarker } from "../src/orc/reconciliation_marker"

describe("M9 overflow route knobs and injectable deps", () => {
  it("uses injected wait_for dependency", async () => {
    const marker = new ReconciliationMarker()
    const routes = create_critical_routes({
      dispatch: async () => ({}),
      lock: { read: () => 0 },
      pubsub: { publish_to_orc: async () => {} },
      marker,
      overflow: {
        shield: async () => true,
        wait_for: async () => true,
      },
    })

    const result = await routes.overflow({ id: "m9-wait", wait_ms: 5 })
    expect(result.path).toBe("wait_for")
    expect(marker.get("m9-wait")).toBeUndefined()
  })

  it("uses shield_timeout_ms knob to force fallback", async () => {
    const marker = new ReconciliationMarker()
    const routes = create_critical_routes({
      dispatch: async () => ({}),
      lock: { read: () => 0 },
      pubsub: { publish_to_orc: async () => {} },
      marker,
      overflow: {
        shield: async () => new Promise<boolean>(() => {}),
        wait_for: async () => true,
      },
    })

    const result = await routes.overflow({ id: "m9-fallback", wait_ms: 25, shield_timeout_ms: 1 })
    expect(result.path).toBe("marker_fallback")
    expect(typeof marker.get("m9-fallback")).toBe("number")
  })
})
