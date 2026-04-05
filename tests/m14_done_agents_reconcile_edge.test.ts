import { describe, expect, it } from "bun:test"
import { create_critical_routes } from "../src/api/routes/critical_endpoints"
import { ReconciliationMarker } from "../src/orc/reconciliation_marker"

describe("M14 done-agents reconcile edge", () => {
  it("keeps reconcile lookup exact for done-agents ids", async () => {
    const marker = new ReconciliationMarker()
    const routes = create_critical_routes({
      dispatch: async () => ({}),
      lock: { read: () => 0 },
      pubsub: { publish_to_orc: async () => {} },
      marker,
    })

    const none = await routes.reconcile("done-agents")
    expect(none).toEqual({ id: "done-agents", marker_at: null })

    const at = marker.set("done-agents")
    const exact = await routes.reconcile("done-agents")
    expect(exact).toEqual({ id: "done-agents", marker_at: at })

    const prefixed = await routes.reconcile("done-agents-worker-1")
    expect(prefixed).toEqual({ id: "done-agents-worker-1", marker_at: null })
  })
})
