import { describe, expect, it } from "bun:test"
import { dispatch_agent } from "../src/orc/dispatch_agent"
import { RefcountLock } from "../src/orc/refcount_lock"
import { ReconciliationMarker } from "../src/orc/reconciliation_marker"

describe("M1 dispatch non-dev hard fail", () => {
  it("blocks dispatch outside dev", async () => {
    await expect(
      dispatch_agent(
        {
          env: "prod",
          key: "k1",
          agent: "a1",
          payload: {},
        },
        {
          lock: new RefcountLock(),
          marker: new ReconciliationMarker(),
          invoke: async () => "ok",
          shield: async () => true,
          wait_for: async () => true,
        },
      ),
    ).rejects.toThrow("non-dev")
  })
})
