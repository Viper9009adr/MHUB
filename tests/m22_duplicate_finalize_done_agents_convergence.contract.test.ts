import { describe, expect, it } from "bun:test"
import { FinalizeGate } from "../src/orc/finalize_gate"

describe("M22 duplicate finalize done-agents convergence contract", () => {
  it("dedupes finalize keys while converging done-agents set", () => {
    const gate = new FinalizeGate()

    const first = gate.finalize({ key: "finalize-1", done_agents: ["agent-a"] })
    expect(first).toEqual({ accept: true, done_agents: ["agent-a"] })

    const duplicate = gate.finalize({ key: "finalize-1", done_agents: ["agent-b", "agent-a"] })
    expect(duplicate).toEqual({ accept: false, done_agents: ["agent-a", "agent-b"] })

    const second = gate.finalize({ key: "finalize-2", done_agents: ["agent-c", "agent-b"] })
    expect(second).toEqual({ accept: true, done_agents: ["agent-a", "agent-b", "agent-c"] })
  })
})
