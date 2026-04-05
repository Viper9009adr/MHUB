export type FinalizeInput = {
  key: string
  done_agents: string[]
}

export type FinalizeDecision = {
  accept: boolean
  done_agents: string[]
}

export class FinalizeGate {
  private finalized = new Set<string>()
  private done = new Set<string>()

  finalize(input: FinalizeInput): FinalizeDecision {
    for (const id of input.done_agents) {
      this.done.add(id)
    }
    const accept = !this.finalized.has(input.key)
    if (accept) {
      this.finalized.add(input.key)
    }
    return {
      accept,
      done_agents: [...this.done],
    }
  }
}
