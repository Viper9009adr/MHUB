import { RefcountLock } from "./refcount_lock"
import { ReconciliationMarker } from "./reconciliation_marker"
import { runHardOverflowSequence } from "./hard_overflow_sequence"

export type DispatchInput = {
  env: string
  key: string
  agent: string
  payload: unknown
  wait_ms?: number
}

export type DispatchDeps = {
  lock: RefcountLock
  marker: ReconciliationMarker
  invoke: (agent: string, payload: unknown) => Promise<unknown>
  shield: () => Promise<boolean>
  wait_for: () => Promise<boolean>
}

export async function dispatch_agent(input: DispatchInput, deps: DispatchDeps) {
  if (input.env !== "dev") {
    throw new Error("dispatch_agent hard-fail: non-dev environment is blocked")
  }

  const unlock = deps.lock.acquire(input.key)
  try {
    const overflow = await runHardOverflowSequence({
      id: input.key,
      wait_ms: input.wait_ms ?? 150,
      shield: deps.shield,
      wait_for: deps.wait_for,
      marker: deps.marker,
    })
    const result = await deps.invoke(input.agent, input.payload)
    return {
      overflow,
      result,
    }
  } finally {
    unlock()
  }
}
