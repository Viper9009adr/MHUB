import { dispatch_agent, type DispatchDeps, type DispatchInput } from "./dispatch_agent"
import { HalOrcPubSub, MemoryBus } from "./hal_orc_pubsub"
import { HalHubPubSub } from "./hal_hub_pubsub"
import { RefcountLock } from "./refcount_lock"
import { ReconciliationMarker } from "./reconciliation_marker"

export { dispatch_agent, HalOrcPubSub, HalHubPubSub, MemoryBus, RefcountLock, ReconciliationMarker }

export function create_orc(deps: Omit<DispatchDeps, "lock" | "marker">) {
  const lock = new RefcountLock()
  const marker = new ReconciliationMarker()
  const bus = new MemoryBus()
  const pubsub = new HalOrcPubSub(bus)
  const hub_pubsub = new HalHubPubSub(bus)

  return {
    lock,
    marker,
    pubsub,
    hub_pubsub,
    dispatch: (input: DispatchInput) => dispatch_agent(input, { ...deps, lock, marker }),
  }
}
