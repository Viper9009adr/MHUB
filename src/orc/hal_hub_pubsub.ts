import type { Bus } from "./hal_orc_pubsub"

type Opts = {
  debounce_ms?: number
  on_drop?: (payload: unknown) => void | Promise<void>
}

export const hub_channel = (hub_id: string) => `orc:hal:${hub_id}`

const snapshot_payload = (payload: unknown) => {
  if (payload === null || typeof payload !== "object") {
    return payload
  }
  try {
    return structuredClone(payload)
  } catch (_error) {
    return payload
  }
}

export class HalHubPubSub {
  constructor(private bus: Bus) {}

  publish(hub_id: string, payload: unknown) {
    return this.bus.publish(hub_channel(hub_id), payload)
  }

  subscribe(hub_id: string, fn: (payload: unknown) => void | Promise<void>, opts: Opts = {}) {
    if (!opts.debounce_ms || opts.debounce_ms <= 0) {
      return this.bus.subscribe(hub_channel(hub_id), fn)
    }

    let active = true
    let wait: ReturnType<typeof setTimeout> | undefined
    let last: unknown
    return this.bus.subscribe(hub_channel(hub_id), async (payload) => {
      if (!active) {
        return
      }
      const stable = snapshot_payload(payload)
      if (wait) {
        clearTimeout(wait)
        if (opts.on_drop) await opts.on_drop(last)
      }
      last = stable
      wait = setTimeout(async () => {
        if (!active) {
          return
        }
        wait = undefined
        await fn(stable)
      }, opts.debounce_ms)
    }).then((off) => {
      return () => {
        active = false
        if (wait) {
          clearTimeout(wait)
          wait = undefined
        }
        off()
      }
    })
  }
}
