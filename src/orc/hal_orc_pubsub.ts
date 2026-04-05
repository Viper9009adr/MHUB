type Data = {
  channel: string
  payload: unknown
}

type HubOpts = {
  debounce_ms?: number
  on_drop?: (payload: unknown) => void | Promise<void>
}

export const hub_channel = (hub_id: string) => `orc:hal:${hub_id}`

export type Bus = {
  publish: (channel: string, payload: unknown) => Promise<void>
  subscribe: (channel: string, fn: (payload: unknown) => void | Promise<void>) => Promise<() => void>
}

export class MemoryBus implements Bus {
  private fn = new Map<string, ((payload: unknown) => void | Promise<void>)[]>()

  async publish(channel: string, payload: unknown) {
    const list = this.fn.get(channel) ?? []
    await Promise.all(list.map((fn) => fn(payload)))
  }

  async subscribe(channel: string, fn: (payload: unknown) => void | Promise<void>) {
    const list = this.fn.get(channel) ?? []
    list.push(fn)
    this.fn.set(channel, list)
    return () => {
      const next = (this.fn.get(channel) ?? []).filter((x) => x !== fn)
      this.fn.set(channel, next)
    }
  }
}

export class HalOrcPubSub {
  constructor(
    private bus: Bus,
    private in_channel = "hal.orc.in",
    private out_channel = "orc.hal.out",
  ) {}

  publish_to_orc(payload: unknown) {
    return this.bus.publish(this.in_channel, payload)
  }

  publish_to_hal(payload: unknown) {
    return this.bus.publish(this.out_channel, payload)
  }

  subscribe_from_hal(fn: (m: Data) => void | Promise<void>) {
    return this.bus.subscribe(this.in_channel, (payload) => fn({ channel: this.in_channel, payload }))
  }

  subscribe_from_orc(fn: (m: Data) => void | Promise<void>) {
    return this.bus.subscribe(this.out_channel, (payload) => fn({ channel: this.out_channel, payload }))
  }

  publish_to_hub(hub_id: string, payload: unknown) {
    return this.bus.publish(hub_channel(hub_id), payload)
  }

  subscribe_from_hub(hub_id: string, fn: (m: Data) => void | Promise<void>, opts: HubOpts = {}) {
    if (!opts.debounce_ms || opts.debounce_ms <= 0) {
      return this.bus.subscribe(hub_channel(hub_id), (payload) => fn({ channel: hub_channel(hub_id), payload }))
    }

    let active = true
    let wait: ReturnType<typeof setTimeout> | undefined
    let last: unknown
    return this.bus.subscribe(hub_channel(hub_id), async (payload) => {
      if (!active) {
        return
      }
      if (wait) {
        clearTimeout(wait)
        if (opts.on_drop) await opts.on_drop(last)
      }
      last = payload
      wait = setTimeout(async () => {
        if (!active) {
          return
        }
        wait = undefined
        await fn({ channel: hub_channel(hub_id), payload })
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
