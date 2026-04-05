import {
  dispatch_request,
  health_response,
  lock_status_response,
  overflow_request,
  publish_request,
  reconcile_response,
} from "../schemas/critical_endpoints"
import { runHardOverflowSequence } from "../../orc/hard_overflow_sequence"

type Ctx = {
  dispatch: (input: {
    env: string
    key: string
    agent: string
    payload: unknown
    wait_ms?: number
  }) => Promise<unknown>
  lock: { read: (key: string) => number }
  pubsub: { publish_to_orc: (payload: unknown) => Promise<void> }
  marker: { get: (id: string) => number | undefined; set: (id: string) => number }
  overflow?: {
    shield?: () => Promise<boolean>
    wait_for?: () => Promise<boolean>
  }
}

export function create_critical_routes(ctx: Ctx) {
  return {
    health: async () => health_response.parse({ status: "ok" }),
    dispatch: async (body: unknown) => {
      const input = dispatch_request.parse(body)
      return ctx.dispatch(input)
    },
    lock_status: async (key: string) => lock_status_response.parse({ key, count: ctx.lock.read(key) }),
    publish: async (body: unknown) => {
      const input = publish_request.parse(body)
      await ctx.pubsub.publish_to_orc(input.payload)
      return { ok: true }
    },
    overflow: async (body: unknown) => {
      const input = overflow_request.parse(body)
      return runHardOverflowSequence({
        id: input.id,
        wait_ms: input.wait_ms,
        shield_timeout_ms: input.shield_timeout_ms,
        shield: ctx.overflow?.shield ?? (async () => true),
        wait_for: ctx.overflow?.wait_for ?? (async () => true),
        marker: ctx.marker,
      })
    },
    reconcile: async (id: string) => reconcile_response.parse({ id, marker_at: ctx.marker.get(id) ?? null }),
  }
}
