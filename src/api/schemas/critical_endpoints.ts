import { z } from "zod"

export const health_response = z.object({
  status: z.literal("ok"),
})

export const dispatch_request = z.object({
  env: z.string(),
  key: z.string().min(1),
  agent: z.string().min(1),
  payload: z.unknown(),
})

export const lock_status_response = z.object({
  key: z.string(),
  count: z.number().int().nonnegative(),
})

export const publish_request = z.object({
  payload: z.unknown(),
})

export const overflow_request = z.object({
  id: z.string().min(1),
  wait_ms: z.number().int().nonnegative().default(150),
  shield_timeout_ms: z.number().int().nonnegative().optional(),
})

export const reconcile_response = z.object({
  id: z.string(),
  marker_at: z.number().int().nullable(),
})

export const critical_endpoints = [
  { method: "GET", path: "/health" },
  { method: "POST", path: "/dispatch" },
  { method: "GET", path: "/locks/:key" },
  { method: "POST", path: "/pubsub/publish" },
  { method: "POST", path: "/overflow/run" },
  { method: "GET", path: "/reconcile/:id" },
] as const
