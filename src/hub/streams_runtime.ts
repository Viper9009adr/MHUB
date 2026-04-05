import { compare_id, ReplayCursor } from "./replay_cursor"

/**
 * In-memory stream runtime used by HUB replay tests.
 */

export type StreamRecord = {
  id: string
  values: Record<string, string>
}

const stream_id = (ms: number, n: number) => `${ms}-${n}`

/**
 * Minimal Redis Streams-like runtime keyed by hub id.
 */
export class RedisStreamsRuntime {
  private streams = new Map<string, StreamRecord[]>()
  private seq = new Map<string, number>()
  private last_ms = new Map<string, number>()

  private key(hub_id: string) {
    return `hub:${hub_id}:events`
  }

  /**
   * Appends an event to the hub stream and returns the generated stream id.
   *
   * Stream millisecond time is clamped to avoid backward movement when the
   * local clock regresses.
   */
  append(hub_id: string, values: Record<string, string>) {
    const key = this.key(hub_id)
    const list = this.streams.get(key) ?? []
    const now = Date.now()
    const prev = this.last_ms.get(key)
    const ms = prev !== undefined && now < prev ? prev : now
    this.last_ms.set(key, ms)
    const n = (this.seq.get(key) ?? -1) + 1
    this.seq.set(key, n)
    const id = stream_id(ms, n)
    list.push({ id, values: { ...values } })
    this.streams.set(key, list)
    return id
  }

  /**
   * Replays events after a cursor id, capped by limit.
   *
   * Falls back to stream head when cursor id is missing.
   */
  replay(hub_id: string, after?: string, limit = 100) {
    const list = this.streams.get(this.key(hub_id)) ?? []
    if (!after) return list.slice(0, limit)
    const at = list.findIndex((x) => x.id === after)
    if (at < 0) {
      const next = list.findIndex((x) => compare_id(x.id, after) > 0)
      if (next < 0) return []
      return list.slice(next, next + limit)
    }
    return list.slice(at + 1, at + 1 + limit)
  }
}

/**
 * Replays events for a hub and advances cursor to the highest returned id.
 */
export const replay_with_cursor = (rt: RedisStreamsRuntime, cursor: ReplayCursor, hub_id: string, limit = 100) => {
  const list = rt.replay(hub_id, cursor.read(hub_id), limit)
  cursor.advance(
    hub_id,
    list.map((x) => x.id),
  )
  return list
}
