export type OverflowInput = {
  id: string
  wait_ms: number
  shield_timeout_ms?: number
  shield: () => Promise<boolean>
  wait_for: () => Promise<boolean>
  marker: {
    set: (id: string) => number
  }
}

export type OverflowOutput = {
  id: string
  path: "wait_for" | "marker_fallback"
  marker_at?: number
}

const timeout = (ms: number) =>
  new Promise<boolean>((done) => {
    setTimeout(() => done(false), ms)
  })

export async function runHardOverflowSequence(input: OverflowInput): Promise<OverflowOutput> {
  if (input.shield_timeout_ms !== undefined && input.shield_timeout_ms <= 0) {
    const at = input.marker.set(input.id)
    return {
      id: input.id,
      path: "marker_fallback",
      marker_at: at,
    }
  }

  const ok =
    input.shield_timeout_ms === undefined
      ? await input.shield()
      : await Promise.race([input.shield(), timeout(input.shield_timeout_ms)])
  if (!ok) {
    const at = input.marker.set(input.id)
    return {
      id: input.id,
      path: "marker_fallback",
      marker_at: at,
    }
  }

  if (input.wait_ms <= 0) {
    const at = input.marker.set(input.id)
    return {
      id: input.id,
      path: "marker_fallback",
      marker_at: at,
    }
  }

  const done = await Promise.race([input.wait_for(), timeout(input.wait_ms)])
  if (done) {
    return {
      id: input.id,
      path: "wait_for",
    }
  }

  const at = input.marker.set(input.id)
  return {
    id: input.id,
    path: "marker_fallback",
    marker_at: at,
  }
}
