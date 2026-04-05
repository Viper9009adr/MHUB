type ReadStore = {
  get: (key: string) => string | undefined
  set: (key: string, value: string) => void
}

const split_id = (id: string) => {
  const [a, b] = id.split("-")
  return [Number(a), Number(b ?? 0)] as const
}

export const compare_id = (a: string, b: string) => {
  const [am, as] = split_id(a)
  const [bm, bs] = split_id(b)
  if (am < bm) return -1
  if (am > bm) return 1
  if (as < bs) return -1
  if (as > bs) return 1
  return 0
}

export class ReplayCursor {
  constructor(private store: ReadStore = new Map<string, string>()) {}

  read(hub_id: string) {
    return this.store.get(hub_id)
  }

  mark(hub_id: string, id: string) {
    this.store.set(hub_id, id)
    return id
  }

  advance(hub_id: string, ids: string[]) {
    if (ids.length === 0) return this.read(hub_id)
    const now = this.read(hub_id)
    const last = ids.reduce((acc, id) => (compare_id(acc, id) >= 0 ? acc : id), ids[0])
    if (!now || compare_id(last, now) > 0) {
      this.mark(hub_id, last)
    }
    return this.read(hub_id)
  }
}
