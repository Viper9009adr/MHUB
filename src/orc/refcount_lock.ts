type Wait = {
  done: () => void
}

export class RefcountLock {
  private count = new Map<string, number>()
  private wait = new Map<string, Wait[]>()

  acquire(key: string) {
    this.count.set(key, (this.count.get(key) ?? 0) + 1)
    let open = true
    return () => {
      if (!open) return
      open = false
      const next = (this.count.get(key) ?? 1) - 1
      if (next > 0) {
        this.count.set(key, next)
        return
      }
      this.count.delete(key)
      this.wait.get(key)?.shift()?.done()
    }
  }

  async wait_for_zero(key: string) {
    if (!this.count.has(key)) return
    await new Promise<void>((done) => {
      const list = this.wait.get(key) ?? []
      list.push({ done })
      this.wait.set(key, list)
    })
  }

  read(key: string) {
    return this.count.get(key) ?? 0
  }
}
