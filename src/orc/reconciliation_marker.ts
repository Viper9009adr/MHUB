export class ReconciliationMarker {
  private mark = new Map<string, number>()

  set(id: string) {
    const at = Date.now()
    this.mark.set(id, at)
    return at
  }

  get(id: string) {
    return this.mark.get(id)
  }
}
