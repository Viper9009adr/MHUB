import { describe, expect, it } from "bun:test"
import { RefcountLock } from "../src/orc/refcount_lock"

describe("M2 refcount lock", () => {
  it("tracks acquire and release", () => {
    const lock = new RefcountLock()
    const off = lock.acquire("k1")
    expect(lock.read("k1")).toBe(1)
    off()
    expect(lock.read("k1")).toBe(0)
  })

  it("waits for zero", async () => {
    const lock = new RefcountLock()
    const off = lock.acquire("k2")
    const done = lock.wait_for_zero("k2")
    off()
    await done
    expect(lock.read("k2")).toBe(0)
  })
})
