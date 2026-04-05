import { describe, expect, it } from "bun:test"
import { readFile } from "node:fs/promises"

describe("M6 CI hooks", () => {
  it("includes M1-M6 checks in ci workflow", async () => {
    const text = await readFile(".github/workflows/ci.yml", "utf8")
    expect(text.includes("test:m1")).toBe(true)
    expect(text.includes("test:m2")).toBe(true)
    expect(text.includes("test:m3")).toBe(true)
    expect(text.includes("test:m4")).toBe(true)
    expect(text.includes("test:m5")).toBe(true)
    expect(text.includes("test:m6")).toBe(true)
    expect(text.includes("openapi+sdk_contract_test")).toBe(true)
    expect(text.includes("overflow_chaos_test")).toBe(true)
  })
})
