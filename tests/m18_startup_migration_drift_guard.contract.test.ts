import { describe, expect, it } from "bun:test"
import { assert_startup_migration_drift } from "../src/hub/startup_guard"

describe("M18 startup migration-drift guard contract", () => {
  it("does not fail when migration state is unavailable", () => {
    expect(() => assert_startup_migration_drift("2026-03-30", undefined)).not.toThrow()
    expect(() => assert_startup_migration_drift(undefined, "2026-03-30")).not.toThrow()
  })

  it("does not fail when expected and applied migration match", () => {
    expect(() => assert_startup_migration_drift("2026-03-30", "2026-03-30")).not.toThrow()
  })

  it("fails with explicit drift message when migration versions diverge", () => {
    expect(() => assert_startup_migration_drift("2026-03-30", "2026-03-31")).toThrow(
      "startup migration drift: expected=2026-03-30 applied=2026-03-31",
    )
  })
})
