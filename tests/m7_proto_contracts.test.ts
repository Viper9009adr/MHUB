import { describe, expect, it } from "bun:test"
import { readFile } from "node:fs/promises"

describe("M7 proto contracts", () => {
  it("defines HubService RPC surface", async () => {
    const proto = await readFile("proto/hub.proto", "utf8")
    expect(proto.includes("service HubService")).toBe(true)
    expect(proto.includes("rpc CreateHub")).toBe(true)
    expect(proto.includes("rpc TerminateHub")).toBe(true)
    expect(proto.includes("rpc JoinHub(stream JoinHubRequest) returns (stream JoinHubEvent)")).toBe(true)
    expect(proto.includes("rpc TapHub")).toBe(true)
    expect(proto.includes("rpc CreateCheckpoint")).toBe(true)
    expect(proto.includes("rpc RollbackCheckpoint")).toBe(true)
    expect(proto.includes("rpc HubStatus")).toBe(true)
  })
})
