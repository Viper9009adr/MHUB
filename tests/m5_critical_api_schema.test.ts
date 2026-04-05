import { describe, expect, it } from "bun:test"
import { create_openapi_doc } from "../src/api/openapi"
import { dispatch_request, overflow_request } from "../src/api/schemas/critical_endpoints"

describe("M5 critical API schema endpoints", () => {
  it("documents six critical endpoints", () => {
    const doc = create_openapi_doc()
    expect(Object.keys(doc.paths).length).toBe(6)
  })

  it("validates dispatch payload", () => {
    expect(() => dispatch_request.parse({ env: "dev", key: "k", agent: "a", payload: {} })).not.toThrow()
    expect(() => dispatch_request.parse({ env: "dev", key: "", agent: "a", payload: {} })).toThrow()
  })

  it("documents overflow schema and validates overflow payload", () => {
    const doc = create_openapi_doc()
    const body_schema = doc.paths["/overflow/run"].post.requestBody.content["application/json"].schema
    const response_schema = doc.paths["/overflow/run"].post.responses["200"].content["application/json"].schema

    expect(body_schema.properties.shield_timeout_ms.type).toBe("integer")
    expect(response_schema.properties.path.enum).toEqual(["wait_for", "marker_fallback"])
    expect(() => overflow_request.parse({ id: "ov1" })).not.toThrow()
    expect(() => overflow_request.parse({ id: "", shield_timeout_ms: 1 })).toThrow()
  })
})
