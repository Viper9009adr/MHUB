import { describe, expect, it } from "bun:test"
import { create_openapi_doc } from "../src/api/openapi"
import { create_critical_routes } from "../src/api/routes/critical_endpoints"
import { critical_endpoints } from "../src/api/schemas/critical_endpoints"
import { ReconciliationMarker } from "../src/orc/reconciliation_marker"

describe("M10 openapi/sdk contract assertions", () => {
  it("keeps deterministic critical path and method contracts", () => {
    const doc = create_openapi_doc()
    const paths = doc.paths as Record<string, Record<string, unknown>>

    expect(Object.keys(paths)).toEqual(critical_endpoints.map((x) => x.path))
    critical_endpoints.forEach((x) => {
      expect(Object.keys(paths[x.path] ?? {})).toEqual([x.method.toLowerCase()])
    })
  })

  it("keeps overflow OpenAPI and route surface stable for sdk generation", () => {
    const doc = create_openapi_doc()
    const body = doc.paths["/overflow/run"].post.requestBody.content["application/json"].schema
    const response = doc.paths["/overflow/run"].post.responses["200"].content["application/json"].schema

    expect(body.required).toEqual(["id"])
    expect(Object.keys(body.properties)).toEqual(["id", "wait_ms", "shield_timeout_ms"])
    expect(response.required).toEqual(["id", "path"])
    expect(response.properties.path.enum).toEqual(["wait_for", "marker_fallback"])

    const routes = create_critical_routes({
      dispatch: async () => ({}),
      lock: { read: () => 0 },
      pubsub: { publish_to_orc: async () => {} },
      marker: new ReconciliationMarker(),
    })

    expect(Object.keys(routes)).toEqual(["health", "dispatch", "lock_status", "publish", "overflow", "reconcile"])
  })
})
