import { critical_endpoints } from "./schemas/critical_endpoints"

export function create_openapi_doc() {
  const base_paths = Object.fromEntries(
    critical_endpoints.map((x) => [
      x.path,
      {
        [x.method.toLowerCase()]: {
          summary: `critical endpoint ${x.method} ${x.path}`,
          responses: {
            "200": { description: "ok" },
          },
        },
      },
    ]),
  )

  const paths = {
    ...base_paths,
    "/overflow/run": {
      post: {
        summary: "critical endpoint POST /overflow/run",
        requestBody: {
          required: true,
          content: {
            "application/json": {
              schema: {
                type: "object",
                required: ["id"],
                properties: {
                  id: { type: "string", minLength: 1 },
                  wait_ms: { type: "integer", minimum: 0, default: 150 },
                  shield_timeout_ms: { type: "integer", minimum: 0 },
                },
              },
            },
          },
        },
        responses: {
          "200": {
            description: "ok",
            content: {
              "application/json": {
                schema: {
                  type: "object",
                  required: ["id", "path"],
                  properties: {
                    id: { type: "string" },
                    path: { type: "string", enum: ["wait_for", "marker_fallback"] },
                    marker_at: { type: "integer", nullable: true },
                  },
                },
              },
            },
          },
        },
      },
    },
  }

  return {
    openapi: "3.1.0",
    info: {
      title: "Meridian-HUB API",
      version: "0.1.0",
    },
    paths,
  }
}
