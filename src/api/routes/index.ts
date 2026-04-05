import { create_critical_routes } from "./critical_endpoints"

export function create_routes(ctx: Parameters<typeof create_critical_routes>[0]) {
  return create_critical_routes(ctx)
}
