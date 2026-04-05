/**
 * Startup migration drift guard helpers.
 */
const normalize = (value?: string | null) => {
  if (!value) return undefined
  const trimmed = value.trim()
  return trimmed.length > 0 ? trimmed : undefined
}

/**
 * Validates expected vs applied migration identifiers at startup.
 *
 * Returns without error when either value is missing after normalization.
 * Throws when both values are present and not equal.
 */
export const assert_startup_migration_drift = (expected?: string, applied?: string) => {
  const want = normalize(expected)
  const have = normalize(applied)

  if (!want || !have) {
    return
  }
  if (want !== have) {
    throw new Error(`startup migration drift: expected=${want} applied=${have}`)
  }
}
