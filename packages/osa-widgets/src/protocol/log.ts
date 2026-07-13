/**
 * Diagnostic logging for the widget runtime.
 *
 * A widget runs inside the host's sandboxed iframe, so the browser console
 * (visible in the iframe's devtools) is the only place its lifecycle can be
 * observed — there is no server round-trip for these. Every line is tagged
 * `[osa-widget:<scope>]` so it is easy to find and filter.
 */

const PREFIX = "[osa-widget";

export function viewLog(scope: string, msg: string, ...rest: unknown[]): void {
  console.info(`${PREFIX}:${scope}] ${msg}`, ...rest);
}

export function viewError(scope: string, msg: string, ...rest: unknown[]): void {
  console.error(`${PREFIX}:${scope}] ${msg}`, ...rest);
}
