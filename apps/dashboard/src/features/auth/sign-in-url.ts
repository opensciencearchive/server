/** The API's Google OIDC entry point, bouncing back to this dashboard. */
export function signInUrl(apiBaseUrl: string, dashboardOrigin: string): string {
  const url = new URL("/api/v1/auth/login", apiBaseUrl);
  url.searchParams.set("redirect_uri", `${dashboardOrigin}/`);
  url.searchParams.set("provider", "google");
  return url.toString();
}
