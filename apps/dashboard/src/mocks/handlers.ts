/**
 * MSW handlers for the real Amacrin Cloud API surface, serving the
 * wire-shaped fixtures. Host is wildcarded so the same handlers work in
 * Vitest (any test base URL) and in the browser (`msw` mode against the
 * same-origin BFF proxy). Tests layer scenario overrides with `server.use`.
 */
import { HttpResponse, http } from "msw";

import archiveDeploying from "./fixtures/archive.deploying.json";
import archiveError from "./fixtures/archive.error.json";
import archiveRunning from "./fixtures/archive.running.json";
import buildBuilding from "./fixtures/build.building.json";
import buildCancelled from "./fixtures/build.cancelled.json";
import buildFailed from "./fixtures/build.failed.json";
import buildPublished from "./fixtures/build.published.json";
import deploymentFailed from "./fixtures/deployment.failed.json";
import deploymentInProgress from "./fixtures/deployment.in-progress.json";
import deploymentPending from "./fixtures/deployment.pending.json";
import deploymentSucceeded from "./fixtures/deployment.succeeded.json";
import me from "./fixtures/me.json";
import members from "./fixtures/members.json";
import organisations from "./fixtures/organisations.json";

const ARCHIVES = [archiveRunning, archiveError, archiveDeploying];

const DEPLOYMENTS_BY_ARCHIVE: Record<string, unknown> = {
  [archiveRunning.id]: deploymentSucceeded,
  [archiveError.id]: deploymentFailed,
  [archiveDeploying.id]: deploymentInProgress,
};

const BUILDS = [buildPublished, buildBuilding, buildFailed, buildCancelled];

function jsonError(status: number, error: string, message: string) {
  return HttpResponse.json({ error, message }, { status });
}

export const handlers = [
  // ── auth ────────────────────────────────────────────────────────────
  // Refresh/logout are the BFF's job now (#185) — the browser never calls the
  // control plane's cookie flow, so only the identity read is mocked here.
  http.get("*/api/v1/auth/me", () => HttpResponse.json(me)),

  // ── organisations ───────────────────────────────────────────────────
  http.get("*/api/v1/organisations", () => HttpResponse.json(organisations)),
  http.post("*/api/v1/organisations", async ({ request }) => {
    const body = (await request.json()) as { name?: string };
    return HttpResponse.json(
      {
        id: "org_msw0created",
        name: body.name ?? "New organisation",
        role: "owner",
        created_at: "2026-07-25T16:00:00Z",
      },
      { status: 201 },
    );
  }),
  http.get("*/api/v1/organisations/:orgId", ({ params }) => {
    const org = organisations.organisations.find((o) => o.id === params["orgId"]);
    return org
      ? HttpResponse.json(org)
      : jsonError(404, "not_found", `organisation '${String(params["orgId"])}' not found`);
  }),
  http.get("*/api/v1/organisations/:orgId/members", () =>
    HttpResponse.json(members),
  ),
  http.get("*/api/v1/organisations/:orgId/archives", ({ params }) => {
    return HttpResponse.json(
      ARCHIVES.filter((a) => a.organisation_id === params["orgId"]),
    );
  }),
  http.post("*/api/v1/organisations/:orgId/archives", async ({ request }) => {
    const body = (await request.json()) as {
      config?: { name?: string; slug?: string };
    };
    const slug = body.config?.slug ?? "new-archive";
    if (ARCHIVES.some((a) => a.slug === slug)) {
      return jsonError(
        409,
        "duplicate",
        `archive slug '${slug}' is already taken`,
      );
    }
    return HttpResponse.json(
      {
        archive: {
          ...archiveDeploying,
          id: "arch_msw0created",
          name: body.config?.name ?? "New archive",
          slug,
          domain: `${slug}.amacr.in`,
        },
        deployment: {
          ...deploymentPending,
          id: "deploy_msw0created",
          archive_id: "arch_msw0created",
        },
      },
      { status: 202 },
    );
  }),

  // ── archives ────────────────────────────────────────────────────────
  http.get("*/api/v1/archives", () => HttpResponse.json(ARCHIVES)),
  http.get("*/api/v1/archives/:id", ({ params }) => {
    const archive = ARCHIVES.find((a) => a.id === params["id"]);
    return archive
      ? HttpResponse.json(archive)
      : jsonError(404, "not_found", `archive '${String(params["id"])}' not found`);
  }),
  http.post("*/api/v1/archives/:id/deploy", ({ params }) =>
    HttpResponse.json(
      {
        ...deploymentPending,
        archive_id: String(params["id"]),
      },
      { status: 202 },
    ),
  ),
  http.get("*/api/v1/archives/:id/deployments", ({ params }) => {
    const deployment = DEPLOYMENTS_BY_ARCHIVE[String(params["id"])];
    return HttpResponse.json(deployment ? [deployment] : []);
  }),
  http.get("*/api/v1/archives/:id/status", ({ params }) => {
    const deployment = DEPLOYMENTS_BY_ARCHIVE[String(params["id"])];
    return deployment
      ? HttpResponse.json(deployment)
      : jsonError(404, "not_found", `archive '${String(params["id"])}' not found`);
  }),
  http.post("*/api/v1/archives/:id/destroy", ({ params }) => {
    const archive = ARCHIVES.find((a) => a.id === params["id"]);
    if (!archive) {
      return jsonError(404, "not_found", `archive '${String(params["id"])}' not found`);
    }
    return HttpResponse.json(
      { ...archive, status: "destroying" },
      { status: 202 },
    );
  }),

  // ── builds ──────────────────────────────────────────────────────────
  // The list is parent-only: components ride GET /builds/{id}.
  http.get("*/api/v1/archives/:id/builds", ({ params }) => {
    const rows = BUILDS.filter((b) => b.archive_id === params["id"])
      .map((build) => {
        const parent = structuredClone(build) as Record<string, unknown>;
        delete parent["components"];
        delete parent["archive_id"];
        return parent;
      })
      .sort((a, b) =>
        String(b["created_at"]).localeCompare(String(a["created_at"])),
      );
    return HttpResponse.json(rows);
  }),
  http.get("*/api/v1/builds/:id", ({ params }) => {
    const build = BUILDS.find((b) => b.id === params["id"]);
    return build
      ? HttpResponse.json(build)
      : jsonError(404, "not_found", `build '${String(params["id"])}' not found`);
  }),
];
