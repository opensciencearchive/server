import { HttpResponse, http } from "msw";
import { describe, expect, it } from "vitest";

import { HttpClient } from "@/api/http/client";
import { SlugTakenError } from "@/api/http/errors";
import archiveRunning from "@/mocks/fixtures/archive.running.json";
import buildCancelled from "@/mocks/fixtures/build.cancelled.json";
import buildPublished from "@/mocks/fixtures/build.published.json";
import deploymentFailed from "@/mocks/fixtures/deployment.failed.json";
import deploymentSucceeded from "@/mocks/fixtures/deployment.succeeded.json";
import me from "@/mocks/fixtures/me.json";
import members from "@/mocks/fixtures/members.json";
import { server } from "@/mocks/server";

import { RealAmacrinService } from "./real";

const BASE = "https://api.test";

function makeService() {
  const client = new HttpClient({ baseUrl: BASE });
  return new RealAmacrinService({ baseUrl: BASE, client });
}

describe("RealAmacrinService", () => {
  it("getMe decodes the session", async () => {
    server.use(http.get(`${BASE}/api/v1/auth/me`, () => HttpResponse.json(me)));
    const session = await makeService().getMe();
    expect(session.user.email).toBe("r.bergstrom@example.ac.uk");
    expect(session.organisations).toHaveLength(3);
  });

  it("createArchive sends the exact osa.yaml-shaped body", async () => {
    let body: unknown;
    server.use(
      http.post(
        `${BASE}/api/v1/organisations/org_7f3k2mq9x1/archives`,
        async ({ request }) => {
          body = await request.json();
          return HttpResponse.json(
            { archive: archiveRunning, deployment: deploymentSucceeded },
            { status: 202 },
          );
        },
      ),
    );

    const result = await makeService().createArchive("org_7f3k2mq9x1", {
      name: "Alpine climate network",
      slug: "alpine-climate",
      orcid: { clientId: "APP-K91F2LQ8XZ40MNRT", clientSecret: "s3cret" },
      adminOrcidIds: ["0000-0002-1825-0097"],
    });

    expect(body).toEqual({
      config: {
        name: "Alpine climate network",
        slug: "alpine-climate",
        auth: {
          providers: {
            orcid: {
              client_id: "APP-K91F2LQ8XZ40MNRT",
              client_secret: "s3cret",
            },
          },
          admins: { orcid: ["0000-0002-1825-0097"] },
        },
      },
    });
    expect(result.archive.slug).toBe("alpine-climate");
    expect(result.deployment.status.kind).toBe("succeeded");
  });

  it("createArchive maps a 409 duplicate to SlugTakenError", async () => {
    server.use(
      http.post(`${BASE}/api/v1/organisations/org_x/archives`, () =>
        HttpResponse.json(
          { error: "duplicate", message: "archive slug 'taken' is already taken" },
          { status: 409 },
        ),
      ),
    );

    const err = await makeService()
      .createArchive("org_x", {
        name: "X",
        slug: "taken",
        orcid: { clientId: "APP-1", clientSecret: "s" },
        adminOrcidIds: [],
      })
      .catch((e: unknown) => e);

    expect(err).toBeInstanceOf(SlugTakenError);
    expect((err as SlugTakenError).slug).toBe("taken");
  });

  it("deploy without auth sends the reuse-credentials body", async () => {
    let body: unknown;
    server.use(
      http.post(`${BASE}/api/v1/archives/arch_1/deploy`, async ({ request }) => {
        body = await request.json();
        return HttpResponse.json(deploymentSucceeded, { status: 202 });
      }),
    );
    await makeService().deploy("arch_1");
    expect(body).toEqual({ config: {} });
  });

  it("deploy with auth sends the rotation body", async () => {
    let body: unknown;
    server.use(
      http.post(`${BASE}/api/v1/archives/arch_1/deploy`, async ({ request }) => {
        body = await request.json();
        return HttpResponse.json(deploymentSucceeded, { status: 202 });
      }),
    );
    await makeService().deploy("arch_1", {
      orcid: { clientId: "APP-2", clientSecret: "n3w" },
      adminOrcidIds: ["0000-0001-5109-3700"],
    });
    expect(body).toEqual({
      config: {
        auth: {
          providers: {
            orcid: { client_id: "APP-2", client_secret: "n3w" },
          },
          admins: { orcid: ["0000-0001-5109-3700"] },
        },
      },
    });
  });

  it("destroyArchive appends force when asked", async () => {
    let url: URL | null = null;
    server.use(
      http.post(`${BASE}/api/v1/archives/arch_1/destroy`, ({ request }) => {
        url = new URL(request.url);
        return HttpResponse.json(
          { ...archiveRunning, status: "destroying" },
          { status: 202 },
        );
      }),
    );
    const archive = await makeService().destroyArchive("arch_1", {
      force: true,
    });
    expect(url!.searchParams.get("force")).toBe("true");
    expect(archive.status.kind).toBe("destroying");
  });

  it("getBuild decodes the full component breakdown", async () => {
    server.use(
      http.get(`${BASE}/api/v1/builds/build_8f3a91c2e5`, () =>
        HttpResponse.json(buildPublished),
      ),
    );
    const build = await makeService().getBuild("build_8f3a91c2e5");
    expect(build.status.kind).toBe("published");
    expect(build.components).toHaveLength(3);
  });

  it("listBuilds decodes the archive's history from parent-only rows", async () => {
    const summary = (build: object) => {
      const copy = structuredClone(build) as Record<string, unknown>;
      delete copy["components"];
      delete copy["archive_id"];
      return copy;
    };
    server.use(
      http.get(`${BASE}/api/v1/archives/arch_1/builds`, () =>
        HttpResponse.json([summary(buildPublished), summary(buildCancelled)]),
      ),
    );

    const builds = await makeService().listBuilds("arch_1");

    expect(builds.map((b) => b.id)).toEqual([
      "build_8f3a91c2e5",
      "build_c9a8b7c6d5",
    ]);
    expect(builds[0]!.statusKind).toBe("published");
    expect(builds[1]!.statusKind).toBe("cancelled");
  });

  it("listBuilds percent-encodes the archive id in the path", async () => {
    let url: URL | null = null;
    server.use(
      http.get(`${BASE}/api/v1/archives/:id/builds`, ({ request }) => {
        url = new URL(request.url);
        return HttpResponse.json([]);
      }),
    );
    await makeService().listBuilds("arch/1");
    expect(url!.pathname).toBe("/api/v1/archives/arch%2F1/builds");
  });

  it("listDeployments decodes the archive's deployment history", async () => {
    server.use(
      http.get(`${BASE}/api/v1/archives/arch_1/deployments`, () =>
        HttpResponse.json([deploymentSucceeded, deploymentFailed]),
      ),
    );

    const history = await makeService().listDeployments("arch_1");

    expect(history.map((d) => d.status.kind)).toEqual(["succeeded", "failed"]);
    expect(history[0]!.osaVersion).toBe("v0.0.9");
  });

  it("listOrgMembers decodes the organisation's roster", async () => {
    server.use(
      http.get(`${BASE}/api/v1/organisations/org_1/members`, () =>
        HttpResponse.json(members),
      ),
    );

    const roster = await makeService().listOrgMembers("org_1");

    expect(roster.map((m) => m.email)).toEqual([
      "r.bergstrom@example.ac.uk",
      "p.marsh@example.ac.uk",
      "t.oliveira@example.ac.uk",
    ]);
    expect(roster[1]!.role).toBe("owner");
  });
});
