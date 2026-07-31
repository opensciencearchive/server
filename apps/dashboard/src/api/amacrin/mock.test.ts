import { describe, expect, it } from "vitest";

import { SlugTakenError } from "@/api/http/errors";

import { MockAmacrinService } from "./mock";

describe("MockAmacrinService", () => {
  it("seeds a session with organisations", async () => {
    const svc = new MockAmacrinService();
    const session = await svc.getMe();
    expect(session.organisations.length).toBeGreaterThanOrEqual(2);
    expect(session.user.email).toBeTruthy();
  });

  it("createOrganisation adds an owned org", async () => {
    const svc = new MockAmacrinService();
    const before = (await svc.listOrganisations()).length;
    const org = await svc.createOrganisation("New Lab");
    expect(org.role).toBe("owner");
    expect(await svc.listOrganisations()).toHaveLength(before + 1);
  });

  it("createArchive rejects a taken slug with SlugTakenError", async () => {
    const svc = new MockAmacrinService();
    const [org] = await svc.listOrganisations();
    const archives = await svc.listOrgArchives(org!.id);
    const takenSlug = archives[0]!.slug;

    await expect(
      svc.createArchive(org!.id, {
        name: "Duplicate",
        slug: takenSlug,
        orcid: { clientId: "APP-1", clientSecret: "s" },
        adminOrcidIds: [],
      }),
    ).rejects.toBeInstanceOf(SlugTakenError);
  });

  it("scripts a deployment: created archive goes deploying → running across polls", async () => {
    const svc = new MockAmacrinService();
    const [org] = await svc.listOrganisations();
    const { archive, deployment } = await svc.createArchive(org!.id, {
      name: "Fresh",
      slug: "fresh-archive",
      orcid: { clientId: "APP-1", clientSecret: "s" },
      adminOrcidIds: ["0000-0002-1825-0097"],
    });
    expect(archive.status.kind).toBe("deploying");
    expect(deployment.status.kind).toBe("pending");

    // Poll until the scripted progression settles.
    let last = deployment;
    for (let i = 0; i < 10 && last.status.kind !== "succeeded"; i++) {
      last = await svc.getDeploymentStatus(archive.id);
    }
    expect(last.status.kind).toBe("succeeded");
    if (last.status.kind === "succeeded") {
      expect(last.status.url).toBe(`https://${archive.domain}`);
    }
    expect((await svc.getArchive(archive.id)).status.kind).toBe("running");
  });

  it("destroyArchive removes the archive from lists", async () => {
    const svc = new MockAmacrinService();
    const [org] = await svc.listOrganisations();
    const archives = await svc.listOrgArchives(org!.id);
    const target = archives.find((a) => a.status.kind === "running")!;

    const destroyed = await svc.destroyArchive(target.id);
    expect(destroyed.status.kind).toBe("destroying");
    const after = await svc.listOrgArchives(org!.id);
    expect(after.some((a) => a.id === target.id)).toBe(false);
  });

  it("scripts a building build to published across polls", async () => {
    const svc = new MockAmacrinService();
    const buildingId = "build_b1u2i3l4d5";
    let build = await svc.getBuild(buildingId);
    expect(build.status.kind).toBe("building");
    for (let i = 0; i < 10 && build.status.kind !== "published"; i++) {
      build = await svc.getBuild(buildingId);
    }
    expect(build.status.kind).toBe("published");
    expect(
      build.components.every((c) => c.status.kind === "succeeded"),
    ).toBe(true);
  });

  it("listBuilds serves the archive's own builds, newest first", async () => {
    const svc = new MockAmacrinService();

    const builds = await svc.listBuilds("arch_a1p1n3c11m");

    expect(builds.length).toBeGreaterThan(1);
    expect(builds.map((b) => b.id)).toContain("build_8f3a91c2e5");
    const createdAt = builds.map((b) => b.createdAt.getTime());
    expect(createdAt).toEqual([...createdAt].sort((a, b) => b - a));
  });

  it("listBuilds is empty for an archive that has never built", async () => {
    const svc = new MockAmacrinService();
    expect(await svc.listBuilds("arch_st0pp3dxx1")).toEqual([]);
  });

  it("listDeployments grows a history: a redeploy lands on top", async () => {
    const svc = new MockAmacrinService();
    const archiveId = "arch_a1p1n3c11m";

    const before = await svc.listDeployments(archiveId);
    expect(before).toHaveLength(1);

    await svc.deploy(archiveId);
    const after = await svc.listDeployments(archiveId);

    expect(after).toHaveLength(2);
    expect(after[0]!.startedAt.getTime()).toBeGreaterThanOrEqual(
      after[1]!.startedAt.getTime(),
    );
    expect(after[1]!.id).toBe(before[0]!.id);
  });

  it("listOrgMembers serves the organisation's roster", async () => {
    const svc = new MockAmacrinService();
    const roster = await svc.listOrgMembers("org_7f3k2mq9x1");
    expect(roster.length).toBeGreaterThan(0);
    expect(roster.every((m) => m.email.includes("@"))).toBe(true);
  });
});
