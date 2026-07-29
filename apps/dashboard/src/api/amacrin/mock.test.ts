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
});
