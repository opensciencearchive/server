import { describe, expect, it } from "vitest";

import archiveDeploying from "@/mocks/fixtures/archive.deploying.json";
import archiveError from "@/mocks/fixtures/archive.error.json";
import archiveRunning from "@/mocks/fixtures/archive.running.json";
import buildBuilding from "@/mocks/fixtures/build.building.json";
import buildCancelled from "@/mocks/fixtures/build.cancelled.json";
import buildFailed from "@/mocks/fixtures/build.failed.json";
import buildPublished from "@/mocks/fixtures/build.published.json";
import deploymentFailedNoMessage from "@/mocks/fixtures/deployment.failed-no-message.json";
import deploymentFailed from "@/mocks/fixtures/deployment.failed.json";
import deploymentInProgress from "@/mocks/fixtures/deployment.in-progress.json";
import deploymentSucceeded from "@/mocks/fixtures/deployment.succeeded.json";
import me from "@/mocks/fixtures/me.json";
import organisations from "@/mocks/fixtures/organisations.json";

import {
  DecodeError,
  decodeArchive,
  decodeBuild,
  decodeDeployment,
  decodeOrganisationList,
  decodeSession,
} from "./decode";

describe("decodeSession (GET /auth/me)", () => {
  it("decodes user and organisations with lowercase roles", () => {
    const session = decodeSession(me);
    expect(session.user.id).toBe("user_01hzy3k8m2");
    expect(session.user.email).toBe("r.bergstrom@example.ac.uk");
    expect(session.user.createdAt).toBeInstanceOf(Date);
    expect(session.organisations).toHaveLength(3);
    expect(session.organisations[0]).toMatchObject({
      id: "org_7f3k2mq9x1",
      name: "Marsh Lab",
      role: "admin",
    });
    expect(session.organisations[1]?.role).toBe("owner");
    expect(session.organisations[2]?.role).toBe("member");
  });

  it("rejects an unknown role string", () => {
    const bad = structuredClone(me);
    bad.organisations[0]!.role = "superuser";
    expect(() => decodeSession(bad)).toThrow(DecodeError);
  });
});

describe("decodeOrganisationList (GET /organisations)", () => {
  it("unwraps the organisations envelope", () => {
    const orgs = decodeOrganisationList(organisations);
    expect(orgs).toHaveLength(3);
    expect(orgs[0]?.name).toBe("Marsh Lab");
    expect(orgs[0]?.createdAt).toBeInstanceOf(Date);
  });

  it("tolerates a missing role (non-membership context)", () => {
    const noRole = {
      organisations: [
        {
          id: "org_x",
          name: "X",
          created_at: "2026-01-01T00:00:00Z",
        },
      ],
    };
    expect(decodeOrganisationList(noRole)[0]?.role).toBeNull();
  });
});

describe("decodeArchive", () => {
  it("decodes a running archive with admins and deployment config", () => {
    const archive = decodeArchive(archiveRunning);
    expect(archive.id).toBe("arch_c0r73xa71a");
    expect(archive.organisationId).toBe("org_7f3k2mq9x1");
    expect(archive.slug).toBe("cortex-atlas");
    expect(archive.domain).toBe("cortex-atlas.amacr.in");
    expect(archive.status).toEqual({ kind: "running" });
    expect(archive.orcidAdmins).toEqual([
      "0000-0002-1825-0097",
      "0000-0001-5109-3700",
    ]);
    expect(archive.deploymentConfig).toEqual({
      provider: "aws_eks",
      region: "eu-west-1",
      volumeSizeGb: 5,
    });
  });

  it("decodes a deploying archive with null config sections", () => {
    const archive = decodeArchive(archiveDeploying);
    expect(archive.status).toEqual({ kind: "deploying" });
    expect(archive.orcidAdmins).toEqual([]);
    expect(archive.deploymentConfig).toBeNull();
  });

  it("folds error_message into the error status variant", () => {
    const archive = decodeArchive(archiveError);
    expect(archive.status.kind).toBe("error");
    if (archive.status.kind === "error") {
      expect(archive.status.message).toMatch(/quota exceeded/);
    }
  });

  it("rejects an unknown status", () => {
    const bad = { ...structuredClone(archiveRunning), status: "sleeping" };
    expect(() => decodeArchive(bad)).toThrow(DecodeError);
  });
});

describe("decodeDeployment", () => {
  it("decodes succeeded with url and completedAt", () => {
    const d = decodeDeployment(deploymentSucceeded);
    expect(d.id).toBe("deploy_9a1b2c3d4e");
    expect(d.status).toEqual({
      kind: "succeeded",
      url: "https://cortex-atlas.amacr.in",
      completedAt: new Date("2026-07-25T14:06:00+00:00"),
    });
    expect(d.startedAt).toBeInstanceOf(Date);
  });

  it("decodes in_progress with no completion fields", () => {
    const d = decodeDeployment(deploymentInProgress);
    expect(d.status).toEqual({ kind: "in_progress" });
  });

  it("decodes failed carrying the verbatim error message", () => {
    const d = decodeDeployment(deploymentFailed);
    expect(d.status.kind).toBe("failed");
    if (d.status.kind === "failed") {
      expect(d.status.errorMessage).toMatch(/PersistentVolumeClaim/);
      expect(d.status.completedAt).toBeInstanceOf(Date);
    }
  });

  it("falls back to a generic message when failed omits error_message", () => {
    const d = decodeDeployment(deploymentFailedNoMessage);
    expect(d.status.kind).toBe("failed");
    if (d.status.kind === "failed") {
      expect(d.status.errorMessage).toBe("deployment failed");
      expect(d.status.completedAt).toBeNull();
    }
  });
});

describe("decodeBuild", () => {
  it("decodes a published build with succeeded components carrying digests", () => {
    const b = decodeBuild(buildPublished);
    expect(b.id).toBe("build_8f3a91c2e5");
    expect(b.conventionSlug).toBe("geo-rnaseq-v2");
    expect(b.conventionRef).toBe("4c1e77b");
    expect(b.status.kind).toBe("published");
    if (b.status.kind === "published") {
      expect(b.status.publishedAt).toEqual(
        new Date("2026-07-25T14:05:12+00:00"),
      );
    }
    expect(b.components).toHaveLength(3);
    const ingester = b.components[0]!;
    expect(ingester.kind).toBe("ingester");
    expect(ingester.status.kind).toBe("succeeded");
    if (ingester.status.kind === "succeeded") {
      expect(ingester.status.imageRef).toContain("geo-ingester:build_");
      expect(ingester.status.digest).toMatch(/^sha256:/);
    }
  });

  it("decodes a building build with pending/building components", () => {
    const b = decodeBuild(buildBuilding);
    expect(b.status).toEqual({ kind: "building" });
    expect(b.components.map((c) => c.status.kind)).toEqual([
      "building",
      "pending",
    ]);
  });

  it("decodes build_failed with the failed component's error and cancelled sibling", () => {
    const b = decodeBuild(buildFailed);
    expect(b.status.kind).toBe("build_failed");
    if (b.status.kind === "build_failed") {
      expect(b.status.errorMessage).toMatch(/SchemaRegistry/);
    }
    const failed = b.components.find((c) => c.status.kind === "failed")!;
    expect(failed.name).toBe("normalise-counts");
    if (failed.status.kind === "failed") {
      expect(failed.status.errorMessage).toMatch(/ImportError/);
    }
    expect(
      b.components.filter((c) => c.status.kind === "cancelled"),
    ).toHaveLength(1);
  });

  it("decodes cancelled as data: who and why", () => {
    const b = decodeBuild(buildCancelled);
    expect(b.status).toEqual({
      kind: "cancelled",
      cancelledBy: "system",
      cancelReason: "superseded by build_8f3a91c2e5",
    });
  });

  it("defaults cancelled_by to system when the wire omits it", () => {
    const bare = {
      ...structuredClone(buildCancelled),
      cancelled_by: undefined,
      cancel_reason: undefined,
    };
    const b = decodeBuild(bare);
    expect(b.status).toEqual({
      kind: "cancelled",
      cancelledBy: "system",
      cancelReason: null,
    });
  });

  it("rejects a succeeded component without an image_ref", () => {
    const bad = structuredClone(buildPublished);
    delete (bad.components[0] as Record<string, unknown>)["image_ref"];
    expect(() => decodeBuild(bad)).toThrow(DecodeError);
  });
});
