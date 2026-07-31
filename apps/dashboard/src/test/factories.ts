import type { Archive } from "@/domain/archive";
import type { Build } from "@/domain/build";
import type { Deployment } from "@/domain/deployment";
import type { Organisation } from "@/domain/organisation";
import type { Session } from "@/domain/user";

export function buildOrganisation(over?: Partial<Organisation>): Organisation {
  return {
    id: "org_test000001",
    name: "Summit Lab",
    role: "admin",
    createdAt: new Date("2026-05-02T09:20:00Z"),
    ...over,
  };
}

export function buildArchive(over?: Partial<Archive>): Archive {
  return {
    id: "arch_test00001",
    organisationId: "org_test000001",
    name: "Alpine climate network",
    slug: "alpine-climate",
    domain: "alpine-climate.amacr.in",
    status: { kind: "running" },
    orcidAdmins: ["0000-0002-1825-0097"],
    deploymentConfig: { provider: "aws_eks", region: "eu-west-1", volumeSizeGb: 5 },
    createdAt: new Date("2026-07-14T10:02:11Z"),
    updatedAt: new Date("2026-07-25T14:06:00Z"),
    ...over,
  };
}

export function buildDeployment(over?: Partial<Deployment>): Deployment {
  return {
    id: "deploy_test0001",
    archiveId: "arch_test00001",
    provider: "aws_eks",
    status: {
      kind: "succeeded",
      url: "https://alpine-climate.amacr.in",
      completedAt: new Date("2026-07-25T14:06:00Z"),
    },
    startedAt: new Date("2026-07-25T14:02:18Z"),
    ...over,
  };
}

export function buildBuild(over?: Partial<Build>): Build {
  return {
    id: "build_test000a1",
    archiveId: "arch_test00001",
    conventionSlug: "geo-rnaseq-v2",
    conventionRef: "4c1e77b",
    status: { kind: "published", publishedAt: new Date("2026-07-25T14:05:12Z") },
    components: [],
    createdAt: new Date("2026-07-25T14:01:30Z"),
    updatedAt: new Date("2026-07-25T14:05:12Z"),
    ...over,
  };
}

export function buildSession(over?: Partial<Session>): Session {
  return {
    user: {
      id: "user_test000001",
      email: "r.bergstrom@example.ac.uk",
      createdAt: new Date("2026-05-02T09:14:00Z"),
    },
    organisations: [buildOrganisation()],
    ...over,
  };
}
