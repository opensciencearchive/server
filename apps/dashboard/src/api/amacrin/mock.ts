/**
 * In-memory double of the REAL AmacrinService surface — no network, used
 * for `mock` mode (demos/previews) and tests. Simulates the interesting
 * asynchrony: a created archive progresses pending → in_progress →
 * succeeded across status polls, and the seeded "building" build settles
 * to published after a few polls. Deterministic: no randomness, no clocks.
 */
import { ApiError, SlugTakenError } from "@/api/http/errors";
import type { Archive } from "@/domain/archive";
import type { Build, ComponentBuild } from "@/domain/build";
import type { Deployment } from "@/domain/deployment";
import { type Mocked, mocked } from "@/domain/mocked";
import type { Organisation } from "@/domain/organisation";
import type {
  BuildListItem,
  DeploymentHistoryEntry,
  OrgMember,
} from "@/domain/tenant";
import type { Session } from "@/domain/user";

import {
  sampleBuildList,
  sampleDeploymentHistory,
  sampleOrgMembers,
} from "./samples";
import type {
  AmacrinService,
  ArchiveAuthInput,
  CreateArchiveInput,
  RefreshedSession,
} from "./service";

const T0 = new Date("2026-07-25T14:00:00Z");

function notFound(what: string): ApiError {
  return new ApiError({
    status: 404,
    code: "not_found",
    message: `${what} not found`,
  });
}

interface MockArchiveState {
  archive: Archive;
  deployment: Deployment;
  /** Remaining status polls before the deployment advances a stage. */
  pollsUntilAdvance: number;
}

export class MockAmacrinService implements AmacrinService {
  private readonly organisations = new Map<string, Organisation>();
  private readonly archives = new Map<string, MockArchiveState>();
  private readonly builds = new Map<string, Build>();
  private buildPolls = 0;
  private nextId = 1;

  constructor() {
    this.seed();
  }

  // ── auth ────────────────────────────────────────────────────────────

  refreshSession(): Promise<RefreshedSession> {
    return Promise.resolve({
      accessToken: "mock-access-token",
      expiresInSeconds: 900,
    });
  }

  logout(): Promise<void> {
    return Promise.resolve();
  }

  getMe(): Promise<Session> {
    return Promise.resolve({
      user: {
        id: "user_mock000001",
        email: "r.bergstrom@example.ac.uk",
        createdAt: new Date("2026-05-02T09:14:00Z"),
      },
      organisations: [...this.organisations.values()],
    });
  }

  // ── organisations ───────────────────────────────────────────────────

  listOrganisations(): Promise<Organisation[]> {
    return Promise.resolve([...this.organisations.values()]);
  }

  createOrganisation(name: string): Promise<Organisation> {
    const org: Organisation = {
      id: this.mintId("org"),
      name,
      role: "owner",
      createdAt: T0,
    };
    this.organisations.set(org.id, org);
    return Promise.resolve(org);
  }

  getOrganisation(orgId: string): Promise<Organisation> {
    const org = this.organisations.get(orgId);
    return org ? Promise.resolve(org) : Promise.reject(notFound("organisation"));
  }

  // ── archives ────────────────────────────────────────────────────────

  listOrgArchives(orgId: string): Promise<Archive[]> {
    return Promise.resolve(
      [...this.archives.values()]
        .map((s) => s.archive)
        .filter((a) => a.organisationId === orgId),
    );
  }

  listArchives(): Promise<Archive[]> {
    return Promise.resolve([...this.archives.values()].map((s) => s.archive));
  }

  getArchive(archiveId: string): Promise<Archive> {
    const state = this.archives.get(archiveId);
    return state
      ? Promise.resolve(state.archive)
      : Promise.reject(notFound("archive"));
  }

  createArchive(
    orgId: string,
    input: CreateArchiveInput,
  ): Promise<{ archive: Archive; deployment: Deployment }> {
    if (!this.organisations.has(orgId)) {
      return Promise.reject(notFound("organisation"));
    }
    const taken = [...this.archives.values()].some(
      (s) => s.archive.slug === input.slug,
    );
    if (taken) {
      return Promise.reject(
        new SlugTakenError(
          input.slug,
          new ApiError({
            status: 409,
            code: "duplicate",
            message: `archive slug '${input.slug}' is already taken`,
          }),
        ),
      );
    }

    const archiveId = this.mintId("arch");
    const archive: Archive = {
      id: archiveId,
      organisationId: orgId,
      name: input.name,
      slug: input.slug,
      domain: `${input.slug}.amacr.in`,
      status: { kind: "deploying" },
      orcidAdmins: input.adminOrcidIds,
      deploymentConfig: {
        provider: "aws_eks",
        region: "eu-west-1",
        volumeSizeGb: 5,
      },
      createdAt: T0,
      updatedAt: T0,
    };
    const deployment: Deployment = {
      id: this.mintId("deploy"),
      archiveId,
      provider: "aws_eks",
      status: { kind: "pending" },
      startedAt: T0,
    };
    this.archives.set(archiveId, {
      archive,
      deployment,
      pollsUntilAdvance: 2,
    });
    return Promise.resolve({ archive, deployment });
  }

  deploy(archiveId: string, auth?: ArchiveAuthInput): Promise<Deployment> {
    void auth; // rotation is accepted and "sealed" — nothing to show back
    const state = this.archives.get(archiveId);
    if (!state) return Promise.reject(notFound("archive"));
    state.archive = {
      ...state.archive,
      status: { kind: "deploying" },
      updatedAt: T0,
    };
    state.deployment = {
      id: this.mintId("deploy"),
      archiveId,
      provider: "aws_eks",
      status: { kind: "pending" },
      startedAt: T0,
    };
    state.pollsUntilAdvance = 2;
    return Promise.resolve(state.deployment);
  }

  getDeploymentStatus(archiveId: string): Promise<Deployment> {
    const state = this.archives.get(archiveId);
    if (!state) return Promise.reject(notFound("archive"));
    this.advanceDeployment(state);
    return Promise.resolve(state.deployment);
  }

  destroyArchive(
    archiveId: string,
    opts?: { force?: boolean },
  ): Promise<Archive> {
    void opts;
    const state = this.archives.get(archiveId);
    if (!state) return Promise.reject(notFound("archive"));
    this.archives.delete(archiveId);
    return Promise.resolve({
      ...state.archive,
      status: { kind: "destroying" },
    });
  }

  // ── builds ──────────────────────────────────────────────────────────

  getBuild(buildId: string): Promise<Build> {
    const build = this.builds.get(buildId);
    if (!build) return Promise.reject(notFound("build"));
    if (build.status.kind === "building") {
      this.buildPolls += 1;
      if (this.buildPolls >= 3) {
        this.builds.set(buildId, publishedVersionOf(build));
      }
    }
    return Promise.resolve(this.builds.get(buildId)!);
  }

  // ── no backing API yet: sample data behind the Mocked brand ─────────

  listBuilds(archiveId: string): Promise<Mocked<BuildListItem[]>> {
    return Promise.resolve(mocked(sampleBuildList(archiveId)));
  }

  getDeploymentHistory(
    archiveId: string,
  ): Promise<Mocked<DeploymentHistoryEntry[]>> {
    return Promise.resolve(mocked(sampleDeploymentHistory(archiveId)));
  }

  listOrgMembers(orgId: string): Promise<Mocked<OrgMember[]>> {
    return Promise.resolve(mocked(sampleOrgMembers(orgId)));
  }

  // ── internals ───────────────────────────────────────────────────────

  private advanceDeployment(state: MockArchiveState): void {
    const kind = state.deployment.status.kind;
    if (kind === "succeeded" || kind === "failed") return;
    if (state.pollsUntilAdvance > 0) {
      state.pollsUntilAdvance -= 1;
      if (kind === "pending" && state.pollsUntilAdvance === 0) {
        state.deployment = {
          ...state.deployment,
          status: { kind: "in_progress" },
        };
        state.pollsUntilAdvance = 2;
      }
      return;
    }
    state.deployment = {
      ...state.deployment,
      status: {
        kind: "succeeded",
        url: `https://${state.archive.domain}`,
        completedAt: new Date("2026-07-25T14:03:42Z"),
      },
    };
    state.archive = { ...state.archive, status: { kind: "running" } };
  }

  private mintId(prefix: string): string {
    const id = `${prefix}_mock${String(this.nextId).padStart(6, "0")}`;
    this.nextId += 1;
    return id;
  }

  private seed(): void {
    const summitLab: Organisation = {
      id: "org_7f3k2mq9x1",
      name: "Summit Lab",
      role: "admin",
      createdAt: new Date("2026-05-02T09:20:00Z"),
    };
    const personal: Organisation = {
      id: "org_p3rs0na1aa",
      name: "Personal",
      role: "owner",
      createdAt: new Date("2026-05-02T09:14:00Z"),
    };
    const radarConsortium: Organisation = {
      id: "org_rad4rc0nsm",
      name: "Radar Array Consortium",
      role: "member",
      createdAt: new Date("2026-04-11T16:40:12Z"),
    };
    for (const org of [summitLab, personal, radarConsortium]) {
      this.organisations.set(org.id, org);
    }

    this.seedArchive({
      id: "arch_a1p1n3c11m",
      organisationId: summitLab.id,
      name: "Alpine climate network",
      slug: "alpine-climate",
      status: { kind: "running" },
      orcidAdmins: ["0000-0002-1825-0097", "0000-0001-5109-3700"],
    });
    this.seedArchive({
      id: "arch_so1lmo1s7r",
      organisationId: summitLab.id,
      name: "Soil moisture baseline",
      slug: "soil-moisture-baseline",
      status: {
        kind: "error",
        message:
          "provisioning failed: PersistentVolumeClaim pending after 600s — storage class 'gp3' quota exceeded in eu-west-1",
      },
      orcidAdmins: ["0000-0002-1825-0097"],
    });
    this.seedArchive({
      id: "arch_sky1mag3ry",
      organisationId: personal.id,
      name: "Sky imagery scratchpad",
      slug: "sky-imagery-scratch",
      status: { kind: "running" },
      orcidAdmins: [],
    });
    this.seedArchive({
      id: "arch_st0pp3dxx1",
      organisationId: personal.id,
      name: "2025 pilot (retired)",
      slug: "pilot-2025",
      status: { kind: "stopped" },
      orcidAdmins: [],
    });
    this.seedArchive({
      id: "arch_d0pp13rswp",
      organisationId: radarConsortium.id,
      name: "Doppler radar sweeps",
      slug: "doppler-sweeps",
      status: { kind: "deploying" },
      orcidAdmins: ["0000-0003-1415-9265"],
    });

    this.seedBuilds();
  }

  private seedArchive(args: {
    id: string;
    organisationId: string;
    name: string;
    slug: string;
    status: Archive["status"];
    orcidAdmins: string[];
  }): void {
    const archive: Archive = {
      ...args,
      domain: `${args.slug}.amacr.in`,
      deploymentConfig: {
        provider: "aws_eks",
        region: "eu-west-1",
        volumeSizeGb: 5,
      },
      createdAt: new Date("2026-07-14T10:02:11Z"),
      updatedAt: new Date("2026-07-25T14:06:00Z"),
    };
    const settled = args.status.kind === "running";
    const failed = args.status.kind === "error";
    const deployment: Deployment = {
      id: `deploy_${args.id.slice(5)}`,
      archiveId: args.id,
      provider: "aws_eks",
      status: failed
        ? {
            kind: "failed",
            errorMessage:
              args.status.kind === "error" ? args.status.message : "failed",
            completedAt: new Date("2026-07-24T19:12:44Z"),
          }
        : settled
          ? {
              kind: "succeeded",
              url: `https://${archive.domain}`,
              completedAt: new Date("2026-07-25T14:06:00Z"),
            }
          : { kind: "pending" },
      startedAt: new Date("2026-07-25T14:02:18Z"),
    };
    this.archives.set(args.id, {
      archive,
      deployment,
      pollsUntilAdvance: 2,
    });
  }

  private seedBuilds(): void {
    const succeeded = (
      kind: ComponentBuild["kind"],
      name: string,
      buildId: string,
    ): ComponentBuild => ({
      kind,
      name,
      status: {
        kind: "succeeded",
        imageRef: `941377154785.dkr.ecr.eu-west-1.amazonaws.com/amacrin/archives/arch_a1p1n3c11m/${kind}/${name}:${buildId}`,
        digest: `sha256:${name.replace(/-/g, "").padEnd(64, "0").slice(0, 64)}`,
      },
      sourceRef: `${kind === "hook" ? "hooks" : "ingesters"}/${name.replace(/-/g, "_")}.py`,
    });

    this.builds.set("build_8f3a91c2e5", {
      id: "build_8f3a91c2e5",
      archiveId: "arch_a1p1n3c11m",
      conventionSlug: "geo-rnaseq-v2",
      conventionRef: "4c1e77b",
      status: {
        kind: "published",
        publishedAt: new Date("2026-07-25T14:05:12Z"),
      },
      components: [
        succeeded("ingester", "ghcn-ingester", "build_8f3a91c2e5"),
        succeeded("hook", "validate-metadata", "build_8f3a91c2e5"),
        succeeded("hook", "resolve-ontology", "build_8f3a91c2e5"),
      ],
      createdAt: new Date("2026-07-25T14:01:30Z"),
      updatedAt: new Date("2026-07-25T14:05:12Z"),
    });

    this.builds.set("build_f4a1e6d902", {
      id: "build_f4a1e6d902",
      archiveId: "arch_a1p1n3c11m",
      conventionSlug: "geo-rnaseq-v2",
      conventionRef: "b2a4f60",
      status: {
        kind: "build_failed",
        errorMessage:
          "component 'normalise-units' failed: ImportError: cannot import name 'SchemaRegistry' from 'osa_sdk.schema'",
      },
      components: [
        succeeded("ingester", "ghcn-ingester", "build_f4a1e6d902"),
        {
          kind: "hook",
          name: "normalise-units",
          status: {
            kind: "failed",
            errorMessage:
              "ImportError: cannot import name 'SchemaRegistry' from 'osa_sdk.schema' (/app/osa_sdk/schema/__init__.py)",
          },
          sourceRef: "hooks/normalise_counts.py",
        },
        {
          kind: "hook",
          name: "validate-metadata",
          status: { kind: "cancelled" },
          sourceRef: "hooks/validate_metadata.py",
        },
      ],
      createdAt: new Date("2026-07-25T11:40:00Z"),
      updatedAt: new Date("2026-07-25T11:44:31Z"),
    });

    this.builds.set("build_c9a8b7c6d5", {
      id: "build_c9a8b7c6d5",
      archiveId: "arch_a1p1n3c11m",
      conventionSlug: "geo-rnaseq-v2",
      conventionRef: "77aa001",
      status: {
        kind: "cancelled",
        cancelledBy: "system",
        cancelReason: "superseded by build_8f3a91c2e5",
      },
      components: [
        {
          kind: "ingester",
          name: "ghcn-ingester",
          status: { kind: "cancelled" },
          sourceRef: "ingesters/geo_ingester.py",
        },
        {
          kind: "hook",
          name: "validate-metadata",
          status: { kind: "cancelled" },
          sourceRef: "hooks/validate_metadata.py",
        },
      ],
      createdAt: new Date("2026-07-25T13:59:00Z"),
      updatedAt: new Date("2026-07-25T14:03:40Z"),
    });

    this.builds.set("build_b1u2i3l4d5", {
      id: "build_b1u2i3l4d5",
      archiveId: "arch_a1p1n3c11m",
      conventionSlug: "geo-rnaseq-v2",
      conventionRef: "9e8d7c6",
      status: { kind: "building" },
      components: [
        {
          kind: "ingester",
          name: "ghcn-ingester",
          status: { kind: "building" },
          sourceRef: "ingesters/geo_ingester.py",
        },
        {
          kind: "hook",
          name: "validate-metadata",
          status: { kind: "pending" },
          sourceRef: "hooks/validate_metadata.py",
        },
      ],
      createdAt: new Date("2026-07-25T15:20:00Z"),
      updatedAt: new Date("2026-07-25T15:21:10Z"),
    });
  }
}

function publishedVersionOf(build: Build): Build {
  return {
    ...build,
    status: {
      kind: "published",
      publishedAt: new Date("2026-07-25T15:24:40Z"),
    },
    components: build.components.map((c) => ({
      ...c,
      status: {
        kind: "succeeded",
        imageRef: `941377154785.dkr.ecr.eu-west-1.amazonaws.com/amacrin/archives/${build.archiveId}/${c.kind}/${c.name}:${build.id}`,
        digest: `sha256:${c.name.replace(/-/g, "").padEnd(64, "1").slice(0, 64)}`,
      },
    })),
    updatedAt: new Date("2026-07-25T15:24:40Z"),
  };
}
