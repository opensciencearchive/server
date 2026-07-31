/**
 * Anti-corruption layer: wire JSON → domain types.
 *
 * Parse-don't-validate: statuses become discriminated unions here, sibling
 * optionals are folded into the variant that owns them, and inconsistent
 * payloads either get an explicit pinned fallback (documented per decoder)
 * or are rejected with `DecodeError`. Nothing outside `api/` ever sees a
 * wire shape.
 */
import type { z } from "zod";

import type {
  Archive,
  ArchiveDeploymentConfig,
  ArchiveStatus,
} from "@/domain/archive";
import type {
  Build,
  BuildStatus,
  ComponentBuild,
  ComponentBuildStatus,
  ComponentKind,
} from "@/domain/build";
import type { Deployment, DeploymentStatus } from "@/domain/deployment";
import { type Organisation, isRole } from "@/domain/organisation";
import type { BuildListItem, OrgMember } from "@/domain/tenant";
import type { Session } from "@/domain/user";

import {
  wireArchive,
  wireArchiveList,
  wireBuild,
  wireBuildList,
  wireBuildSummary,
  wireComponentBuild,
  wireCreateArchiveResponse,
  wireDeployment,
  wireDeploymentList,
  wireMeResponse,
  wireOrgMemberList,
  wireOrganisation,
  wireOrganisationList,
} from "./schemas";

export class DecodeError extends Error {
  constructor(message: string, options?: { cause?: unknown }) {
    super(message, options);
    this.name = "DecodeError";
  }
}

function parse<S extends z.ZodType>(
  schema: S,
  raw: unknown,
  what: string,
): z.output<S> {
  const result = schema.safeParse(raw);
  if (!result.success) {
    throw new DecodeError(`malformed ${what}: ${result.error.message}`, {
      cause: result.error,
    });
  }
  return result.data;
}

function parseDate(value: string, what: string): Date {
  const date = new Date(value);
  if (Number.isNaN(date.getTime())) {
    throw new DecodeError(`invalid timestamp in ${what}: "${value}"`);
  }
  return date;
}

// ---------------------------------------------------------------------------
// Organisations & session
// ---------------------------------------------------------------------------

function toOrganisation(wire: z.output<typeof wireOrganisation>): Organisation {
  let role: Organisation["role"] = null;
  if (wire.role != null) {
    if (!isRole(wire.role)) {
      throw new DecodeError(`unknown role "${wire.role}"`);
    }
    role = wire.role;
  }
  return {
    id: wire.id,
    name: wire.name,
    role,
    createdAt: parseDate(wire.created_at, "organisation"),
  };
}

export function decodeOrganisation(raw: unknown): Organisation {
  return toOrganisation(parse(wireOrganisation, raw, "organisation"));
}

export function decodeOrganisationList(raw: unknown): Organisation[] {
  return parse(wireOrganisationList, raw, "organisation list").organisations.map(
    toOrganisation,
  );
}

export function decodeOrgMemberList(raw: unknown): OrgMember[] {
  return parse(wireOrgMemberList, raw, "member list").map((wire) => {
    if (!isRole(wire.role)) {
      throw new DecodeError(`unknown role "${wire.role}"`);
    }
    return {
      userId: wire.user_id,
      email: wire.email,
      role: wire.role,
      joinedAt: parseDate(wire.joined_at, "member"),
    };
  });
}

export function decodeSession(raw: unknown): Session {
  const wire = parse(wireMeResponse, raw, "me response");
  return {
    user: {
      id: wire.user.id,
      email: wire.user.email,
      createdAt: parseDate(wire.user.created_at, "user"),
    },
    organisations: wire.organisations.map(toOrganisation),
  };
}

// ---------------------------------------------------------------------------
// Archives
// ---------------------------------------------------------------------------

function toArchiveStatus(
  status: string,
  errorMessage: string | null | undefined,
): ArchiveStatus {
  switch (status) {
    case "deploying":
    case "running":
    case "stopped":
    case "destroying":
      return { kind: status };
    case "error":
      // Pinned fallback: an error archive without a message still renders.
      return { kind: "error", message: errorMessage ?? "archive is in error" };
    default:
      throw new DecodeError(`unknown archive status "${status}"`);
  }
}

export function decodeArchive(raw: unknown): Archive {
  const wire = parse(wireArchive, raw, "archive");

  let deploymentConfig: ArchiveDeploymentConfig | null = null;
  const wireDeploymentConfig = wire.config.deployment;
  if (wireDeploymentConfig != null) {
    if (wireDeploymentConfig.provider !== "aws_eks") {
      throw new DecodeError(
        `unknown provider "${wireDeploymentConfig.provider}"`,
      );
    }
    deploymentConfig = {
      provider: "aws_eks",
      region: wireDeploymentConfig.region,
      volumeSizeGb: wireDeploymentConfig.volume_size_gb,
    };
  }

  return {
    id: wire.id,
    organisationId: wire.organisation_id,
    name: wire.name,
    slug: wire.slug,
    domain: wire.domain,
    status: toArchiveStatus(wire.status, wire.error_message),
    orcidAdmins: wire.config.auth?.admins?.orcid ?? [],
    deploymentConfig,
    createdAt: parseDate(wire.created_at, "archive"),
    updatedAt: parseDate(wire.updated_at, "archive"),
  };
}

export function decodeArchiveList(raw: unknown): Archive[] {
  return parse(wireArchiveList, raw, "archive list").map((a) =>
    decodeArchive(a),
  );
}

// ---------------------------------------------------------------------------
// Deployments
// ---------------------------------------------------------------------------

export function decodeDeployment(raw: unknown): Deployment {
  const wire = parse(wireDeployment, raw, "deployment");

  const completedAt = wire.completed_at
    ? parseDate(wire.completed_at, "deployment")
    : null;

  let status: DeploymentStatus;
  switch (wire.status) {
    case "pending":
    case "in_progress":
      status = { kind: wire.status };
      break;
    case "succeeded":
      status = { kind: "succeeded", url: wire.url ?? null, completedAt };
      break;
    case "failed":
      // Pinned fallback: the wire may omit error_message on failure.
      status = {
        kind: "failed",
        errorMessage: wire.error_message ?? "deployment failed",
        completedAt,
      };
      break;
    default:
      throw new DecodeError(`unknown deployment status "${wire.status}"`);
  }

  if (wire.provider !== "aws_eks") {
    throw new DecodeError(`unknown provider "${wire.provider}"`);
  }

  return {
    id: wire.id,
    archiveId: wire.archive_id,
    provider: "aws_eks",
    status,
    osaVersion: wire.osa_version ?? null,
    startedAt: parseDate(wire.started_at, "deployment"),
  };
}

/** The archive's deployment history, newest first as the server orders it. */
export function decodeDeploymentList(raw: unknown): Deployment[] {
  return parse(wireDeploymentList, raw, "deployment list").map((d) =>
    decodeDeployment(d),
  );
}

export function decodeCreateArchiveResponse(raw: unknown): {
  archive: Archive;
  deployment: Deployment;
} {
  const wire = parse(wireCreateArchiveResponse, raw, "create-archive response");
  return {
    archive: decodeArchive(wire.archive),
    deployment: decodeDeployment(wire.deployment),
  };
}

// ---------------------------------------------------------------------------
// Builds
// ---------------------------------------------------------------------------

function toComponentStatus(
  wire: z.output<typeof wireComponentBuild>,
): ComponentBuildStatus {
  switch (wire.status) {
    case "pending":
    case "building":
    case "cancelled":
      return { kind: wire.status };
    case "succeeded": {
      // Digest-as-truth: a succeeded component always has its image ref.
      if (wire.image_ref == null) {
        throw new DecodeError(
          `succeeded component "${wire.name}" has no image_ref`,
        );
      }
      return {
        kind: "succeeded",
        imageRef: wire.image_ref,
        digest: wire.digest ?? null,
      };
    }
    case "failed":
      return {
        kind: "failed",
        errorMessage: wire.error_message ?? "build failed",
      };
    default:
      throw new DecodeError(`unknown component status "${wire.status}"`);
  }
}

function toComponentKind(kind: string, name: string): ComponentKind {
  if (kind !== "hook" && kind !== "ingester") {
    throw new DecodeError(`unknown component kind "${kind}" on "${name}"`);
  }
  return kind;
}

function toBuildStatus(wire: z.output<typeof wireBuildSummary>): BuildStatus {
  switch (wire.status) {
    case "queued":
    case "building":
    case "publishing":
      return { kind: wire.status };
    case "published":
      // Pinned fallback: published_at should always be set on publish; if the
      // wire omits it, the last update is the closest truthful timestamp.
      return {
        kind: "published",
        publishedAt: parseDate(wire.published_at ?? wire.updated_at, "build"),
      };
    case "build_failed":
    case "publish_failed":
      return {
        kind: wire.status,
        errorMessage: wire.error_message ?? "build failed",
      };
    case "cancelled": {
      // Pinned fallback: cancelled_by defaults to "system" if omitted.
      const by = wire.cancelled_by ?? "system";
      if (by !== "user" && by !== "system") {
        throw new DecodeError(`unknown cancelled_by "${by}"`);
      }
      return {
        kind: "cancelled",
        cancelledBy: by,
        cancelReason: wire.cancel_reason ?? null,
      };
    }
    default:
      throw new DecodeError(`unknown build status "${wire.status}"`);
  }
}

export function decodeBuild(raw: unknown): Build {
  const wire = parse(wireBuild, raw, "build");

  const components: ComponentBuild[] = wire.components.map((c) => ({
    kind: toComponentKind(c.kind, c.name),
    name: c.name,
    status: toComponentStatus(c),
    sourceRef: c.source_ref,
  }));

  return {
    id: wire.id,
    archiveId: wire.archive_id,
    conventionSlug: wire.convention_slug,
    conventionRef: wire.convention_ref ?? null,
    status: toBuildStatus(wire),
    components,
    createdAt: parseDate(wire.created_at, "build"),
    updatedAt: parseDate(wire.updated_at, "build"),
  };
}

/**
 * The build history. Rows are parent-only — the list carries the status kind,
 * not the full variant; per-component detail rides `decodeBuild`.
 */
export function decodeBuildList(raw: unknown): BuildListItem[] {
  return parse(wireBuildList, raw, "build list").map((wire) => ({
    id: wire.id,
    conventionSlug: wire.convention_slug,
    conventionRef: wire.convention_ref ?? null,
    statusKind: toBuildStatus(wire).kind,
    createdAt: parseDate(wire.created_at, "build"),
  }));
}
