/**
 * Archive lifecycle. There is no "pending": creation is atomic
 * create-and-deploy, so an archive is born `deploying`. Only the `error`
 * variant carries a message.
 */
export type ArchiveStatus =
  | { kind: "deploying" }
  | { kind: "running" }
  | { kind: "stopped" }
  | { kind: "error"; message: string }
  | { kind: "destroying" };

export type ArchiveStatusKind = ArchiveStatus["kind"];

export const ARCHIVE_STATUS_KINDS: readonly ArchiveStatusKind[] = [
  "deploying",
  "running",
  "stopped",
  "error",
  "destroying",
];

export interface ArchiveDeploymentConfig {
  provider: "aws_eks";
  region: string;
  volumeSizeGb: number;
}

/** A hosted OSA server configuration. */
export interface Archive {
  id: string;
  organisationId: string;
  /** Display name — free-form, not unique. */
  name: string;
  /** RFC 1123 label, globally unique; the domain is always `{slug}.amacr.in`. */
  slug: string;
  domain: string;
  status: ArchiveStatus;
  /** Administrator ORCID iDs from the non-secret auth config. */
  orcidAdmins: string[];
  deploymentConfig: ArchiveDeploymentConfig | null;
  createdAt: Date;
  updatedAt: Date;
}

/** Redeploy and destroy are unavailable mid-transition. */
export function isDeployBlocked(status: ArchiveStatus): boolean {
  return status.kind === "deploying" || status.kind === "destroying";
}

/** Slug rules mirrored from the server: RFC 1123 label, 3–63 chars. */
export const SLUG_PATTERN = /^[a-z0-9]([a-z0-9-]{1,61}[a-z0-9])$/;

export function isValidSlug(slug: string): boolean {
  return SLUG_PATTERN.test(slug);
}
