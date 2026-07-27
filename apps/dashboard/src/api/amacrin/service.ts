/**
 * AmacrinService — the cloud control plane (api.amacrin.com).
 *
 * Real methods map 1:1 to routes on the Rust server. Methods returning
 * `Mocked<T>` have NO backing endpoint yet (see the @mock tags and issue
 * #77's exclusions) — both implementations serve sample data for them, and
 * deleting the `Mocked<…>` wrapper once an endpoint ships turns every call
 * site into a compile error.
 */
import type { Archive } from "@/domain/archive";
import type { Build } from "@/domain/build";
import type { Deployment } from "@/domain/deployment";
import type { Mocked } from "@/domain/mocked";
import type { Organisation } from "@/domain/organisation";
import type {
  BuildListItem,
  DeploymentHistoryEntry,
  OrgMember,
} from "@/domain/tenant";
import type { Session } from "@/domain/user";

/** ORCID sign-in credentials + admins, as supplied on create or rotation. */
export interface ArchiveAuthInput {
  orcid: { clientId: string; clientSecret: string };
  adminOrcidIds: string[];
}

export interface CreateArchiveInput extends ArchiveAuthInput {
  name: string;
  slug: string;
}

export interface RefreshedSession {
  accessToken: string;
  expiresInSeconds: number;
}

export interface AmacrinService {
  // ── auth ────────────────────────────────────────── real endpoints ──
  /** POST /auth/refresh — the browser attaches the httpOnly cookie. */
  refreshSession(): Promise<RefreshedSession>;
  /** POST /auth/logout — revokes the session; idempotent. */
  logout(): Promise<void>;
  /** GET /auth/me */
  getMe(): Promise<Session>;

  // ── organisations ───────────────────────────────── real endpoints ──
  /** GET /organisations */
  listOrganisations(): Promise<Organisation[]>;
  /** POST /organisations — caller becomes Owner. */
  createOrganisation(name: string): Promise<Organisation>;
  /** GET /organisations/{id} */
  getOrganisation(orgId: string): Promise<Organisation>;

  // ── archives ────────────────────────────────────── real endpoints ──
  /** GET /organisations/{id}/archives */
  listOrgArchives(orgId: string): Promise<Archive[]>;
  /** GET /archives — union across the caller's organisations. */
  listArchives(): Promise<Archive[]>;
  /** GET /archives/{id} */
  getArchive(archiveId: string): Promise<Archive>;
  /**
   * POST /organisations/{id}/archives — atomic create-and-deploy (202).
   * Throws SlugTakenError on a 409.
   */
  createArchive(
    orgId: string,
    input: CreateArchiveInput,
  ): Promise<{ archive: Archive; deployment: Deployment }>;
  /**
   * POST /archives/{id}/deploy — redeploy. Omit `auth` to reuse the sealed
   * credentials and stored admins; supply it to rotate both.
   */
  deploy(archiveId: string, auth?: ArchiveAuthInput): Promise<Deployment>;
  /** GET /archives/{id}/status — latest deployment. */
  getDeploymentStatus(archiveId: string): Promise<Deployment>;
  /** POST /archives/{id}/destroy — the only removal path (Owner only). */
  destroyArchive(
    archiveId: string,
    opts?: { force?: boolean },
  ): Promise<Archive>;

  // ── builds ──────────────────────────────────────── real endpoints ──
  /** GET /builds/{id} — parent status + per-component breakdown. */
  getBuild(buildId: string): Promise<Build>;

  // ── no backing API yet ──────────────────────────── sample data ─────
  /** @mock No builds-list endpoint exists (top exclusion in #77). */
  listBuilds(archiveId: string): Promise<Mocked<BuildListItem[]>>;
  /** @mock The API serves only the latest deployment, never history. */
  getDeploymentHistory(
    archiveId: string,
  ): Promise<Mocked<DeploymentHistoryEntry[]>>;
  /** @mock Membership management is API-deferred; no members endpoint. */
  listOrgMembers(orgId: string): Promise<Mocked<OrgMember[]>>;
}
