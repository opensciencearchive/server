/**
 * AmacrinService — the cloud control plane (api.amacrin.com).
 *
 * Every method maps 1:1 to a route on the Rust server; there is no sample
 * data behind this interface. `MockAmacrinService` is an in-memory double of
 * the same surface for demos and tests.
 */
import type { Archive } from "@/domain/archive";
import type { Build } from "@/domain/build";
import type { Deployment } from "@/domain/deployment";
import type { Organisation } from "@/domain/organisation";
import type { BuildListItem, OrgMember } from "@/domain/tenant";
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

export interface AmacrinService {
  // ── auth ────────────────────────────────────────── real endpoints ──
  // Sign-in, refresh and logout are handled by the BFF (#185): the browser
  // never holds a control-plane token, so the service only reads identity.
  /** GET /auth/me (through the BFF proxy). */
  getMe(): Promise<Session>;

  // ── organisations ───────────────────────────────── real endpoints ──
  /** GET /organisations */
  listOrganisations(): Promise<Organisation[]>;
  /** POST /organisations — caller becomes Owner. */
  createOrganisation(name: string): Promise<Organisation>;
  /** GET /organisations/{id} */
  getOrganisation(orgId: string): Promise<Organisation>;
  /** GET /organisations/{id}/members — the roster, oldest first. */
  listOrgMembers(orgId: string): Promise<OrgMember[]>;

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
  /** GET /archives/{id}/deployments — deployment history, newest first. */
  listDeployments(archiveId: string): Promise<Deployment[]>;
  /** POST /archives/{id}/destroy — the only removal path (Owner only). */
  destroyArchive(
    archiveId: string,
    opts?: { force?: boolean },
  ): Promise<Archive>;

  // ── builds ──────────────────────────────────────── real endpoints ──
  /** GET /builds/{id} — parent status + per-component breakdown. */
  getBuild(buildId: string): Promise<Build>;
  /** GET /archives/{id}/builds — build history, newest first, parent only. */
  listBuilds(archiveId: string): Promise<BuildListItem[]>;
}
