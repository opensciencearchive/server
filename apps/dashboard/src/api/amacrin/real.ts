import type { HttpClient } from "@/api/http/client";
import { ApiError, SlugTakenError } from "@/api/http/errors";
import type { Archive } from "@/domain/archive";
import type { Build } from "@/domain/build";
import type { Deployment } from "@/domain/deployment";
import type { Organisation } from "@/domain/organisation";
import type { BuildListItem, OrgMember } from "@/domain/tenant";
import type { Session } from "@/domain/user";

import {
  decodeArchive,
  decodeArchiveList,
  decodeBuild,
  decodeBuildList,
  decodeCreateArchiveResponse,
  decodeDeployment,
  decodeDeploymentList,
  decodeOrgMemberList,
  decodeOrganisation,
  decodeOrganisationList,
  decodeSession,
} from "./wire/decode";
import type {
  AmacrinService,
  ArchiveAuthInput,
  CreateArchiveInput,
} from "./service";

/** The wire shape of `config.auth` on create/deploy requests. */
function authBody(auth: ArchiveAuthInput) {
  return {
    providers: {
      orcid: {
        client_id: auth.orcid.clientId,
        client_secret: auth.orcid.clientSecret,
      },
    },
    admins: { orcid: auth.adminOrcidIds },
  };
}

export class RealAmacrinService implements AmacrinService {
  private readonly baseUrl: string;
  private readonly client: HttpClient;

  constructor(args: { baseUrl: string; client: HttpClient }) {
    this.baseUrl = args.baseUrl.replace(/\/$/, "");
    this.client = args.client;
  }

  // ── auth ────────────────────────────────────────────────────────────
  // Sign-in / refresh / logout live in the BFF (#185); this reads identity
  // through the same-origin proxy.

  async getMe(): Promise<Session> {
    return decodeSession(await this.client.get("/api/v1/auth/me"));
  }

  // ── organisations ───────────────────────────────────────────────────

  async listOrganisations(): Promise<Organisation[]> {
    return decodeOrganisationList(await this.client.get("/api/v1/organisations"));
  }

  async createOrganisation(name: string): Promise<Organisation> {
    return decodeOrganisation(
      await this.client.post("/api/v1/organisations", { name }),
    );
  }

  async getOrganisation(orgId: string): Promise<Organisation> {
    return decodeOrganisation(
      await this.client.get(`/api/v1/organisations/${orgId}`),
    );
  }

  async listOrgMembers(orgId: string): Promise<OrgMember[]> {
    return decodeOrgMemberList(
      await this.client.get(
        `/api/v1/organisations/${encodeURIComponent(orgId)}/members`,
      ),
    );
  }

  // ── archives ────────────────────────────────────────────────────────

  async listOrgArchives(orgId: string): Promise<Archive[]> {
    return decodeArchiveList(
      await this.client.get(`/api/v1/organisations/${orgId}/archives`),
    );
  }

  async listArchives(): Promise<Archive[]> {
    return decodeArchiveList(await this.client.get("/api/v1/archives"));
  }

  async getArchive(archiveId: string): Promise<Archive> {
    return decodeArchive(await this.client.get(`/api/v1/archives/${archiveId}`));
  }

  async createArchive(
    orgId: string,
    input: CreateArchiveInput,
  ): Promise<{ archive: Archive; deployment: Deployment }> {
    try {
      const raw = await this.client.post(
        `/api/v1/organisations/${orgId}/archives`,
        {
          config: {
            name: input.name,
            slug: input.slug,
            auth: authBody(input),
          },
        },
      );
      return decodeCreateArchiveResponse(raw);
    } catch (error) {
      if (error instanceof ApiError && error.status === 409) {
        throw new SlugTakenError(input.slug, error);
      }
      throw error;
    }
  }

  async deploy(
    archiveId: string,
    auth?: ArchiveAuthInput,
  ): Promise<Deployment> {
    const config = auth === undefined ? {} : { auth: authBody(auth) };
    return decodeDeployment(
      await this.client.post(`/api/v1/archives/${archiveId}/deploy`, {
        config,
      }),
    );
  }

  async getDeploymentStatus(archiveId: string): Promise<Deployment> {
    return decodeDeployment(
      await this.client.get(`/api/v1/archives/${archiveId}/status`),
    );
  }

  async listDeployments(archiveId: string): Promise<Deployment[]> {
    return decodeDeploymentList(
      await this.client.get(
        `/api/v1/archives/${encodeURIComponent(archiveId)}/deployments`,
      ),
    );
  }

  async destroyArchive(
    archiveId: string,
    opts?: { force?: boolean },
  ): Promise<Archive> {
    const query = opts?.force ? "?force=true" : "";
    return decodeArchive(
      await this.client.post(`/api/v1/archives/${archiveId}/destroy${query}`),
    );
  }

  // ── builds ──────────────────────────────────────────────────────────

  async getBuild(buildId: string): Promise<Build> {
    return decodeBuild(await this.client.get(`/api/v1/builds/${buildId}`));
  }

  async listBuilds(archiveId: string): Promise<BuildListItem[]> {
    return decodeBuildList(
      await this.client.get(
        `/api/v1/archives/${encodeURIComponent(archiveId)}/builds`,
      ),
    );
  }

}
