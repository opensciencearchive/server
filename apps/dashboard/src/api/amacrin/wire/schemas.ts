/**
 * Wire DTO schemas — verbatim mirrors of the Rust serializers in
 * osa-cloud-server (`dto/archives.rs`, `dto/builds.rs`, `routes/auth.rs`,
 * `routes/organisations.rs`). PRIVATE to `api/`: nothing outside this layer
 * may see these shapes; decoders (decode.ts) are the anti-corruption layer
 * that turns them into domain types.
 */
import { z } from "zod";

export const wireOrganisation = z.object({
  id: z.string(),
  name: z.string(),
  role: z.string().nullish(),
  created_at: z.string(),
});

export const wireOrganisationList = z.object({
  organisations: z.array(wireOrganisation),
});

export const wireMeResponse = z.object({
  user: z.object({
    id: z.string(),
    email: z.string(),
    created_at: z.string(),
  }),
  organisations: z.array(wireOrganisation),
});

export const wireTokenResponse = z.object({
  access_token: z.string(),
  expires_in: z.number(),
  token_type: z.string(),
  refresh_token: z.string().nullish(),
});

export const wireArchiveConfig = z.object({
  auth: z
    .object({
      admins: z.object({ orcid: z.array(z.string()).nullish() }).nullish(),
    })
    .nullish(),
  deployment: z
    .object({
      provider: z.string(),
      region: z.string(),
      volume_size_gb: z.number(),
    })
    .nullish(),
});

export const wireArchive = z.object({
  id: z.string(),
  organisation_id: z.string(),
  name: z.string(),
  slug: z.string(),
  domain: z.string(),
  config: wireArchiveConfig,
  status: z.string(),
  error_message: z.string().nullish(),
  created_at: z.string(),
  updated_at: z.string(),
});

export const wireArchiveList = z.array(wireArchive);

export const wireDeployment = z.object({
  id: z.string(),
  archive_id: z.string(),
  provider: z.string(),
  status: z.string(),
  url: z.string().nullish(),
  error_message: z.string().nullish(),
  /** Tag of the provisioned OSA image — recorded on success only. */
  osa_version: z.string().nullish(),
  started_at: z.string(),
  completed_at: z.string().nullish(),
});

export const wireDeploymentList = z.array(wireDeployment);

export const wireOrgMember = z.object({
  user_id: z.string(),
  email: z.string(),
  role: z.string(),
  joined_at: z.string(),
});

export const wireOrgMemberList = z.array(wireOrgMember);

export const wireCreateArchiveResponse = z.object({
  archive: wireArchive,
  deployment: wireDeployment,
});

export const wireComponentBuild = z.object({
  kind: z.string(),
  name: z.string(),
  status: z.string(),
  image_ref: z.string().nullish(),
  digest: z.string().nullish(),
  source_ref: z.string(),
  error_message: z.string().nullish(),
});

/** One row of the build history — the parent build, no components. */
export const wireBuildSummary = z.object({
  id: z.string(),
  convention_slug: z.string(),
  status: z.string(),
  error_message: z.string().nullish(),
  cancelled_by: z.string().nullish(),
  cancel_reason: z.string().nullish(),
  convention_ref: z.string().nullish(),
  published_at: z.string().nullish(),
  created_at: z.string(),
  updated_at: z.string(),
});

export const wireBuildList = z.array(wireBuildSummary);

export const wireBuild = wireBuildSummary.extend({
  archive_id: z.string(),
  components: z.array(wireComponentBuild),
});

export const wireErrorResponse = z.object({
  error: z.string(),
  message: z.string(),
  reason: z.string().nullish(),
  request_id: z.string().nullish(),
});
