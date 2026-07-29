import type { Archive } from "@/domain/archive";
import { SELF_HOST_ARCHIVE_ID } from "@/api/config";

/**
 * The stand-in `Archive` for a self-hosted node. There is no cloud archive
 * record — this server *is* the archive — so project pages that expect an
 * `Archive` (shell, overview) get this synthetic one. Cloud-only fields
 * (organisation, deployment config, public domain) are empty and the self-host
 * UI never surfaces them. M2c may enrich `name` from node discovery (`GET /`).
 */
export function syntheticLocalArchive(): Archive {
  return {
    id: SELF_HOST_ARCHIVE_ID,
    organisationId: "",
    name: "Local archive",
    slug: SELF_HOST_ARCHIVE_ID,
    domain: "",
    status: { kind: "running" },
    orcidAdmins: [],
    deploymentConfig: null,
    createdAt: new Date(0),
    updatedAt: new Date(0),
  };
}
