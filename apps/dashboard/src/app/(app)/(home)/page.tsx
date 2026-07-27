import type { Metadata } from "next";
import { redirect } from "next/navigation";

import { isPlatformFromEnv, SELF_HOST_ARCHIVE_ID } from "@/api/config";
import { OrganisationsHome } from "@/features/organisations/OrganisationsHome";

export const metadata: Metadata = { title: "Organisations" };

export default function OrganisationsPage() {
  // Self-host has no org plane — collapse the root onto the single archive.
  // (Middleware also redirects this at the edge; this is the authoritative
  // guard so the platform-only OrganisationsHome never renders self-host.)
  if (!isPlatformFromEnv()) redirect(`/archives/${SELF_HOST_ARCHIVE_ID}`);
  return <OrganisationsHome />;
}
