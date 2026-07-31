import { DeployingScreen } from "@/features/deployments/DeployingScreen";

/**
 * Post-create deployment progress (#185). Lives OUTSIDE `archives/[archiveId]/`
 * so it renders chrome-less (no `ArchiveShell`); it redirects into the archive
 * dashboard once the deployment succeeds.
 */
export default async function DeployingPage({
  params,
}: {
  params: Promise<{ archiveId: string }>;
}) {
  const { archiveId } = await params;
  return <DeployingScreen archiveId={archiveId} />;
}
