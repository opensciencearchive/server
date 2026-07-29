import { ArchiveOverview } from "@/features/archives/ArchiveOverview";

export default async function ArchivePage({
  params,
}: {
  params: Promise<{ archiveId: string }>;
}) {
  const { archiveId } = await params;
  return <ArchiveOverview archiveId={archiveId} />;
}
