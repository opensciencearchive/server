import { ArchiveShell } from "@/features/archives/ArchiveShell";

export default async function ArchiveLayout({
  children,
  params,
}: {
  children: React.ReactNode;
  params: Promise<{ archiveId: string }>;
}) {
  const { archiveId } = await params;
  return <ArchiveShell archiveId={archiveId}>{children}</ArchiveShell>;
}
