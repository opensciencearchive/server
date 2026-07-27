import { OrgShell } from "@/features/organisations/OrgShell";

export default async function OrgLayout({
  children,
  params,
}: {
  children: React.ReactNode;
  params: Promise<{ orgId: string }>;
}) {
  const { orgId } = await params;
  return <OrgShell orgId={orgId}>{children}</OrgShell>;
}
