import type { Metadata } from "next";

import { CreateArchiveForm } from "@/features/archives/CreateArchiveForm";
import { AppShell } from "@/features/shell/AppShell";
import { Breadcrumbs } from "@/features/shell/Breadcrumbs";

export const metadata: Metadata = { title: "New archive" };

/** Mockup's dotted canvas — tokens only, no raw literals. */
const canvasStyle: React.CSSProperties = {
  minHeight: "100%",
  background:
    "radial-gradient(var(--color-border-subtle) 1px, transparent 1px) 0 0 / 24px 24px, var(--color-bg-alt)",
};

export default async function NewArchivePage({
  searchParams,
}: {
  searchParams: Promise<{ org?: string }>;
}) {
  const { org } = await searchParams;

  return (
    <AppShell breadcrumbs={<Breadcrumbs crumbs={[{ label: "New archive" }]} />}>
      <div style={canvasStyle}>
        <CreateArchiveForm defaultOrgId={org} />
      </div>
    </AppShell>
  );
}
