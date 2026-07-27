import { PublicFrame } from "@/features/shell/PublicFrame";

export default function PublicLayout({
  children,
}: {
  children: React.ReactNode;
}) {
  return <PublicFrame>{children}</PublicFrame>;
}
