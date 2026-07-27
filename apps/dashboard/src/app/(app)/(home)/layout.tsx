import { AppShell } from "@/features/shell/AppShell";
import { HomeSidebar } from "@/features/shell/HomeSidebar";

export default function HomeLayout({
  children,
}: {
  children: React.ReactNode;
}) {
  return <AppShell sidebar={<HomeSidebar />}>{children}</AppShell>;
}
