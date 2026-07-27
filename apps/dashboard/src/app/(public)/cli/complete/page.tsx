import type { Metadata } from "next";

import { CliComplete } from "@/features/auth/CliComplete";

export const metadata: Metadata = { title: "CLI signed in" };

export default function CliCompletePage() {
  return <CliComplete />;
}
