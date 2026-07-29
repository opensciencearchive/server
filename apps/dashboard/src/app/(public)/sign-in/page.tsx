import type { Metadata } from "next";

import { apiBaseUrlFromEnv, isPlatformFromEnv } from "@/api/config";
import { PasswordSignInCard } from "@/features/auth/PasswordSignInCard";
import { SignInCard } from "@/features/auth/SignInCard";

export const metadata: Metadata = { title: "Sign in" };

export default async function SignInPage({
  searchParams,
}: {
  searchParams: Promise<{ error?: string }>;
}) {
  // Self-host: a dashboard credential form. Platform: the cloud OAuth card.
  if (!isPlatformFromEnv()) return <PasswordSignInCard />;
  const { error } = await searchParams;
  return <SignInCard apiBaseUrl={apiBaseUrlFromEnv()} error={error ?? null} />;
}
