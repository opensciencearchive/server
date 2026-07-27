import type { Organisation } from "./organisation";

/** A person's platform identity. Email is contact-only; login is via OIDC. */
export interface User {
  id: string;
  email: string;
  createdAt: Date;
}

/** The authenticated caller and their memberships (from GET /auth/me). */
export interface Session {
  user: User;
  organisations: Organisation[];
}

/** Uppercase initials for the avatar chip, e.g. "r.bergstrom@…" → "RB". */
export function userInitials(email: string): string {
  const local = email.split("@")[0] ?? "";
  const parts = local.split(/[._-]+/).filter(Boolean);
  const letters =
    parts.length >= 2
      ? [parts[0]![0], parts[parts.length - 1]![0]]
      : [local[0], local[1]];
  return letters.filter(Boolean).join("").toUpperCase() || "?";
}
