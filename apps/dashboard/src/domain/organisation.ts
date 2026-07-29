/** A user's role within an organisation. Ordered: Member < Admin < Owner. */
export type Role = "member" | "admin" | "owner";

export const ROLES: readonly Role[] = ["member", "admin", "owner"];

const RANK: Record<Role, number> = { member: 0, admin: 1, owner: 2 };

/** "At least role X" — higher roles inherit everything below them. */
export function roleAtLeast(have: Role, need: Role): boolean {
  return RANK[have] >= RANK[need];
}

export function isRole(value: string): value is Role {
  return (ROLES as readonly string[]).includes(value);
}

/** Unit of tenancy; owns archives. */
export interface Organisation {
  id: string;
  name: string;
  /** The caller's role, or null outside a membership context. */
  role: Role | null;
  createdAt: Date;
}
