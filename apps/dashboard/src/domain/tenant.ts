/**
 * Tenant-archive concepts — what lives INSIDE an OSA instance (records,
 * validation, usage) plus platform-shaped data the cloud API cannot serve
 * yet (build lists, deployment history, members).
 *
 * Every consumer of these types receives them wrapped in `Mocked<T>`
 * (domain/mocked.ts): the open-source OSA server has endpoints for some of
 * this, but the dashboard has no auth path to tenant instances today, and
 * the cloud API has no list/aggregate endpoints for the rest.
 */

import type { BuildStatusKind } from "./build";
import type { Role } from "./organisation";

/** "What's in here" hero stats. */
export interface RecordStats {
  publishedRecords: number;
  recordsThisMonth: number;
  depositors: number;
  labs: number;
  derivedFeaturesPerRecord: number;
  storageBytes: number;
}

/** One month of accepted depositions (bar chart). */
export interface DepositionPoint {
  /** ISO month, e.g. "2026-03". */
  month: string;
  count: number;
}

export interface RecordTypeCount {
  type: string;
  count: number;
}

export interface ValidationCheck {
  name: string;
  /** Which convention or policy defines the check. */
  definedBy: string;
  passing: number;
  failing: number;
}

export interface ValidationSummary {
  /** Share of records passing every check, 0–100. */
  passRatePercent: number;
  lastFullPassAt: Date | null;
  checks: ValidationCheck[];
}

/** "Who's using it" usage stats. */
export interface UsageStats {
  recordDownloads: number;
  uniqueClients: number;
  apiQueries: number;
  /** Share of API queries from agents, 0–1. */
  agentQueryShare: number;
  bulkExports: number;
  mirroringNodes: number;
}

export interface TenantRecord {
  id: string;
  title: string;
  type: string;
  depositor: string;
  depositedAt: Date;
}

export interface TenantHook {
  name: string;
  liveVersion: string;
  description: string;
  lastRunAt: Date | null;
}

export interface TenantIngester {
  name: string;
  liveVersion: string;
  description: string;
  acceptedFormats: string[];
}

export interface ObservabilityComponent {
  name: string;
  status: "healthy" | "degraded";
  detail: string;
}

export interface ObservabilitySnapshot {
  status: "ready" | "degraded";
  uptimeDays: number;
  components: ObservabilityComponent[];
}

/** The archive's sign-in configuration as shown on the Authentication page. */
export interface TenantAuthView {
  provider: "orcid";
  clientId: string;
  adminOrcidIds: string[];
}

/** A row in the (API-less) builds list. */
export interface BuildListItem {
  id: string;
  conventionSlug: string;
  conventionRef: string | null;
  statusKind: BuildStatusKind;
  createdAt: Date;
}

/** A past deployment (the cloud API only serves the latest). */
export interface DeploymentHistoryEntry {
  id: string;
  deployedAt: Date;
  fromRef: string;
  durationSeconds: number;
  outcome: "succeeded" | "failed";
}

/** An organisation member (membership API is deferred). */
export interface OrgMember {
  name: string;
  email: string;
  role: Role;
  joinedAt: Date;
}

export interface SearchOverview {
  indexedRecords: number;
  lastIndexedAt: Date | null;
  exampleQueries: string[];
}
