/**
 * Tenant-archive concepts — what lives INSIDE an OSA instance (records,
 * features, hooks, ingesters, observability, sign-in) plus platform-shaped data
 * the cloud API cannot serve yet (build lists, deployment history, members).
 *
 * In self-host, `RealOSAService` fills these from the archive's real endpoints.
 * On the platform build, `MockOSAService` returns sample values (surfaced with a
 * `<SampleDataChip/>`), since the tenant read path through the control plane
 * hasn't shipped. The `Build*`/`Deployment*`/`OrgMember` types remain cloud-only.
 */

import type { BuildStatusKind } from "./build";
import type { Role } from "./organisation";

/** "What's in here" hero stats. */
export interface RecordStats {
  publishedRecords: number;
  recordsThisMonth: number;
  derivedFeaturesPerRecord: number;
  storageBytes: number;
}

export interface RecordTypeCount {
  type: string;
  count: number;
}

/** A published record row, schema-agnostic: id + created date + its metadata. */
export interface TenantRecord {
  id: string;
  schema: string;
  createdAt: Date;
  /** The record's metadata fields (schema-specific), keyed by field name. */
  fields: Record<string, unknown>;
}

/** A registered derived-feature table (one per hook, per schema). */
export interface FeatureTable {
  name: string;
  schema: string;
  columns: number;
  rows: number;
}

export interface TenantHook {
  name: string;
  liveVersion: string;
  description: string;
  lastRunAt: Date | null;
}

export interface TenantIngester {
  name: string;
  schema: string;
  description: string;
  image: string;
  digest: string;
  /** Cron expression, or null for a manual/one-shot ingester. */
  schedule: string | null;
}

export type IngestionStatus = "pending" | "running" | "completed" | "failed";

/** One ingest run. `pending`/`running` are still in-progress. */
export interface IngestionRun {
  id: string;
  convention: string;
  status: IngestionStatus;
  ingestionFinished: boolean;
  batchesIngested: number;
  batchesCompleted: number;
  batchesFailed: number;
  publishedCount: number;
  startedAt: Date;
  completedAt: Date | null;
  failureReason: string | null;
}

export interface ObservabilityComponent {
  name: string;
  status: "healthy" | "degraded";
  detail: string;
}

export interface ObservabilitySnapshot {
  status: "ready" | "degraded";
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
