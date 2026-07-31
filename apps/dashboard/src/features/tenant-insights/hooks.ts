"use client";

/**
 * Query hooks over OSAService. In self-host these read the archive's real
 * endpoints (through the BFF proxy); on the platform build the same methods are
 * served by `MockOSAService`, so a panel shows a <SampleDataChip/> based on
 * `useServices().isPlatform`, not on the data.
 */
import { useQuery } from "@tanstack/react-query";

import { useServices } from "@/api/services";

const tenantKeys = {
  recordStats: (id: string) => ["tenant", id, "record-stats"] as const,
  recordTypes: (id: string) => ["tenant", id, "record-types"] as const,
  schemas: (id: string) => ["tenant", id, "schemas"] as const,
  records: (id: string, schema: string) =>
    ["tenant", id, "records", schema] as const,
  featureTables: (id: string) => ["tenant", id, "feature-tables"] as const,
  hooks: (id: string) => ["tenant", id, "hooks"] as const,
  ingesters: (id: string) => ["tenant", id, "ingesters"] as const,
  ingestions: (id: string) => ["tenant", id, "ingestions"] as const,
  observability: (id: string) => ["tenant", id, "observability"] as const,
  authConfig: (id: string) => ["tenant", id, "auth-config"] as const,
};

export function useRecordStats(archiveId: string) {
  const { osa } = useServices();
  return useQuery({
    queryKey: tenantKeys.recordStats(archiveId),
    queryFn: () => osa.getRecordStats(archiveId),
  });
}

export function useRecordTypeBreakdown(archiveId: string) {
  const { osa } = useServices();
  return useQuery({
    queryKey: tenantKeys.recordTypes(archiveId),
    queryFn: () => osa.getRecordTypeBreakdown(archiveId),
  });
}

export function useTenantSchemas(archiveId: string) {
  const { osa } = useServices();
  return useQuery({
    queryKey: tenantKeys.schemas(archiveId),
    queryFn: () => osa.listSchemas(archiveId),
  });
}

export function useTenantRecords(archiveId: string, schema: string | undefined) {
  const { osa } = useServices();
  return useQuery({
    queryKey: tenantKeys.records(archiveId, schema ?? ""),
    queryFn: () => osa.listRecords(archiveId, schema as string),
    enabled: schema !== undefined,
  });
}

export function useTenantFeatureTables(archiveId: string) {
  const { osa } = useServices();
  return useQuery({
    queryKey: tenantKeys.featureTables(archiveId),
    queryFn: () => osa.listFeatureTables(archiveId),
  });
}

export function useTenantHooks(archiveId: string) {
  const { osa } = useServices();
  return useQuery({
    queryKey: tenantKeys.hooks(archiveId),
    queryFn: () => osa.listHooks(archiveId),
  });
}

export function useTenantIngesters(archiveId: string) {
  const { osa } = useServices();
  return useQuery({
    queryKey: tenantKeys.ingesters(archiveId),
    queryFn: () => osa.listIngesters(archiveId),
  });
}

export function useIngestionRuns(archiveId: string) {
  const { osa } = useServices();
  return useQuery({
    queryKey: tenantKeys.ingestions(archiveId),
    queryFn: () => osa.listIngestionRuns(archiveId),
    // Poll while any run is in-progress so counters update live.
    refetchInterval: (query) => {
      const active = query.state.data?.some(
        (r) => r.status === "pending" || r.status === "running",
      );
      return active ? 5000 : false;
    },
  });
}

export function useObservability(archiveId: string) {
  const { osa } = useServices();
  return useQuery({
    queryKey: tenantKeys.observability(archiveId),
    queryFn: () => osa.getObservability(archiveId),
  });
}

export function useTenantAuthConfig(archiveId: string) {
  const { osa } = useServices();
  return useQuery({
    queryKey: tenantKeys.authConfig(archiveId),
    queryFn: () => osa.getAuthConfig(archiveId),
  });
}
