"use client";

/**
 * Query hooks over OSAService — ALL of this is `Mocked<T>` sample data
 * until a dashboard→tenant auth path exists. Components unwrapping these
 * must render a <SampleDataChip/> beside the section they feed.
 */
import { useQuery } from "@tanstack/react-query";

import { useServices } from "@/api/services";

const tenantKeys = {
  recordStats: (id: string) => ["tenant", id, "record-stats"] as const,
  depositions: (id: string) => ["tenant", id, "depositions"] as const,
  recordTypes: (id: string) => ["tenant", id, "record-types"] as const,
  validation: (id: string) => ["tenant", id, "validation"] as const,
  usage: (id: string) => ["tenant", id, "usage"] as const,
  records: (id: string) => ["tenant", id, "records"] as const,
  hooks: (id: string) => ["tenant", id, "hooks"] as const,
  ingesters: (id: string) => ["tenant", id, "ingesters"] as const,
  search: (id: string) => ["tenant", id, "search"] as const,
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

export function useDepositionSeries(archiveId: string) {
  const { osa } = useServices();
  return useQuery({
    queryKey: tenantKeys.depositions(archiveId),
    queryFn: () => osa.getDepositionSeries(archiveId),
  });
}

export function useRecordTypeBreakdown(archiveId: string) {
  const { osa } = useServices();
  return useQuery({
    queryKey: tenantKeys.recordTypes(archiveId),
    queryFn: () => osa.getRecordTypeBreakdown(archiveId),
  });
}

export function useValidationSummary(archiveId: string) {
  const { osa } = useServices();
  return useQuery({
    queryKey: tenantKeys.validation(archiveId),
    queryFn: () => osa.getValidationSummary(archiveId),
  });
}

export function useUsageStats(archiveId: string) {
  const { osa } = useServices();
  return useQuery({
    queryKey: tenantKeys.usage(archiveId),
    queryFn: () => osa.getUsageStats(archiveId),
  });
}

export function useTenantRecords(archiveId: string) {
  const { osa } = useServices();
  return useQuery({
    queryKey: tenantKeys.records(archiveId),
    queryFn: () => osa.listRecords(archiveId),
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

export function useSearchOverview(archiveId: string) {
  const { osa } = useServices();
  return useQuery({
    queryKey: tenantKeys.search(archiveId),
    queryFn: () => osa.getSearchOverview(archiveId),
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
