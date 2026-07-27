/**
 * MockOSAService — the only OSAService implementation until a
 * dashboard→tenant auth path exists. Sample data tells the Cortex-cell-atlas
 * story from the design mockup; deterministic on purpose.
 */
import { type Mocked, mocked } from "@/domain/mocked";
import type {
  ObservabilitySnapshot,
  RecordStats,
  RecordTypeCount,
  TenantAuthView,
  TenantHook,
  TenantIngester,
  TenantRecord,
  UsageStats,
  ValidationSummary,
} from "@/domain/tenant";

import type { OSAService } from "./service";

export class MockOSAService implements OSAService {
  getRecordStats(archiveId: string): Promise<Mocked<RecordStats>> {
    void archiveId;
    return Promise.resolve(
      mocked({
        publishedRecords: 12_481,
        recordsThisMonth: 318,
        derivedFeaturesPerRecord: 3,
        storageBytes: 1.4e12,
      }),
    );
  }

  getRecordTypeBreakdown(
    archiveId: string,
  ): Promise<Mocked<RecordTypeCount[]>> {
    void archiveId;
    return Promise.resolve(
      mocked([
        { type: "Single-cell RNA-seq", count: 7204 },
        { type: "Spatial transcriptomics", count: 3118 },
        { type: "Bulk RNA-seq", count: 1502 },
        { type: "Imaging", count: 657 },
      ]),
    );
  }

  getValidationSummary(archiveId: string): Promise<Mocked<ValidationSummary>> {
    void archiveId;
    return Promise.resolve(
      mocked({
        passRatePercent: 99.9,
        lastFullPassAt: new Date("2026-07-25T16:22:00Z"),
        checks: [
          {
            name: "Ontology terms resolve",
            definedBy: "geo-rnaseq-v2",
            passing: 12_481,
            failing: 0,
          },
          {
            name: "Counts matrix well-formed",
            definedBy: "geo-rnaseq-v2",
            passing: 12_481,
            failing: 0,
          },
          {
            name: "Assay metadata complete",
            definedBy: "geo-rnaseq-v2",
            passing: 12_469,
            failing: 12,
          },
          {
            name: "Donor consent recorded",
            definedBy: "marsh-lab-policy",
            passing: 12_481,
            failing: 0,
          },
        ],
      }),
    );
  }

  getUsageStats(archiveId: string): Promise<Mocked<UsageStats>> {
    void archiveId;
    return Promise.resolve(
      mocked({
        recordDownloads: 8940,
        uniqueClients: 1204,
        apiQueries: 14_902,
        agentQueryShare: 0.62,
        bulkExports: 62,
        mirroringNodes: 2,
      }),
    );
  }

  listRecords(archiveId: string): Promise<Mocked<TenantRecord[]>> {
    void archiveId;
    return Promise.resolve(
      mocked([
        {
          id: "srn:rec:9f2c4a1e",
          title: "Prefrontal cortex snRNA-seq, donor H-1042",
          type: "Single-cell RNA-seq",
          depositor: "0000-0002-1825-0097",
          depositedAt: new Date("2026-07-24T10:31:00Z"),
        },
        {
          id: "srn:rec:1a2b3c4d",
          title: "Visium spatial series, BA9 section 3",
          type: "Spatial transcriptomics",
          depositor: "0000-0001-5109-3700",
          depositedAt: new Date("2026-07-23T15:02:00Z"),
        },
        {
          id: "srn:rec:5e6f7a8b",
          title: "Bulk RNA-seq, cortical organoid batch 12",
          type: "Bulk RNA-seq",
          depositor: "0000-0003-1415-9265",
          depositedAt: new Date("2026-07-22T09:47:00Z"),
        },
        {
          id: "srn:rec:3c4d5e6f",
          title: "Immunofluorescence panel, layer V neurons",
          type: "Imaging",
          depositor: "0000-0002-7183-4581",
          depositedAt: new Date("2026-07-21T18:20:00Z"),
        },
      ]),
    );
  }

  listHooks(archiveId: string): Promise<TenantHook[]> {
    void archiveId;
    return Promise.resolve([
      {
        name: "validate-metadata",
        liveVersion: "build_8f3a91c2e5",
        description: "Rejects depositions with incomplete assay metadata.",
        lastRunAt: new Date("2026-07-25T16:20:00Z"),
      },
      {
        name: "resolve-ontology",
        liveVersion: "build_8f3a91c2e5",
        description: "Resolves tissue and cell-type terms against UBERON/CL.",
        lastRunAt: new Date("2026-07-25T16:21:30Z"),
      },
    ]);
  }

  listIngesters(archiveId: string): Promise<Mocked<TenantIngester[]>> {
    void archiveId;
    return Promise.resolve(
      mocked([
        {
          name: "geo-ingester",
          liveVersion: "build_8f3a91c2e5",
          description:
            "Imports GEO series and normalises them into the archive convention.",
          acceptedFormats: ["SOFT", "MINiML", "supplementary TAR"],
        },
      ]),
    );
  }

  getObservability(archiveId: string): Promise<Mocked<ObservabilitySnapshot>> {
    void archiveId;
    return Promise.resolve(
      mocked({
        status: "ready",
        components: [
          { name: "api", status: "healthy", detail: "p99 84 ms" },
          { name: "db", status: "healthy", detail: "connections 12/100" },
          { name: "workers", status: "healthy", detail: "queue depth 0" },
          { name: "runner", status: "healthy", detail: "2 hooks live" },
        ],
      }),
    );
  }

  getAuthConfig(archiveId: string): Promise<Mocked<TenantAuthView>> {
    void archiveId;
    return Promise.resolve(
      mocked({
        provider: "orcid",
        clientId: "APP-K91F2LQ8XZ40MNRT",
        adminOrcidIds: ["0000-0002-1825-0097", "0000-0001-5109-3700"],
      }),
    );
  }
}
