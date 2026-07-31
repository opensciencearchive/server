import { describe, expect, it } from "vitest";

import {
  decodeAgentSurface,
  decodeAuthConfig,
  decodeFeatureTables,
  decodeHookCatalog,
  decodeIngesterCatalog,
  decodeIngestionRuns,
  decodeObservability,
  decodeRecordStats,
  decodeRecordsPage,
} from "./decode";

describe("decodeHookCatalog", () => {
  it("maps a hook with a live release", () => {
    const out = decodeHookCatalog({
      items: [
        {
          name: "validate-metadata",
          // Real TableFeatureSpec has kind/cardinality/columns and NO name.
          feature: {
            kind: "table",
            cardinality: "many",
            columns: [{ name: "a" }, { name: "b" }],
          },
          live_release: {
            version: 3,
            digest: "sha256:abc",
            source_ref: "git@sha",
            built_at: "2026-07-25T16:20:00Z",
          },
        },
      ],
    });

    expect(out).toHaveLength(1);
    expect(out[0]).toMatchObject({
      name: "validate-metadata",
      liveVersion: "v3",
    });
    expect(out[0]!.description).toContain("validate-metadata");
    expect(out[0]!.description).toContain("2 columns");
    expect(out[0]!.lastRunAt).toEqual(new Date("2026-07-25T16:20:00Z"));
  });

  it("handles a hook with no live release", () => {
    const out = decodeHookCatalog({
      items: [{ name: "x", feature: { name: "f" }, live_release: null }],
    });
    expect(out[0]!.liveVersion).toBe("—");
    expect(out[0]!.lastRunAt).toBeNull();
  });

  it("rejects a malformed payload", () => {
    expect(() => decodeHookCatalog({ items: [{ name: 123 }] })).toThrow();
  });
});

describe("decodeAgentSurface", () => {
  const discovery = {
    node: {
      name: "My Archive",
      domain: "archive.example.org",
      description: "d",
      osa_version: "0.0.7",
    },
    skill_url: "https://archive.example.org/SKILL.md",
    reference_base: "https://archive.example.org/api/v1/data",
    data_url: "https://archive.example.org/api/v1/data",
    openapi_url: "https://archive.example.org/api/v1/openapi.json",
  };

  it("maps skill + node and derives the /mcp url from the origin", () => {
    const out = decodeAgentSurface({ skill: "# Hello", discovery });
    expect(out.skillMarkdown).toBe("# Hello");
    expect(out.node.name).toBe("My Archive");
    expect(out.node.osaVersion).toBe("0.0.7");
    expect(out.mcpUrl).toBe("https://archive.example.org/mcp");
  });

  it("falls back to /mcp when skill_url is relative", () => {
    const out = decodeAgentSurface({
      skill: "x",
      discovery: { ...discovery, skill_url: "/SKILL.md" },
    });
    expect(out.mcpUrl).toBe("/mcp");
  });
});

describe("decodeRecordStats", () => {
  it("maps the stats envelope to the record-stats tiles", () => {
    const out = decodeRecordStats({
      records: 42,
      records_this_month: 5,
      storage_bytes: 1024,
      features_per_record: 2.5,
      computed_at: "2026-07-27T00:00:00Z",
      data_url: "/api/v1/data",
    });
    expect(out).toEqual({
      publishedRecords: 42,
      recordsThisMonth: 5,
      storageBytes: 1024,
      derivedFeaturesPerRecord: 2.5,
    });
  });
});

describe("decodeRecordsPage", () => {
  it("splits implicit columns from schema-specific metadata fields", () => {
    const out = decodeRecordsPage(
      {
        rows: [
          {
            id: "abc",
            srn: "urn:osa:localhost:rec:abc@1",
            schema_id: "s",
            version: 1,
            created_at: "2026-07-24T10:31:00Z",
            title: "A record",
            cells: 8420,
          },
        ],
        next_cursor: null,
        has_more: false,
      },
      "station-timeseries@2.1.0",
    );
    expect(out).toHaveLength(1);
    expect(out[0]!.id).toBe("abc");
    expect(out[0]!.schema).toBe("station-timeseries@2.1.0");
    expect(out[0]!.createdAt).toEqual(new Date("2026-07-24T10:31:00Z"));
    expect(out[0]!.fields).toEqual({ title: "A record", cells: 8420 });
  });
});

describe("decodeFeatureTables", () => {
  it("keeps only feature-kind tables and counts their columns", () => {
    const out = decodeFeatureTables(
      {
        id: "s",
        version: "1.0.0",
        title: "S",
        table_resources: [
          { name: "records", kind: "records", row_count: 10 },
          {
            name: "qc-metrics",
            kind: "feature",
            row_count: 10,
            columns: [{ name: "a" }, { name: "b" }],
          },
        ],
      },
      "s@1.0.0",
    );
    expect(out).toEqual([
      { name: "qc-metrics", schema: "s@1.0.0", columns: 2, rows: 10 },
    ]);
  });
});

describe("decodeIngesterCatalog", () => {
  it("maps items to tenant ingesters", () => {
    const out = decodeIngesterCatalog({
      items: [
        {
          name: "geo@1",
          title: "GHCN ingester",
          description: "d",
          schema_id: "geo@1.0.0",
          image: "ghcr.io/x/geo",
          digest: "sha256:abc",
          source_ref: null,
          schedule: "0 3 * * *",
        },
      ],
    });
    expect(out[0]).toEqual({
      name: "GHCN ingester",
      schema: "geo@1.0.0",
      description: "d",
      image: "ghcr.io/x/geo",
      digest: "sha256:abc",
      schedule: "0 3 * * *",
    });
  });
});

describe("decodeIngestionRuns", () => {
  it("maps runs and keeps in-progress statuses", () => {
    const out = decodeIngestionRuns({
      items: [
        {
          id: "ing_1",
          convention_id: "c@1.0.0",
          status: "running",
          ingestion_finished: false,
          batches_ingested: 12,
          batches_completed: 9,
          batches_failed: 0,
          published_count: 8940,
          started_at: "2026-07-27T09:15:00Z",
          completed_at: null,
          failure_reason: null,
        },
        {
          id: "ing_2",
          convention_id: "c@1.0.0",
          status: "failed",
          ingestion_finished: false,
          batches_ingested: 3,
          batches_completed: 2,
          batches_failed: 1,
          published_count: 1920,
          started_at: "2026-07-24T11:02:00Z",
          completed_at: "2026-07-24T11:09:00Z",
          failure_reason: "OOM on batch 3",
        },
      ],
    });
    expect(out[0]!.status).toBe("running");
    expect(out[0]!.completedAt).toBeNull();
    expect(out[1]!.failureReason).toBe("OOM on batch 3");
    expect(out[1]!.completedAt).toEqual(new Date("2026-07-24T11:09:00Z"));
  });
});

describe("decodeObservability", () => {
  it("maps readiness components (error → degraded, else healthy)", () => {
    const out = decodeObservability({
      status: "ready",
      version: "0.0.7",
      components: {
        db: { status: "ok", detail: "1/100" },
        runner: { status: "unchecked" },
        workers: { status: "error", detail: "down" },
      },
    });
    expect(out.status).toBe("ready");
    const byName = Object.fromEntries(out.components.map((c) => [c.name, c]));
    expect(byName.db!.status).toBe("healthy");
    expect(byName.runner!.status).toBe("healthy");
    expect(byName.runner!.detail).toBe("not checked");
    expect(byName.workers!.status).toBe("degraded");
  });
});

describe("decodeAuthConfig", () => {
  it("maps client id + admins and never surfaces a secret", () => {
    const out = decodeAuthConfig({
      provider: "orcid",
      client_id: "APP-123",
      admin_orcids: ["0000-0002-1825-0097"],
    });
    expect(out).toEqual({
      provider: "orcid",
      clientId: "APP-123",
      adminOrcidIds: ["0000-0002-1825-0097"],
    });
  });
});
