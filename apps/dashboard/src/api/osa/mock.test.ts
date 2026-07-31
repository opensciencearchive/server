import { describe, expect, it } from "vitest";

import { MockOSAService } from "./mock";

describe("MockOSAService", () => {
  const svc = new MockOSAService();

  it("returns plain record stats", async () => {
    const stats = await svc.getRecordStats("arch_x");
    expect(stats.publishedRecords).toBeGreaterThan(0);
    expect(stats.storageBytes).toBeGreaterThan(0);
  });

  it("lists records for a schema with metadata fields", async () => {
    const schemas = await svc.listSchemas("arch_x");
    expect(schemas.length).toBeGreaterThan(0);
    const records = await svc.listRecords("arch_x", schemas[0]!);
    expect(records.length).toBeGreaterThan(0);
    expect(records[0]!.schema).toBe(schemas[0]);
    expect(Object.keys(records[0]!.fields).length).toBeGreaterThan(0);
  });

  it("lists feature tables and ingesters", async () => {
    expect((await svc.listFeatureTables("arch_x")).length).toBeGreaterThan(0);
    const ingesters = await svc.listIngesters("arch_x");
    expect(ingesters[0]!.digest).toMatch(/^sha256:/);
  });
});
