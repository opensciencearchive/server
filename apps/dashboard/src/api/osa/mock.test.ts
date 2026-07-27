import { describe, expect, it } from "vitest";

import { MockOSAService } from "./mock";

describe("MockOSAService", () => {
  const svc = new MockOSAService();

  it("returns Mocked-branded record stats", async () => {
    const stats = await svc.getRecordStats("arch_x");
    expect(stats.__mock).toBe(true);
    expect(stats.data.publishedRecords).toBeGreaterThan(0);
  });

  it("returns twelve months of depositions", async () => {
    const series = await svc.getDepositionSeries("arch_x");
    expect(series.data).toHaveLength(12);
  });

  it("validation summary is consistent with its checks", async () => {
    const v = await svc.getValidationSummary("arch_x");
    expect(v.data.checks.length).toBeGreaterThan(0);
    expect(v.data.passRatePercent).toBeGreaterThan(0);
    expect(v.data.passRatePercent).toBeLessThanOrEqual(100);
  });
});
