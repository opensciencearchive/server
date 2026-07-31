// @vitest-environment node
import { afterEach, describe, expect, it, vi } from "vitest";

import type { TenantAuthView } from "@/domain/tenant";

import { RealOSAService } from "./real";

const STATS = {
  records: 10,
  records_this_month: 2,
  features_per_record: 3,
  storage_bytes: 100,
};

function spyFetch(payload: unknown): string[] {
  const seen: string[] = [];
  vi.spyOn(globalThis, "fetch").mockImplementation((url: RequestInfo | URL) => {
    seen.push(String(url));
    return Promise.resolve(Response.json(payload));
  });
  return seen;
}

afterEach(() => {
  vi.restoreAllMocks();
});

describe("RealOSAService base resolution", () => {
  it("self-host ignores archiveId and reads the single-archive proxy base", async () => {
    const seen = spyFetch(STATS);
    await new RealOSAService(() => "/api/osa").getRecordStats("arch_ignored");
    expect(seen[0]).toBe("/api/osa/stats");
  });

  it("platform resolves the per-archive control-plane read-proxy base", async () => {
    const seen = spyFetch(STATS);
    const svc = new RealOSAService(
      (id) => `/api/amacrin/api/v1/archives/${id}/osa`,
    );
    await svc.getRecordStats("arch_1");
    expect(seen[0]).toBe("/api/amacrin/api/v1/archives/arch_1/osa/stats");
  });

  it("uses the injected auth-config source instead of the bespoke route", async () => {
    const fetchSpy = vi.spyOn(globalThis, "fetch");
    const override = vi.fn((): Promise<TenantAuthView> => Promise.resolve({} as TenantAuthView));
    const svc = new RealOSAService(() => "/base", { getAuthConfig: override });

    await svc.getAuthConfig("arch_1");

    expect(override).toHaveBeenCalledWith("arch_1");
    expect(fetchSpy).not.toHaveBeenCalled(); // never hits /api/auth-config
  });
});
