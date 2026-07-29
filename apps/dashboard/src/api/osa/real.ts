/**
 * RealOSAService — reads the local archive through the same-origin BFF proxy
 * (`/api/osa/*`), which attaches the server-held SUPERADMIN token (issue #173).
 *
 * Only the surfaces with a shipped, mapped endpoint are real; the rest still
 * come from `MockOSAService` (sample data, shown with a chip) and flip to real
 * method-by-method as endpoints and mappings land — exactly as `service.ts`
 * prescribes. Runs client-side, so a relative `baseUrl` resolves to the
 * dashboard origin and rides the httpOnly session cookie.
 */
import type { Mocked } from "@/domain/mocked";
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

import { MockOSAService } from "./mock";
import type { OSAService } from "./service";
import { decodeHookCatalog } from "./wire/decode";

export class RealOSAService implements OSAService {
  private readonly baseUrl: string;
  private readonly mock = new MockOSAService();

  constructor(baseUrl: string) {
    this.baseUrl = baseUrl.replace(/\/$/, "");
  }

  private async getJson(path: string): Promise<unknown> {
    const res = await fetch(`${this.baseUrl}${path}`, {
      headers: { accept: "application/json" },
    });
    if (!res.ok) {
      throw new Error(`OSA request failed (${res.status}) for ${path}`);
    }
    return res.json();
  }

  // ── real ────────────────────────────────────────────────────────────
  async listHooks(archiveId: string): Promise<TenantHook[]> {
    void archiveId;
    return decodeHookCatalog(await this.getJson("/hooks"));
  }

  // ── mocked (no mapped endpoint yet) ───────────────────────────────────
  getRecordStats(archiveId: string): Promise<Mocked<RecordStats>> {
    return this.mock.getRecordStats(archiveId);
  }
  getRecordTypeBreakdown(archiveId: string): Promise<Mocked<RecordTypeCount[]>> {
    return this.mock.getRecordTypeBreakdown(archiveId);
  }
  getValidationSummary(archiveId: string): Promise<Mocked<ValidationSummary>> {
    return this.mock.getValidationSummary(archiveId);
  }
  getUsageStats(archiveId: string): Promise<Mocked<UsageStats>> {
    return this.mock.getUsageStats(archiveId);
  }
  listRecords(archiveId: string): Promise<Mocked<TenantRecord[]>> {
    return this.mock.listRecords(archiveId);
  }
  listIngesters(archiveId: string): Promise<Mocked<TenantIngester[]>> {
    return this.mock.listIngesters(archiveId);
  }
  getObservability(archiveId: string): Promise<Mocked<ObservabilitySnapshot>> {
    return this.mock.getObservability(archiveId);
  }
  getAuthConfig(archiveId: string): Promise<Mocked<TenantAuthView>> {
    return this.mock.getAuthConfig(archiveId);
  }
}
