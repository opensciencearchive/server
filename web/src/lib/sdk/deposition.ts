/**
 * Deposition namespace for the OSA SDK.
 */

import type { HttpClient } from './http';
import type {
  ConventionListResponse,
  ConventionDetail,
  CreateDepositionResponse,
  Deposition,
  SpreadsheetUploadResponse,
  FileUploadResponse,
} from '@/types';

// ---------------------------------------------------------------------------
// Public interface
// ---------------------------------------------------------------------------

export interface DepositionInterface {
  /** List available conventions. */
  listConventions(): Promise<ConventionListResponse>;

  /** Get full convention details by SRN. */
  getConvention(srn: string): Promise<ConventionDetail>;

  /** Create a new deposition against a convention. */
  create(conventionSrn: string): Promise<CreateDepositionResponse>;

  /** Get a deposition by SRN. */
  get(srn: string): Promise<Deposition>;

  /** Download the metadata template for a convention. */
  downloadTemplate(conventionSrn: string): Promise<Blob>;

  /** Upload a metadata spreadsheet to a deposition. */
  uploadSpreadsheet(depositionSrn: string, file: File): Promise<SpreadsheetUploadResponse>;

  /** Upload a data file to a deposition. */
  uploadFile(depositionSrn: string, file: File): Promise<FileUploadResponse>;

  /** Delete a file from a deposition. */
  deleteFile(depositionSrn: string, filename: string): Promise<void>;

  /** Submit a deposition for validation. */
  submit(depositionSrn: string): Promise<void>;
}

// ---------------------------------------------------------------------------
// Real HTTP implementation
// ---------------------------------------------------------------------------

export class DepositionNamespace implements DepositionInterface {
  constructor(private http: HttpClient) {}

  /** List available conventions. */
  async listConventions(): Promise<ConventionListResponse> {
    const res = await this.http.fetch('/conventions');
    if (!res.ok) throw await res.json();
    return res.json();
  }

  /** Get full convention details by SRN. */
  async getConvention(srn: string): Promise<ConventionDetail> {
    const res = await this.http.fetch(`/conventions/${encodeURIComponent(srn)}`);
    if (!res.ok) throw await res.json();
    return res.json();
  }

  /** Create a new deposition against a convention. */
  async create(conventionSrn: string): Promise<CreateDepositionResponse> {
    const res = await this.http.fetch('/depositions', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ convention_srn: conventionSrn }),
    });
    if (!res.ok) throw await res.json();
    return res.json();
  }

  /** Get a deposition by SRN. */
  async get(srn: string): Promise<Deposition> {
    const res = await this.http.fetch(`/depositions/${encodeURIComponent(srn)}`);
    if (!res.ok) throw await res.json();
    return res.json();
  }

  /** Download the metadata template for a convention. */
  async downloadTemplate(conventionSrn: string): Promise<Blob> {
    const res = await this.http.fetch(
      `/conventions/${encodeURIComponent(conventionSrn)}/template`,
    );
    if (!res.ok) throw await res.json();
    return res.blob();
  }

  /** Upload a metadata spreadsheet to a deposition. */
  async uploadSpreadsheet(depositionSrn: string, file: File): Promise<SpreadsheetUploadResponse> {
    const form = new FormData();
    form.append('file', file);
    const res = await this.http.fetch(
      `/depositions/${encodeURIComponent(depositionSrn)}/spreadsheet`,
      { method: 'POST', body: form },
    );
    if (!res.ok) throw await res.json();
    return res.json();
  }

  /** Upload a data file to a deposition. */
  async uploadFile(depositionSrn: string, file: File): Promise<FileUploadResponse> {
    const form = new FormData();
    form.append('file', file);
    const res = await this.http.fetch(
      `/depositions/${encodeURIComponent(depositionSrn)}/files`,
      { method: 'POST', body: form },
    );
    if (!res.ok) throw await res.json();
    return res.json();
  }

  /** Delete a file from a deposition. */
  async deleteFile(depositionSrn: string, filename: string): Promise<void> {
    const res = await this.http.fetch(
      `/depositions/${encodeURIComponent(depositionSrn)}/files/${encodeURIComponent(filename)}`,
      { method: 'DELETE' },
    );
    if (!res.ok) throw await res.json();
  }

  /** Submit a deposition for validation. */
  async submit(depositionSrn: string): Promise<void> {
    const res = await this.http.fetch(
      `/depositions/${encodeURIComponent(depositionSrn)}/submit`,
      { method: 'POST' },
    );
    if (!res.ok) throw await res.json();
  }
}
