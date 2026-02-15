/**
 * Mock deposition namespace for development.
 * Stateful within a session: tracks created depositions and uploaded files.
 */

import type { DepositionInterface } from '../deposition';
import type {
  ConventionListResponse,
  ConventionDetail,
  CreateDepositionResponse,
  Deposition,
  SpreadsheetUploadResponse,
  FileUploadResponse,
  DepositionFile,
} from '@/types';
import { MOCK_CONVENTIONS } from './deposition-data';

function delay(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

let depositionCounter = 0;

function generateDepositionSrn(): string {
  depositionCounter++;
  const id = String(depositionCounter).padStart(26, '0');
  return `urn:osa:localhost:dep:${id}`;
}

function simpleChecksum(name: string, size: number): string {
  const raw = `${name}:${size}:${Date.now()}`;
  let hash = 0;
  for (let i = 0; i < raw.length; i++) {
    hash = ((hash << 5) - hash + raw.charCodeAt(i)) | 0;
  }
  return `sha256:${Math.abs(hash).toString(16).padStart(16, '0')}`;
}

export class MockDepositionNamespace implements DepositionInterface {
  private depositions = new Map<string, Deposition>();

  async listConventions(): Promise<ConventionListResponse> {
    await delay(200 + Math.random() * 300);
    return {
      items: MOCK_CONVENTIONS.map((c) => ({
        srn: c.srn,
        title: c.title,
        description: c.description,
        schema_srn: c.schema_srn,
        created_at: c.created_at,
      })),
    };
  }

  async getConvention(srn: string): Promise<ConventionDetail> {
    await delay(150 + Math.random() * 200);
    const conv = MOCK_CONVENTIONS.find((c) => c.srn === srn);
    if (!conv) {
      throw new Error(`Convention not found: ${srn}`);
    }
    return conv;
  }

  async create(conventionSrn: string): Promise<CreateDepositionResponse> {
    await delay(200 + Math.random() * 300);
    const now = new Date().toISOString();
    const srn = generateDepositionSrn();
    const deposition: Deposition = {
      srn,
      convention_srn: conventionSrn,
      status: 'draft',
      metadata: {},
      files: [],
      record_srn: null,
      created_at: now,
      updated_at: now,
    };
    this.depositions.set(srn, deposition);
    return { srn };
  }

  async get(srn: string): Promise<Deposition> {
    await delay(150 + Math.random() * 200);
    const dep = this.depositions.get(srn);
    if (!dep) {
      throw new Error(`Deposition not found: ${srn}`);
    }
    return dep;
  }

  async downloadTemplate(conventionSrn: string): Promise<Blob> {
    await delay(300 + Math.random() * 400);
    const conv = MOCK_CONVENTIONS.find((c) => c.srn === conventionSrn);
    if (!conv) {
      throw new Error(`Convention not found: ${conventionSrn}`);
    }
    const content = `template for ${conv.title}\n`;
    return new Blob([content], {
      type: 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
    });
  }

  async uploadSpreadsheet(depositionSrn: string, file: File): Promise<SpreadsheetUploadResponse> {
    await delay(400 + Math.random() * 500);
    const dep = this.depositions.get(depositionSrn);
    if (dep) {
      dep.updated_at = new Date().toISOString();
    }
    return {
      parse_result: {
        metadata: { title: 'Sample 1', organism: 'Homo sapiens' },
        warnings: [`Column "batch_id" in ${file.name} is not in the schema — it will be ignored`],
        errors: [],
      },
    };
  }

  async uploadFile(depositionSrn: string, file: File): Promise<FileUploadResponse> {
    await delay(200 + Math.random() * 300);
    const dep = this.depositions.get(depositionSrn);
    const entry: DepositionFile = {
      name: file.name,
      size: file.size,
      checksum: simpleChecksum(file.name, file.size),
      content_type: file.type || null,
      uploaded_at: new Date().toISOString(),
    };
    if (dep) {
      dep.files.push(entry);
      dep.updated_at = new Date().toISOString();
    }
    return { file: entry };
  }

  async deleteFile(depositionSrn: string, filename: string): Promise<void> {
    await delay(150 + Math.random() * 200);
    const dep = this.depositions.get(depositionSrn);
    if (dep) {
      dep.files = dep.files.filter((f) => f.name !== filename);
      dep.updated_at = new Date().toISOString();
    }
  }

  async submit(depositionSrn: string): Promise<void> {
    await delay(300 + Math.random() * 400);
    const dep = this.depositions.get(depositionSrn);
    if (dep) {
      dep.status = 'in_validation';
      dep.updated_at = new Date().toISOString();
    }
  }
}
