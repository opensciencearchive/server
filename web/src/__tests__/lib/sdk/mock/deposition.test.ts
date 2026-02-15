import { describe, it, expect, beforeEach } from 'vitest';
import { MockDepositionNamespace } from '@/lib/sdk/mock/deposition';
import type { DepositionInterface } from '@/lib/sdk/deposition';

describe('MockDepositionNamespace', () => {
  let api: DepositionInterface;

  beforeEach(() => {
    api = new MockDepositionNamespace();
  });

  describe('listConventions', () => {
    it('returns conventions with expected shape', async () => {
      const res = await api.listConventions();
      expect(res.items.length).toBeGreaterThan(0);
      for (const conv of res.items) {
        expect(conv).toHaveProperty('srn');
        expect(conv).toHaveProperty('title');
        expect(conv).toHaveProperty('description');
        expect(conv).toHaveProperty('schema_srn');
        expect(conv).toHaveProperty('created_at');
        expect(conv.srn).toMatch(/^urn:osa:/);
      }
    });
  });

  describe('getConvention', () => {
    it('returns detail with file requirements for a known SRN', async () => {
      const list = await api.listConventions();
      const srn = list.items[0].srn;
      const res = await api.getConvention(srn);
      expect(res.srn).toBe(srn);
      expect(res.file_requirements).toHaveProperty('accepted_types');
      expect(res.file_requirements).toHaveProperty('min_count');
      expect(res.file_requirements).toHaveProperty('max_count');
      expect(res.file_requirements).toHaveProperty('max_file_size');
      expect(res.validator_refs.length).toBeGreaterThan(0);
    });

    it('throws for unknown SRN', async () => {
      await expect(api.getConvention('urn:osa:localhost:conv:nonexistent')).rejects.toThrow();
    });
  });

  describe('create', () => {
    it('returns a response with a valid SRN', async () => {
      const list = await api.listConventions();
      const convSrn = list.items[0].srn;
      const res = await api.create(convSrn);
      expect(res.srn).toMatch(/^urn:osa:localhost:dep:/);
    });
  });

  describe('get', () => {
    it('returns the created deposition', async () => {
      const list = await api.listConventions();
      const convSrn = list.items[0].srn;
      const { srn } = await api.create(convSrn);
      const dep = await api.get(srn);
      expect(dep.srn).toBe(srn);
      expect(dep.convention_srn).toBe(convSrn);
      expect(dep.status).toBe('draft');
      expect(dep.files).toEqual([]);
      expect(dep.metadata).toEqual({});
      expect(dep.record_srn).toBeNull();
    });
  });

  describe('uploadSpreadsheet', () => {
    it('returns parse result with metadata and warnings', async () => {
      const list = await api.listConventions();
      const { srn } = await api.create(list.items[0].srn);
      const file = new File(['data'], 'metadata.xlsx', { type: 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet' });
      const res = await api.uploadSpreadsheet(srn, file);
      expect(res.parse_result).toHaveProperty('metadata');
      expect(res.parse_result).toHaveProperty('warnings');
      expect(res.parse_result).toHaveProperty('errors');
      expect(typeof res.parse_result.metadata).toBe('object');
      expect(Array.isArray(res.parse_result.errors)).toBe(true);
    });
  });

  describe('uploadFile', () => {
    it('returns file metadata with name and size', async () => {
      const list = await api.listConventions();
      const { srn } = await api.create(list.items[0].srn);
      const file = new File(['content'], 'sample.fastq', { type: 'application/octet-stream' });
      const res = await api.uploadFile(srn, file);
      expect(res.file.name).toBe('sample.fastq');
      expect(res.file.size).toBeGreaterThan(0);
      expect(res.file.checksum).toBeTruthy();
      expect(res.file).toHaveProperty('content_type');
      expect(res.file.uploaded_at).toBeTruthy();
    });
  });

  describe('deleteFile', () => {
    it('succeeds without error', async () => {
      const list = await api.listConventions();
      const { srn } = await api.create(list.items[0].srn);
      await expect(api.deleteFile(srn, 'sample.fastq')).resolves.toBeUndefined();
    });
  });

  describe('submit', () => {
    it('succeeds without error', async () => {
      const list = await api.listConventions();
      const { srn } = await api.create(list.items[0].srn);
      await expect(api.submit(srn)).resolves.toBeUndefined();
    });
  });

  describe('downloadTemplate', () => {
    it('returns a Blob', async () => {
      const list = await api.listConventions();
      const blob = await api.downloadTemplate(list.items[0].srn);
      expect(blob).toBeInstanceOf(Blob);
      expect(blob.size).toBeGreaterThan(0);
    });
  });
});
