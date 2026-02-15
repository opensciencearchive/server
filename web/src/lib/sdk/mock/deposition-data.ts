/**
 * Mock convention data for development.
 */

import type { ConventionDetail } from '@/types';

export const MOCK_CONVENTIONS: ConventionDetail[] = [
  {
    srn: 'urn:osa:localhost:conv:proteomics-ms',
    title: 'Proteomics Mass Spectrometry',
    description:
      'Mass spectrometry-based proteomics data including raw spectra, peptide identifications, and protein quantification tables.',
    schema_srn: 'urn:osa:localhost:schema:proteomics-ms@1.4.0',
    created_at: '2025-01-15T10:00:00Z',
    file_requirements: {
      accepted_types: ['.raw', '.mzML', '.mzXML', '.mgf', '.csv', '.tsv'],
      min_count: 1,
      max_count: 500,
      max_file_size: 10_737_418_240,
    },
    validator_refs: [
      { image: 'ghcr.io/osap/validators/proteomics-ms', digest: 'sha256:abc123' },
    ],
  },
  {
    srn: 'urn:osa:localhost:conv:bulk-rnaseq-timeseries',
    title: 'Bulk RNA-Seq Time Series',
    description:
      'Longitudinal gene expression profiling with time-point annotations, developmental stages, and differential expression across conditions.',
    schema_srn: 'urn:osa:localhost:schema:bulk-rnaseq-timeseries@1.3.0',
    created_at: '2025-02-01T12:00:00Z',
    file_requirements: {
      accepted_types: ['.fastq', '.fastq.gz', '.bam', '.csv', '.tsv'],
      min_count: 1,
      max_count: 1000,
      max_file_size: 53_687_091_200,
    },
    validator_refs: [
      { image: 'ghcr.io/osap/validators/bulk-rnaseq', digest: 'sha256:def456' },
    ],
  },
  {
    srn: 'urn:osa:localhost:conv:single-cell-rnaseq',
    title: 'Single-Cell RNA-Seq',
    description:
      'Single-cell resolution transcriptomics with cell type annotations, UMI counts, and spatial coordinates.',
    schema_srn: 'urn:osa:localhost:schema:single-cell-rnaseq@1.2.0',
    created_at: '2025-03-10T09:00:00Z',
    file_requirements: {
      accepted_types: ['.h5ad', '.h5', '.fastq.gz', '.bam', '.csv', '.mtx', '.tsv'],
      min_count: 1,
      max_count: 200,
      max_file_size: 10_737_418_240,
    },
    validator_refs: [
      { image: 'ghcr.io/osap/validators/scrna-seq', digest: 'sha256:ghi789' },
    ],
  },
];
