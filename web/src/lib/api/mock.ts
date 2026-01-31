/**
 * MockAPI Implementation
 * In-memory implementation with realistic sample data for development.
 */

import type { ApiInterface } from './interface';
import type {
  SearchOptions,
  IndexListResponse,
  SearchResponse,
  RecordResponse,
  RecordMetadata,
} from '@/types';
import { DEFAULT_INDEX, DEFAULT_LIMIT } from '@/lib/utils/constants';

// Sample records representing biological datasets
const MOCK_RECORDS: Array<{ srn: string; metadata: RecordMetadata }> = [
  {
    srn: 'urn:osa:localhost:dep:a1b2c3d4-e5f6-7890-abcd-ef1234567890',
    metadata: {
      title: "Alzheimer's disease neuroinflammatory risk genes expression profiling in human brain tissue",
      summary: "Genome-wide expression analysis of postmortem brain samples from Alzheimer's disease patients and age-matched controls. This study identifies novel neuroinflammatory pathways and risk genes associated with disease progression.",
      organism: 'Homo sapiens',
      sample_count: '189',
      pub_date: '2025/11/15',
      platform: 'Illumina HiSeq 4000',
      gds_type: 'Expression profiling by high throughput sequencing',
      entry_type: 'Series',
    },
  },
  {
    srn: 'urn:osa:localhost:dep:b2c3d4e5-f6a7-8901-bcde-f23456789012',
    metadata: {
      title: 'Single-cell RNA sequencing of mouse brain development atlas',
      summary: 'Comprehensive single-cell transcriptomic atlas of mouse brain development from embryonic day 10 to postnatal day 30. Over 500,000 cells profiled across major brain regions.',
      organism: 'Mus musculus',
      sample_count: '48',
      pub_date: '2025/09/22',
      platform: '10x Genomics Chromium',
      gds_type: 'Single cell analysis',
      entry_type: 'Series',
    },
  },
  {
    srn: 'urn:osa:localhost:dep:c3d4e5f6-a7b8-9012-cdef-345678901234',
    metadata: {
      title: 'Cancer drug resistance mechanisms in triple-negative breast cancer organoids',
      summary: 'Multi-omics profiling of patient-derived organoids treated with chemotherapy agents. Identifies molecular signatures predictive of drug resistance and potential therapeutic targets.',
      organism: 'Homo sapiens',
      sample_count: '324',
      pub_date: '2025/12/01',
      platform: 'Illumina NovaSeq 6000',
      gds_type: 'Expression profiling by high throughput sequencing',
      entry_type: 'Series',
    },
  },
  {
    srn: 'urn:osa:localhost:dep:d4e5f6a7-b8c9-0123-defa-456789012345',
    metadata: {
      title: 'CRISPR screen identifies novel T cell exhaustion regulators',
      summary: 'Genome-wide CRISPR knockout screen in human T cells to identify genes regulating T cell exhaustion. Discovers new targets for cancer immunotherapy enhancement.',
      organism: 'Homo sapiens',
      sample_count: '96',
      pub_date: '2025/10/08',
      platform: 'Illumina NextSeq 2000',
      gds_type: 'CRISPR/Cas9 screen',
      entry_type: 'Series',
    },
  },
  {
    srn: 'urn:osa:localhost:dep:e5f6a7b8-c9d0-1234-efab-567890123456',
    metadata: {
      title: 'Gut microbiome changes during inflammatory bowel disease flares',
      summary: 'Longitudinal metagenomics study tracking gut microbiome composition in IBD patients through disease flares and remission periods. Identifies microbial signatures predictive of flare onset.',
      organism: 'human gut metagenome',
      sample_count: '512',
      pub_date: '2025/08/14',
      platform: 'Illumina MiSeq',
      gds_type: 'Metagenomics',
      entry_type: 'Series',
    },
  },
  {
    srn: 'urn:osa:localhost:dep:f6a7b8c9-d0e1-2345-fabc-678901234567',
    metadata: {
      title: 'Spatial transcriptomics of human liver tumor microenvironment',
      summary: 'High-resolution spatial gene expression mapping of hepatocellular carcinoma samples revealing immune cell infiltration patterns and tumor-stroma interactions.',
      organism: 'Homo sapiens',
      sample_count: '24',
      pub_date: '2025/07/30',
      platform: '10x Genomics Visium',
      gds_type: 'Spatial transcriptomics',
      entry_type: 'Series',
    },
  },
  {
    srn: 'urn:osa:localhost:dep:a7b8c9d0-e1f2-3456-abcd-789012345678',
    metadata: {
      title: 'Zebrafish heart regeneration transcriptome time course',
      summary: 'Time-series RNA-seq analysis of zebrafish heart regeneration following ventricular resection. Captures gene expression changes from injury through complete regeneration.',
      organism: 'Danio rerio',
      sample_count: '72',
      pub_date: '2025/06/18',
      platform: 'Illumina HiSeq 2500',
      gds_type: 'Expression profiling by high throughput sequencing',
      entry_type: 'Series',
    },
  },
  {
    srn: 'urn:osa:localhost:dep:b8c9d0e1-f2a3-4567-bcde-890123456789',
    metadata: {
      title: 'Aging-associated epigenetic changes in human blood cells',
      summary: 'Comprehensive DNA methylation profiling across age groups to identify epigenetic clock markers and age-associated regulatory changes in peripheral blood mononuclear cells.',
      organism: 'Homo sapiens',
      sample_count: '856',
      pub_date: '2025/05/25',
      platform: 'Illumina EPIC BeadChip',
      gds_type: 'Methylation profiling by array',
      entry_type: 'Series',
    },
  },
  {
    srn: 'urn:osa:localhost:dep:c9d0e1f2-a3b4-5678-cdef-901234567890',
    metadata: {
      title: "Parkinson's disease patient iPSC-derived dopaminergic neurons",
      summary: "Transcriptomic and proteomic characterization of iPSC-derived midbrain dopaminergic neurons from Parkinson's disease patients with LRRK2 mutations compared to isogenic controls.",
      organism: 'Homo sapiens',
      sample_count: '36',
      pub_date: '2025/04/12',
      platform: 'Illumina NovaSeq 6000',
      gds_type: 'Expression profiling by high throughput sequencing',
      entry_type: 'Series',
    },
  },
  {
    srn: 'urn:osa:localhost:dep:d0e1f2a3-b4c5-6789-defa-012345678901',
    metadata: {
      title: 'COVID-19 severity associated immune cell signatures',
      summary: 'Single-cell immune profiling of peripheral blood from COVID-19 patients across disease severity spectrum. Identifies cell populations and gene signatures associated with severe outcomes.',
      organism: 'Homo sapiens',
      sample_count: '156',
      pub_date: '2025/03/20',
      platform: '10x Genomics Chromium',
      gds_type: 'Single cell analysis',
      entry_type: 'Series',
    },
  },
  {
    srn: 'urn:osa:localhost:dep:e1f2a3b4-c5d6-7890-efab-123456789012',
    metadata: {
      title: 'Plant drought stress response in Arabidopsis root tips',
      summary: 'Single-cell transcriptomics of Arabidopsis root tips under drought stress conditions. Maps cell-type-specific responses and identifies key regulatory networks.',
      organism: 'Arabidopsis thaliana',
      sample_count: '18',
      pub_date: '2025/02/08',
      platform: '10x Genomics Chromium',
      gds_type: 'Single cell analysis',
      entry_type: 'Series',
    },
  },
  {
    srn: 'urn:osa:localhost:dep:f2a3b4c5-d6e7-8901-fabc-234567890123',
    metadata: {
      title: 'Autism spectrum disorder cortical organoid transcriptomes',
      summary: 'Comparison of cerebral organoids derived from ASD patients and controls, profiling gene expression during cortical development to identify neurodevelopmental alterations.',
      organism: 'Homo sapiens',
      sample_count: '64',
      pub_date: '2025/01/15',
      platform: 'Illumina NovaSeq 6000',
      gds_type: 'Expression profiling by high throughput sequencing',
      entry_type: 'Series',
    },
  },
];

/**
 * Calculate a mock relevance score based on query term matching.
 */
function calculateMockScore(query: string, metadata: RecordMetadata): number {
  const queryTerms = query.toLowerCase().split(/\s+/);
  const searchText = [
    metadata.title,
    metadata.summary,
    metadata.organism,
    metadata.gds_type,
  ]
    .filter(Boolean)
    .join(' ')
    .toLowerCase();

  let matches = 0;
  for (const term of queryTerms) {
    if (searchText.includes(term)) {
      matches++;
    }
  }

  // Base score + bonus for matches
  const matchRatio = matches / queryTerms.length;
  return Math.min(0.95, 0.3 + matchRatio * 0.6 + Math.random() * 0.1);
}

/**
 * MockAPI - In-memory implementation for development.
 */
export class MockAPI implements ApiInterface {
  private records = MOCK_RECORDS;

  async listIndexes(): Promise<IndexListResponse> {
    // Simulate network delay
    await this.delay(100);
    return { indexes: ['vector'] };
  }

  async search(
    query: string,
    indexName: string = DEFAULT_INDEX,
    options: SearchOptions = {}
  ): Promise<SearchResponse> {
    const { offset = 0, limit = DEFAULT_LIMIT } = options;

    // Simulate network delay
    await this.delay(300 + Math.random() * 200);

    // Score and sort records
    const scoredResults = this.records
      .map((record) => ({
        srn: record.srn,
        score: calculateMockScore(query, record.metadata),
        metadata: record.metadata,
      }))
      .sort((a, b) => b.score - a.score);

    // Apply pagination
    const paginatedResults = scoredResults.slice(offset, offset + limit);
    const hasMore = offset + limit < scoredResults.length;

    return {
      query,
      index: indexName,
      total: scoredResults.length,
      has_more: hasMore,
      results: paginatedResults,
    };
  }

  async getRecord(srn: string): Promise<RecordResponse> {
    // Simulate network delay
    await this.delay(200);

    const record = this.records.find((r) => r.srn === srn);

    if (!record) {
      throw { detail: `Record not found: ${srn}` };
    }

    return { record };
  }

  private delay(ms: number): Promise<void> {
    return new Promise((resolve) => setTimeout(resolve, ms));
  }
}
