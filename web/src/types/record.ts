/**
 * Record types for Lingual Bio Search
 * Aligned with OSA Search API v1.0.0
 */

/**
 * Record metadata. Fields vary by source/domain.
 * Common fields are listed; additional fields may be present.
 */
export interface RecordMetadata {
  /** Title of the record */
  title: string;

  /** Detailed description or abstract */
  summary?: string | null;

  /** Species/organism (biology domain) */
  organism?: string | null;

  /** Number of samples (as string from API) */
  sample_count?: string | null;

  /** Publication or release date (format: "YYYY/MM/DD") */
  pub_date?: string | null;

  /** Platform or instrument information */
  platform?: string | null;

  /** Dataset type classification */
  gds_type?: string | null;

  /** Entry type */
  entry_type?: string | null;

  /** Additional dynamic fields */
  [key: string]: unknown;
}

/**
 * A record from the Open Science Archive.
 */
export interface Record {
  /** Structured Resource Name (unique identifier) */
  srn: string;

  /** Record metadata */
  metadata: RecordMetadata;
}

/**
 * A search hit combining a record with its relevance score.
 */
export interface SearchHit {
  /** Structured Resource Name */
  srn: string;

  /** Relevance score (0.0 to 1.0, higher is better) */
  score: number;

  /** Record metadata */
  metadata: RecordMetadata;
}
