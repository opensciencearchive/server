/**
 * Convention types matching backend DTOs.
 */

export interface Convention {
  srn: string;
  title: string;
  description: string | null;
  schema_srn: string;
  created_at: string;
}

export interface ConventionDetail extends Convention {
  file_requirements: FileRequirements;
  validator_refs: ValidatorRef[];
}

export interface FileRequirements {
  accepted_types: string[];
  min_count: number;
  max_count: number;
  max_file_size: number;
}

export interface ValidatorRef {
  image: string;
  digest: string;
}

export interface ConventionListResponse {
  items: Convention[];
}
