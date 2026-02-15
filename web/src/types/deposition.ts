/**
 * Deposition types matching backend DTOs.
 */

export type DepositionStatus =
  | 'draft'
  | 'in_validation'
  | 'in_review'
  | 'accepted'
  | 'rejected';

export interface DepositionFile {
  name: string;
  size: number;
  checksum: string;
  content_type: string | null;
  uploaded_at: string;
}

export interface Deposition {
  srn: string;
  convention_srn: string;
  status: DepositionStatus;
  metadata: Record<string, unknown>;
  files: DepositionFile[];
  record_srn: string | null;
  created_at: string;
  updated_at: string;
}

export interface SpreadsheetError {
  field: string;
  message: string;
}

export interface SpreadsheetParseResult {
  metadata: Record<string, unknown>;
  warnings: string[];
  errors: SpreadsheetError[];
}

export interface CreateDepositionResponse {
  srn: string;
}

export interface SpreadsheetUploadResponse {
  parse_result: SpreadsheetParseResult;
}

export interface FileUploadResponse {
  file: DepositionFile;
}
