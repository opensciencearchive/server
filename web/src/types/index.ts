/**
 * Type exports for OSA
 */

export type {
  RecordMetadata,
  Record,
  SearchHit,
} from './record';

export type {
  SearchOptions,
  IndexListResponse,
  SearchResponse,
  RecordResponse,
  ApiError,
  ApiResult,
} from './api';

export { isApiError } from './api';

export type {
  Convention,
  ConventionDetail,
  ConventionListResponse,
  FileRequirements,
  ValidatorRef,
} from './convention';

export type {
  Deposition,
  DepositionFile,
  DepositionStatus,
  SpreadsheetError,
  SpreadsheetParseResult,
  CreateDepositionResponse,
  SpreadsheetUploadResponse,
  FileUploadResponse,
} from './deposition';
