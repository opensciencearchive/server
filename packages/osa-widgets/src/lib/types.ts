/**
 * TypeScript mirrors of the OSA server payloads consumed by widgets.
 *
 * The server serializes with exclude_none, so optional fields may be ABSENT
 * from the wire JSON (never null). Model them as optional properties.
 */

export type FieldType = "text" | "number" | "date" | "boolean" | "term" | "url";

export interface ColumnSpec {
  name: string;
  type: FieldType;
  format?: string;
  description?: string;
  unit?: string;
}

export interface SortSpec {
  column: string;
  direction: "asc" | "desc";
}

export type FilterOp =
  | "eq"
  | "neq"
  | "gt"
  | "gte"
  | "lt"
  | "lte"
  | "in"
  | "contains"
  | "is_null";

export type FilterValue = string | number | boolean | string[] | number[] | null;

/** Wire form; `field` is a dotted path string when emitted by widgets. */
export type FilterExpr =
  | { kind: "predicate"; field: string; op: FilterOp; value?: FilterValue }
  | { kind: "and"; operands: FilterExpr[] }
  | { kind: "or"; operands: FilterExpr[] }
  | { kind: "not"; operand: FilterExpr };

export interface TableQuery {
  schema: string;
  table: string;
  filter?: FilterExpr;
  sort: SortSpec[];
  limit: number;
}

export interface TablePage {
  query: TableQuery;
  columns: ColumnSpec[];
  rows: Record<string, unknown>[];
  next_cursor?: string;
  truncated: boolean;
}

export interface ChartData {
  kind: "line" | "scatter" | "bar";
  x: string;
  y: string;
  series?: string;
  page: TablePage;
}

export interface DatasetSummary {
  id: string;
  version: string;
  srn: string;
  title: string;
  record_count: number;
  feature_tables: string[];
}

export interface DatasetList {
  node_domain: string;
  datasets: DatasetSummary[];
}

export interface RecordDetailData {
  record: {
    id: string;
    srn: string;
    schema_id: string;
    version: number;
    metadata: Record<string, unknown>;
    created_at: string;
  };
  schema: string;
  feature_tables: string[];
}

export interface Facet {
  field: string;
  label: string;
  kind: "range" | "select" | "text";
  type: FieldType;
  description?: string;
  unit?: string;
}

export interface FilterPanelData {
  schema: string;
  table: string;
  facets: Facet[];
}

export interface ColumnSample {
  column: string;
  values: (string | number | boolean)[];
  truncated: boolean;
}
