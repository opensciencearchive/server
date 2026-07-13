/** Single-record card with metadata grid and lazily loaded feature sections. */

import { Fragment, useState } from "react";

import { fetchPageArgs } from "../../lib/args";
import { formatCell } from "../../lib/format";
import type { RecordDetailData, TableQuery, TablePage } from "../../lib/types";
import type { WidgetHostApi } from "../../protocol/host";

interface RecordDetailProps {
  data: RecordDetailData;
  host: WidgetHostApi;
}

type SectionState =
  | { status: "idle" }
  | { status: "loading" }
  | { status: "error"; message: string }
  | { status: "loaded"; page: TablePage };

const FEATURE_ROW_LIMIT = 50;

/** Feature rows link back to their producing hook run — surface it as "run". */
function columnTitle(name: string): string {
  return name === "run_id" ? "run" : name;
}

export function RecordDetail({ data, host }: RecordDetailProps) {
  const [open, setOpen] = useState<Record<string, boolean>>({});
  const [sections, setSections] = useState<Record<string, SectionState>>({});

  async function loadSection(feature: string) {
    setSections((prev) => ({ ...prev, [feature]: { status: "loading" } }));
    try {
      const query: TableQuery = {
        schema: data.schema,
        table: feature,
        filter: {
          kind: "predicate",
          field: `features.${feature}.record_srn`,
          op: "eq",
          value: data.record.srn,
        },
        sort: [],
        limit: FEATURE_ROW_LIMIT,
      };
      const page = await host.callTool<TablePage>("fetch_page", fetchPageArgs(query));
      setSections((prev) => ({ ...prev, [feature]: { status: "loaded", page } }));
    } catch (err) {
      setSections((prev) => ({
        ...prev,
        [feature]: {
          status: "error",
          message: err instanceof Error ? err.message : String(err),
        },
      }));
    }
  }

  function toggle(feature: string) {
    const isOpen = open[feature] === true;
    setOpen((prev) => ({ ...prev, [feature]: !isOpen }));
    if (!isOpen && (sections[feature] ?? { status: "idle" }).status === "idle") {
      void loadSection(feature);
    }
  }

  const { record } = data;
  const metadataEntries = Object.entries(record.metadata);

  return (
    <div>
      <div className="card">
        <h2>{record.id}</h2>
        <div className="muted">{record.srn}</div>
        <dl className="kv-grid">
          <dt>schema</dt>
          <dd>{record.schema_id}</dd>
          <dt>version</dt>
          <dd>{record.version}</dd>
          <dt>created</dt>
          <dd>{formatCell(record.created_at, "date")}</dd>
        </dl>
      </div>

      <div className="card">
        <h2>Metadata</h2>
        {metadataEntries.length === 0 ? (
          <div className="muted">No metadata.</div>
        ) : (
          <dl className="kv-grid">
            {metadataEntries.map(([key, value]) => (
              <Fragment key={key}>
                <dt>{key}</dt>
                <dd>{value === null || value === undefined ? "—" : String(value)}</dd>
              </Fragment>
            ))}
          </dl>
        )}
      </div>

      {data.feature_tables.map((feature) => {
        const state = sections[feature] ?? { status: "idle" };
        return (
          <div className="section" key={feature}>
            <button
              type="button"
              className="section-toggle"
              aria-expanded={open[feature] === true}
              onClick={() => toggle(feature)}
            >
              {open[feature] === true ? "▾" : "▸"} {feature}
            </button>
            {open[feature] === true && (
              <div className="section-body">
                {state.status === "loading" && (
                  <div className="widget-status">Loading {feature}…</div>
                )}
                {state.status === "error" && (
                  <div className="widget-status widget-error">
                    Failed to load {feature}: {state.message}
                  </div>
                )}
                {state.status === "loaded" && <FeatureRows page={state.page} />}
              </div>
            )}
          </div>
        );
      })}
    </div>
  );
}

function FeatureRows({ page }: { page: TablePage }) {
  if (page.rows.length === 0) {
    return <div className="muted">No feature rows for this record.</div>;
  }
  return (
    <table className="osa-table">
      <thead>
        <tr>
          {page.columns.map((column) => (
            <th key={column.name}>{columnTitle(column.name)}</th>
          ))}
        </tr>
      </thead>
      <tbody>
        {page.rows.map((row, index) => (
          <tr key={index}>
            {page.columns.map((column) => (
              <td key={column.name}>{formatCell(row[column.name], column.type)}</td>
            ))}
          </tr>
        ))}
      </tbody>
    </table>
  );
}
