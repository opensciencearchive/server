/** Interactive records/features table with server-side sort and cursor paging. */

import { Fragment, useState } from "react";

import { fetchPageArgs } from "../../lib/args";
import { alignmentClass, formatCell } from "../../lib/format";
import type { ColumnSpec, SortSpec, TablePage } from "../../lib/types";
import type { WidgetHostApi } from "../../protocol/host";

interface DataTableProps {
  initial: TablePage;
  host: WidgetHostApi;
}

function rowSrn(row: Record<string, unknown>, index: number): string {
  const candidate = row["srn"] ?? row["record_srn"] ?? row["id"];
  return typeof candidate === "string" ? candidate : `row ${index + 1}`;
}

function headerLabel(column: ColumnSpec): string {
  return column.unit ? `${column.name} (${column.unit})` : column.name;
}

export function DataTable({ initial, host }: DataTableProps) {
  const [page, setPage] = useState(initial);
  // Cursor of the page currently shown (undefined = first page) plus the
  // trail of cursors behind it, so Prev can walk back without server help.
  const [cursor, setCursor] = useState<string | undefined>(undefined);
  const [cursorStack, setCursorStack] = useState<(string | undefined)[]>([]);
  const [expanded, setExpanded] = useState<number | null>(null);
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState<string | null>(null);

  const activeSort: SortSpec | undefined = page.query.sort[0];

  async function load(
    nextCursor: string | undefined,
    sortOverride?: SortSpec[],
  ): Promise<boolean> {
    setBusy(true);
    setError(null);
    try {
      const next = await host.callTool<TablePage>(
        "fetch_page",
        fetchPageArgs(page.query, nextCursor, sortOverride),
      );
      setPage(next);
      setExpanded(null);
      return true;
    } catch (err) {
      setError(err instanceof Error ? err.message : String(err));
      return false;
    } finally {
      setBusy(false);
    }
  }

  async function onSort(column: string) {
    const direction =
      activeSort?.column === column && activeSort.direction === "asc" ? "desc" : "asc";
    if (await load(undefined, [{ column, direction }])) {
      setCursor(undefined);
      setCursorStack([]);
    }
  }

  async function onNext() {
    const target = page.next_cursor;
    if (target === undefined) return;
    const previous = cursor;
    if (await load(target)) {
      setCursorStack([...cursorStack, previous]);
      setCursor(target);
    }
  }

  async function onPrev() {
    if (cursorStack.length === 0) return;
    const target = cursorStack[cursorStack.length - 1];
    if (await load(target)) {
      setCursorStack(cursorStack.slice(0, -1));
      setCursor(target);
    }
  }

  function onRowClick(index: number, row: Record<string, unknown>) {
    if (expanded === index) {
      setExpanded(null);
      return;
    }
    setExpanded(index);
    host.updateModelContext(`user opened record ${rowSrn(row, index)}`);
  }

  if (page.rows.length === 0) {
    return <div className="widget-status">No rows match.</div>;
  }

  return (
    <div>
      {error !== null && <div className="notice notice-warning">{error}</div>}
      <table className="osa-table">
        <thead>
          <tr>
            {page.columns.map((column) => (
              <th key={column.name} className={alignmentClass(column.type)}>
                <button
                  type="button"
                  onClick={() => void onSort(column.name)}
                  disabled={busy}
                  title={column.description}
                >
                  {headerLabel(column)}
                  {activeSort?.column === column.name
                    ? activeSort.direction === "asc"
                      ? " ▲"
                      : " ▼"
                    : ""}
                </button>
              </th>
            ))}
          </tr>
        </thead>
        <tbody>
          {page.rows.map((row, index) => (
            <Fragment key={index}>
              <tr className="clickable" onClick={() => onRowClick(index, row)}>
                {page.columns.map((column) => (
                  <td key={column.name} className={alignmentClass(column.type)}>
                    {column.type === "url" && typeof row[column.name] === "string" ? (
                      <a href={row[column.name] as string} target="_blank" rel="noreferrer">
                        {formatCell(row[column.name], column.type)}
                      </a>
                    ) : (
                      formatCell(row[column.name], column.type)
                    )}
                  </td>
                ))}
              </tr>
              {expanded === index && (
                <tr className="expanded-row">
                  <td colSpan={page.columns.length}>
                    <dl className="expanded-grid">
                      {page.columns.map((column) => (
                        <Fragment key={column.name}>
                          <dt>{headerLabel(column)}</dt>
                          <dd>{formatCell(row[column.name], column.type, column.unit)}</dd>
                        </Fragment>
                      ))}
                    </dl>
                  </td>
                </tr>
              )}
            </Fragment>
          ))}
        </tbody>
      </table>
      <div className="table-footer">
        <button
          type="button"
          className="osa-button"
          onClick={() => void onPrev()}
          disabled={busy || cursorStack.length === 0}
        >
          Prev
        </button>
        <button
          type="button"
          className="osa-button"
          onClick={() => void onNext()}
          disabled={busy || page.next_cursor === undefined}
        >
          Next
        </button>
        <span>{page.rows.length} rows</span>
        {page.truncated && page.next_cursor === undefined && (
          <span className="notice notice-warning">
            showing first {page.rows.length} rows (truncated)
          </span>
        )}
      </div>
    </div>
  );
}
