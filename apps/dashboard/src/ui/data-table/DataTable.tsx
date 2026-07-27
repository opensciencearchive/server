import styles from "./DataTable.module.css";

export interface Column<Row> {
  key: string;
  header: React.ReactNode;
  align?: "left" | "right";
  render: (row: Row) => React.ReactNode;
}

export interface DataTableProps<Row> {
  columns: Array<Column<Row>>;
  rows: Row[];
  rowKey: (row: Row) => string;
  /** Tint a row (e.g. the failing validation check). */
  rowTone?: (row: Row) => "warning" | "danger" | undefined;
  caption?: string;
}

export function DataTable<Row>({
  columns,
  rows,
  rowKey,
  rowTone,
  caption,
}: DataTableProps<Row>) {
  return (
    <table className={styles.table}>
      {caption && <caption className={styles.caption}>{caption}</caption>}
      <thead>
        <tr>
          {columns.map((column) => (
            <th
              key={column.key}
              className={column.align === "right" ? styles.right : undefined}
            >
              {column.header}
            </th>
          ))}
        </tr>
      </thead>
      <tbody>
        {rows.map((row) => {
          const tone = rowTone?.(row);
          return (
            <tr
              key={rowKey(row)}
              className={tone ? styles[`tone-${tone}`] : undefined}
            >
              {columns.map((column) => (
                <td
                  key={column.key}
                  className={column.align === "right" ? styles.right : undefined}
                >
                  {column.render(row)}
                </td>
              ))}
            </tr>
          );
        })}
      </tbody>
    </table>
  );
}
