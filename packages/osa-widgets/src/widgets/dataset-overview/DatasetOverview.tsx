/** Node-level catalog: one card per published dataset. */

import type { DatasetList, DatasetSummary } from "../../lib/types";
import type { WidgetHostApi } from "../../protocol/host";

interface DatasetOverviewProps {
  data: DatasetList;
  host: WidgetHostApi;
}

export function DatasetOverview({ data, host }: DatasetOverviewProps) {
  function onSelect(dataset: DatasetSummary) {
    host.updateModelContext(
      `user selected dataset ${dataset.id} — show its records table`,
    );
  }

  return (
    <div>
      <div className="muted">Datasets on {data.node_domain}</div>
      {data.datasets.length === 0 && (
        <div className="widget-status">No published datasets on this node yet.</div>
      )}
      {data.datasets.map((dataset) => (
        <div
          key={dataset.srn}
          className="card clickable"
          role="button"
          tabIndex={0}
          onClick={() => onSelect(dataset)}
          onKeyDown={(e) => {
            if (e.key === "Enter" || e.key === " ") onSelect(dataset);
          }}
        >
          <h2>{dataset.title}</h2>
          <div className="muted">
            {dataset.id}@{dataset.version} · {dataset.record_count.toLocaleString()}{" "}
            records
          </div>
          {dataset.feature_tables.length > 0 && (
            <div className="chips">
              {dataset.feature_tables.map((feature) => (
                <span key={feature} className="chip">
                  {feature}
                </span>
              ))}
            </div>
          )}
        </div>
      ))}
    </div>
  );
}
