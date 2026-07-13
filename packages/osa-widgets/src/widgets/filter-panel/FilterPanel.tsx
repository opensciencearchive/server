/**
 * Facet-driven filter builder. The composed FilterExpr's consumer is the
 * model/host (via updateModelContext), not a live table in this widget.
 */

import { useState } from "react";

import { sampleValuesArgs } from "../../lib/args";
import type {
  ColumnSample,
  Facet,
  FilterExpr,
  FilterPanelData,
  FilterValue,
} from "../../lib/types";
import type { WidgetHostApi } from "../../protocol/host";

interface FilterPanelProps {
  data: FilterPanelData;
  host: WidgetHostApi;
}

interface FacetInput {
  min?: string;
  max?: string;
  selected?: string;
  text?: string;
}

type OptionsState =
  | { status: "idle" }
  | { status: "loading" }
  | { status: "error"; message: string }
  | { status: "loaded"; values: string[] };

function coerceValue(raw: string, facet: Facet): FilterValue {
  if (facet.type === "number") return Number(raw);
  if (facet.type === "boolean") return raw === "true";
  return raw; // dates travel as ISO strings
}

interface ComposedFilter {
  expr: FilterExpr;
  summary: string;
}

export function composeFilter(
  facets: Facet[],
  inputs: Record<string, FacetInput>,
): ComposedFilter | null {
  const predicates: FilterExpr[] = [];
  const parts: string[] = [];

  for (const facet of facets) {
    const input = inputs[facet.field] ?? {};
    if (facet.kind === "range") {
      if (input.min !== undefined && input.min !== "") {
        predicates.push({
          kind: "predicate",
          field: facet.field,
          op: "gte",
          value: coerceValue(input.min, facet),
        });
        parts.push(`${facet.label} ≥ ${input.min}`);
      }
      if (input.max !== undefined && input.max !== "") {
        predicates.push({
          kind: "predicate",
          field: facet.field,
          op: "lte",
          value: coerceValue(input.max, facet),
        });
        parts.push(`${facet.label} ≤ ${input.max}`);
      }
    } else if (facet.kind === "select") {
      if (input.selected !== undefined && input.selected !== "") {
        predicates.push({
          kind: "predicate",
          field: facet.field,
          op: "eq",
          value: coerceValue(input.selected, facet),
        });
        parts.push(`${facet.label} = ${input.selected}`);
      }
    } else if (input.text !== undefined && input.text !== "") {
      predicates.push({
        kind: "predicate",
        field: facet.field,
        op: "contains",
        value: input.text,
      });
      parts.push(`${facet.label} contains "${input.text}"`);
    }
  }

  if (predicates.length === 0) return null;
  const expr: FilterExpr =
    predicates.length === 1 ? predicates[0]! : { kind: "and", operands: predicates };
  return { expr, summary: parts.join(", ") };
}

export function FilterPanel({ data, host }: FilterPanelProps) {
  const [inputs, setInputs] = useState<Record<string, FacetInput>>({});
  const [options, setOptions] = useState<Record<string, OptionsState>>({});
  const [composed, setComposed] = useState<FilterExpr | null>(null);

  function updateInput(field: string, patch: FacetInput) {
    setInputs((prev) => ({ ...prev, [field]: { ...prev[field], ...patch } }));
  }

  // Term options are sampled lazily on first focus so panels with many term
  // facets do not fan out tool calls at mount.
  async function loadOptions(facet: Facet) {
    if ((options[facet.field] ?? { status: "idle" }).status !== "idle") return;
    setOptions((prev) => ({ ...prev, [facet.field]: { status: "loading" } }));
    try {
      const sample = await host.callTool<ColumnSample>(
        "sample_values",
        sampleValuesArgs(data.schema, data.table, facet.label),
      );
      setOptions((prev) => ({
        ...prev,
        [facet.field]: { status: "loaded", values: sample.values.map(String) },
      }));
    } catch (err) {
      setOptions((prev) => ({
        ...prev,
        [facet.field]: {
          status: "error",
          message: err instanceof Error ? err.message : String(err),
        },
      }));
    }
  }

  function onApply() {
    const result = composeFilter(data.facets, inputs);
    if (result === null) {
      setComposed(null);
      host.updateModelContext("user cleared all filters");
      return;
    }
    setComposed(result.expr);
    host.updateModelContext(`user set filter: ${result.summary}`);
  }

  return (
    <div>
      <div className="muted">
        Filter {data.schema} / {data.table}
      </div>
      {data.facets.map((facet) => (
        <FacetControl
          key={facet.field}
          facet={facet}
          input={inputs[facet.field] ?? {}}
          options={options[facet.field] ?? { status: "idle" }}
          onChange={(patch) => updateInput(facet.field, patch)}
          onNeedOptions={() => void loadOptions(facet)}
        />
      ))}
      <button type="button" className="osa-button primary" onClick={onApply}>
        Apply
      </button>
      {composed !== null && (
        <details className="filter-json" open>
          <summary>Filter JSON</summary>
          <pre data-testid="filter-json">{JSON.stringify(composed, null, 2)}</pre>
        </details>
      )}
    </div>
  );
}

function FacetControl({
  facet,
  input,
  options,
  onChange,
  onNeedOptions,
}: {
  facet: Facet;
  input: FacetInput;
  options: OptionsState;
  onChange: (patch: FacetInput) => void;
  onNeedOptions: () => void;
}) {
  const label = (
    <label htmlFor={`facet-${facet.field}`}>
      {facet.label}
      {facet.unit !== undefined && <span className="hint"> ({facet.unit})</span>}
      {facet.description !== undefined && (
        <span className="hint"> — {facet.description}</span>
      )}
    </label>
  );

  if (facet.kind === "range") {
    const inputType = facet.type === "date" ? "date" : "number";
    return (
      <div className="facet">
        {label}
        <div className="range-inputs">
          <input
            id={`facet-${facet.field}`}
            type={inputType}
            placeholder="min"
            aria-label={`${facet.label} min`}
            value={input.min ?? ""}
            onChange={(e) => onChange({ min: e.target.value })}
          />
          <span className="muted">to</span>
          <input
            type={inputType}
            placeholder="max"
            aria-label={`${facet.label} max`}
            value={input.max ?? ""}
            onChange={(e) => onChange({ max: e.target.value })}
          />
        </div>
      </div>
    );
  }

  if (facet.kind === "select") {
    const values =
      facet.type === "boolean"
        ? ["true", "false"]
        : options.status === "loaded"
          ? options.values
          : [];
    return (
      <div className="facet">
        {label}
        <select
          id={`facet-${facet.field}`}
          aria-label={facet.label}
          value={input.selected ?? ""}
          onFocus={facet.type === "term" ? onNeedOptions : undefined}
          onChange={(e) => onChange({ selected: e.target.value })}
        >
          <option value="">(any)</option>
          {options.status === "loading" && <option disabled>Loading…</option>}
          {values.map((value) => (
            <option key={value} value={value}>
              {value}
            </option>
          ))}
        </select>
        {options.status === "error" && (
          <div className="widget-error">Could not load options: {options.message}</div>
        )}
      </div>
    );
  }

  return (
    <div className="facet">
      {label}
      <input
        id={`facet-${facet.field}`}
        type="text"
        placeholder="contains…"
        aria-label={facet.label}
        value={input.text ?? ""}
        onChange={(e) => onChange({ text: e.target.value })}
      />
    </div>
  );
}
