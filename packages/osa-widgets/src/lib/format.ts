/** Type-aware cell formatting for table and detail views. */

import type { FieldType } from "./types";

const MISSING = "—";

export function formatCell(value: unknown, type: FieldType, unit?: string): string {
  if (value === null || value === undefined) return MISSING;
  switch (type) {
    case "number":
      return formatNumber(value, unit);
    case "date":
      return formatDate(value);
    case "boolean":
      return value === true || value === "true" ? "✓" : "✗";
    case "url":
    case "text":
    case "term":
      return String(value);
  }
}

function formatNumber(value: unknown, unit?: string): string {
  const n = typeof value === "number" ? value : Number(value);
  if (typeof value !== "number" && (typeof value !== "string" || value.trim() === "")) {
    return String(value);
  }
  if (!Number.isFinite(n)) return String(value);
  const text = Number.isInteger(n)
    ? n.toLocaleString()
    : n.toLocaleString(undefined, { maximumFractionDigits: 3 });
  return unit ? `${text} ${unit}` : text;
}

function formatDate(value: unknown): string {
  const date = new Date(String(value));
  if (Number.isNaN(date.getTime())) return String(value);
  return date.toLocaleString();
}

export function isRightAligned(type: FieldType): boolean {
  return type === "number" || type === "date";
}

export function alignmentClass(type: FieldType): string {
  return isRightAligned(type) ? "cell-right" : "cell-left";
}
