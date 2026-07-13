import { describe, expect, it } from "vitest";

import { alignmentClass, formatCell, isRightAligned } from "./format";

describe("formatCell", () => {
  it("formats integers with locale grouping and no decimals", () => {
    expect(formatCell(1234567, "number")).toBe((1234567).toLocaleString());
  });

  it("formats floats with limited precision", () => {
    expect(formatCell(3.14159265, "number")).toBe(
      (3.14159265).toLocaleString(undefined, { maximumFractionDigits: 3 }),
    );
  });

  it("appends a unit suffix when given", () => {
    expect(formatCell(42, "number", "mg/L")).toBe(`${(42).toLocaleString()} mg/L`);
  });

  it("coerces numeric strings", () => {
    expect(formatCell("1500", "number")).toBe((1500).toLocaleString());
  });

  it("passes through non-numeric values for number columns", () => {
    expect(formatCell("n/a", "number")).toBe("n/a");
  });

  it("formats dates as a local date-time", () => {
    const iso = "2026-03-01T12:30:00Z";
    expect(formatCell(iso, "date")).toBe(new Date(iso).toLocaleString());
  });

  it("passes through unparseable dates", () => {
    expect(formatCell("not-a-date", "date")).toBe("not-a-date");
  });

  it("renders booleans as check and cross marks", () => {
    expect(formatCell(true, "boolean")).toBe("✓");
    expect(formatCell(false, "boolean")).toBe("✗");
  });

  it("renders text, term, and url values as-is", () => {
    expect(formatCell("mouse", "term")).toBe("mouse");
    expect(formatCell("hello", "text")).toBe("hello");
    expect(formatCell("https://example.org/x", "url")).toBe("https://example.org/x");
  });

  it("renders missing values as an em dash", () => {
    expect(formatCell(null, "text")).toBe("—");
    expect(formatCell(undefined, "number")).toBe("—");
  });
});

describe("alignment", () => {
  it("right-aligns numbers and dates only", () => {
    expect(isRightAligned("number")).toBe(true);
    expect(isRightAligned("date")).toBe(true);
    expect(isRightAligned("text")).toBe(false);
    expect(isRightAligned("term")).toBe(false);
    expect(isRightAligned("boolean")).toBe(false);
    expect(isRightAligned("url")).toBe(false);
  });

  it("maps alignment to a css class", () => {
    expect(alignmentClass("number")).toBe("cell-right");
    expect(alignmentClass("text")).toBe("cell-left");
  });
});
