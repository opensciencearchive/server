import { describe, expect, it } from "vitest";

import { fetchPageArgs, sampleValuesArgs } from "./args";
import type { TableQuery } from "./types";

const query: TableQuery = {
  schema: "geo-seq",
  table: "records",
  sort: [{ column: "created_at", direction: "desc" }],
  limit: 25,
};

describe("fetchPageArgs", () => {
  it("carries schema, table, sort, and limit from the query", () => {
    expect(fetchPageArgs(query)).toEqual({
      schema: "geo-seq",
      table: "records",
      sort: [{ column: "created_at", direction: "desc" }],
      limit: 25,
    });
  });

  it("omits filter and cursor when absent (server serializes exclude_none)", () => {
    const args = fetchPageArgs(query);
    expect("filter" in args).toBe(false);
    expect("cursor" in args).toBe(false);
  });

  it("includes the query filter when present", () => {
    const filtered: TableQuery = {
      ...query,
      filter: { kind: "predicate", field: "metadata.species", op: "eq", value: "mouse" },
    };
    expect(fetchPageArgs(filtered)["filter"]).toEqual(filtered.filter);
  });

  it("passes the cursor through", () => {
    expect(fetchPageArgs(query, "abc123")["cursor"]).toBe("abc123");
  });

  it("applies a sort override without mutating the query", () => {
    const args = fetchPageArgs(query, undefined, [{ column: "mass", direction: "asc" }]);
    expect(args["sort"]).toEqual([{ column: "mass", direction: "asc" }]);
    expect(query.sort).toEqual([{ column: "created_at", direction: "desc" }]);
  });
});

describe("sampleValuesArgs", () => {
  it("builds the sample_values tool arguments", () => {
    expect(sampleValuesArgs("geo-seq", "records", "species")).toEqual({
      schema: "geo-seq",
      table: "records",
      column: "species",
    });
  });
});
