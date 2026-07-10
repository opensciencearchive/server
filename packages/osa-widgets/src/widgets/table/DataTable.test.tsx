import { render, screen, waitFor } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { describe, expect, it } from "vitest";

import type { TablePage } from "../../lib/types";
import { mockHost } from "../testing";
import { DataTable } from "./DataTable";

function makePage(overrides: Partial<TablePage> = {}): TablePage {
  return {
    query: {
      schema: "geo-seq",
      table: "records",
      sort: [{ column: "mass", direction: "asc" }],
      limit: 2,
    },
    columns: [
      { name: "srn", type: "text" },
      { name: "mass", type: "number", unit: "kg" },
      { name: "collected", type: "date" },
      { name: "alive", type: "boolean" },
    ],
    rows: [
      { srn: "urn:osa:x:rec:1", mass: 1234.5, collected: "2026-01-05T00:00:00Z", alive: true },
      { srn: "urn:osa:x:rec:2", mass: 7, collected: "2026-02-06T00:00:00Z", alive: false },
    ],
    truncated: false,
    ...overrides,
  };
}

describe("DataTable", () => {
  it("renders type-aware headers and formatted cells", () => {
    const { host } = mockHost();
    render(<DataTable initial={makePage()} host={host} />);

    expect(screen.getByRole("button", { name: /mass \(kg\)/ })).toBeDefined();
    expect(screen.getByText((1234.5).toLocaleString(undefined, { maximumFractionDigits: 3 }))).toBeDefined();
    expect(screen.getByText("✓")).toBeDefined();
    expect(screen.getByText("✗")).toBeDefined();
    expect(
      screen.getByText(new Date("2026-01-05T00:00:00Z").toLocaleString()),
    ).toBeDefined();
  });

  it("shows the empty state when there are no rows", () => {
    const { host } = mockHost();
    render(<DataTable initial={makePage({ rows: [] })} host={host} />);
    expect(screen.getByText("No rows match.")).toBeDefined();
  });

  it("re-sorts server-side on header click, toggling direction", async () => {
    const user = userEvent.setup();
    const { host, callTool } = mockHost();
    const sorted = makePage({
      query: {
        schema: "geo-seq",
        table: "records",
        sort: [{ column: "mass", direction: "desc" }],
        limit: 2,
      },
    });
    callTool.mockResolvedValue(sorted);
    render(<DataTable initial={makePage()} host={host} />);

    // Initial sort is mass asc, so clicking mass toggles to desc.
    await user.click(screen.getByRole("button", { name: /mass/ }));

    await waitFor(() =>
      expect(callTool).toHaveBeenCalledWith("fetch_page", {
        schema: "geo-seq",
        table: "records",
        sort: [{ column: "mass", direction: "desc" }],
        limit: 2,
      }),
    );
  });

  it("sorts a different column ascending first", async () => {
    const user = userEvent.setup();
    const { host, callTool } = mockHost();
    callTool.mockResolvedValue(makePage());
    render(<DataTable initial={makePage()} host={host} />);

    await user.click(screen.getByRole("button", { name: /collected/ }));

    await waitFor(() =>
      expect(callTool).toHaveBeenCalledWith(
        "fetch_page",
        expect.objectContaining({ sort: [{ column: "collected", direction: "asc" }] }),
      ),
    );
  });

  it("pages forward with next_cursor and back with the cursor stack", async () => {
    const user = userEvent.setup();
    const { host, callTool } = mockHost();
    const pageOne = makePage({ next_cursor: "c1", truncated: true });
    const pageTwo = makePage({
      rows: [{ srn: "urn:osa:x:rec:3", mass: 9, collected: "2026-03-01T00:00:00Z", alive: true }],
      next_cursor: "c2",
      truncated: true,
    });
    callTool.mockResolvedValueOnce(pageTwo).mockResolvedValueOnce(pageOne);
    render(<DataTable initial={pageOne} host={host} />);

    await user.click(screen.getByRole("button", { name: "Next" }));
    await waitFor(() =>
      expect(callTool).toHaveBeenNthCalledWith(
        1,
        "fetch_page",
        expect.objectContaining({ cursor: "c1" }),
      ),
    );
    await screen.findByText("urn:osa:x:rec:3");

    await user.click(screen.getByRole("button", { name: "Prev" }));
    await waitFor(() => expect(callTool).toHaveBeenCalledTimes(2));
    const secondArgs = callTool.mock.calls[1]?.[1] as Record<string, unknown>;
    expect("cursor" in secondArgs).toBe(false); // back to the first page
  });

  it("disables Prev on the first page and Next without a cursor", () => {
    const { host } = mockHost();
    render(<DataTable initial={makePage()} host={host} />);
    expect((screen.getByRole("button", { name: "Prev" }) as HTMLButtonElement).disabled).toBe(true);
    expect((screen.getByRole("button", { name: "Next" }) as HTMLButtonElement).disabled).toBe(true);
  });

  it("expands a clicked row and updates model context with the srn", async () => {
    const user = userEvent.setup();
    const { host, updateModelContext } = mockHost();
    render(<DataTable initial={makePage()} host={host} />);

    await user.click(screen.getByText("urn:osa:x:rec:1"));

    expect(updateModelContext).toHaveBeenCalledWith("user opened record urn:osa:x:rec:1");
    // Expanded row repeats every cell with its label.
    expect(screen.getAllByText("urn:osa:x:rec:1").length).toBeGreaterThan(1);
  });

  it("shows a truncation notice on an unpaged truncated view", () => {
    const { host } = mockHost();
    render(<DataTable initial={makePage({ truncated: true })} host={host} />);
    expect(screen.getByText(/showing first 2 rows \(truncated\)/)).toBeDefined();
  });
});
