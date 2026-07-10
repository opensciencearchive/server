/**
 * WidgetHost wraps the official ext-apps `App`. These tests cover only OUR
 * glue — the SDK owns the transport/handshake and is tested upstream — by
 * mocking `App` with a controllable double.
 */

import { beforeEach, describe, expect, it, vi } from "vitest";

interface FakeApp {
  ontoolresult: ((params: unknown) => void) | undefined;
  connect: ReturnType<typeof vi.fn>;
  callServerTool: ReturnType<typeof vi.fn>;
  updateModelContext: ReturnType<typeof vi.fn>;
}

// The instance registry and fake live inside the (hoisted) factory; a module
// export exposes the registry to the tests.
vi.mock("@modelcontextprotocol/ext-apps", () => {
  const instances: FakeApp[] = [];
  class FakeAppImpl implements FakeApp {
    ontoolresult: ((params: unknown) => void) | undefined;
    connect = vi.fn().mockResolvedValue(undefined);
    callServerTool = vi.fn();
    updateModelContext = vi.fn().mockResolvedValue({});
    constructor() {
      instances.push(this);
    }
  }
  return {
    App: FakeAppImpl,
    PostMessageTransport: class {
      constructor(
        public target: unknown,
        public source: unknown,
      ) {}
    },
    __instances: instances,
  };
});

import * as extApps from "@modelcontextprotocol/ext-apps";

import { WidgetHost } from "./host";

const appInstances = (extApps as unknown as { __instances: FakeApp[] }).__instances;

function makeHost(): { host: WidgetHost; app: FakeApp } {
  appInstances.length = 0;
  const host = WidgetHost.fromWindow(window);
  return { host, app: appInstances[0]! };
}

beforeEach(() => {
  appInstances.length = 0;
});

describe("connect", () => {
  it("resolves with the tool result's structuredContent", async () => {
    const { host, app } = makeHost();
    const connected = host.connect();
    // Handler was registered before connect resolved; the host pushes it now.
    app.ontoolresult?.({ structuredContent: { rows: [1, 2, 3] } });
    await expect(connected).resolves.toEqual({ rows: [1, 2, 3] });
  });

  it("falls back to the whole params when structuredContent is absent", async () => {
    const { host, app } = makeHost();
    const connected = host.connect();
    app.ontoolresult?.({ content: [{ type: "text", text: "hi" }] });
    await expect(connected).resolves.toEqual({ content: [{ type: "text", text: "hi" }] });
  });

  it("rejects when the handshake fails", async () => {
    const { host, app } = makeHost();
    app.connect.mockRejectedValueOnce(new Error("no host"));
    await expect(host.connect()).rejects.toThrow("no host");
  });
});

describe("callTool", () => {
  it("returns structuredContent on success", async () => {
    const { host, app } = makeHost();
    app.callServerTool.mockResolvedValue({ structuredContent: { next_cursor: "abc" } });
    const out = await host.callTool<{ next_cursor: string }>("fetch_page", { limit: 2 });
    expect(app.callServerTool).toHaveBeenCalledWith({
      name: "fetch_page",
      arguments: { limit: 2 },
    });
    expect(out).toEqual({ next_cursor: "abc" });
  });

  it("throws the tool's error text when isError is set", async () => {
    const { host, app } = makeHost();
    app.callServerTool.mockResolvedValue({
      isError: true,
      content: [{ type: "text", text: "Unknown column 'bogus'" }],
    });
    await expect(host.callTool("show_chart", {})).rejects.toThrow("Unknown column 'bogus'");
  });
});

describe("updateModelContext", () => {
  it("sends a text content block", () => {
    const { host, app } = makeHost();
    host.updateModelContext("user filtered to high-ductility");
    expect(app.updateModelContext).toHaveBeenCalledWith({
      content: [{ type: "text", text: "user filtered to high-ductility" }],
    });
  });

  it("truncates long summaries", () => {
    const { host, app } = makeHost();
    host.updateModelContext("x".repeat(500));
    const arg = app.updateModelContext.mock.calls[0]![0] as { content: { text: string }[] };
    expect(arg.content[0]!.text.length).toBeLessThanOrEqual(200);
    expect(arg.content[0]!.text.endsWith("…")).toBe(true);
  });
});
