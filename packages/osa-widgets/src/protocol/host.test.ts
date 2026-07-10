import { describe, expect, it } from "vitest";

import { JsonRpcEndpoint, type JsonRpcRequest } from "./jsonrpc";
import { Messages, PROTOCOL_VERSION, WidgetHost } from "./host";
import { FakeTransport } from "./jsonrpc.test";

function makeHost(): { host: WidgetHost; transport: FakeTransport } {
  const transport = new FakeTransport();
  return { host: new WidgetHost(new JsonRpcEndpoint(transport)), transport };
}

function respondTo(transport: FakeTransport, request: JsonRpcRequest, result: unknown): void {
  transport.receive({ jsonrpc: "2.0", id: request.id, result });
}

describe("WidgetHost.connect", () => {
  it("announces the protocol version in ui/initialize", () => {
    const { host, transport } = makeHost();
    void host.connect();

    const request = transport.lastSent();
    expect(request.method).toBe(Messages.initialize);
    expect(request.params).toEqual({ protocolVersion: PROTOCOL_VERSION });
    expect(PROTOCOL_VERSION).toBe("2026-01-26");
  });

  it("resolves render data from the initialize response `renderData` key", async () => {
    const { host, transport } = makeHost();
    const connected = host.connect();
    respondTo(transport, transport.lastSent(), { renderData: { hello: 1 } });

    await expect(connected).resolves.toEqual({ hello: 1 });
  });

  it("resolves render data from `toolResult.structuredContent`", async () => {
    const { host, transport } = makeHost();
    const connected = host.connect();
    respondTo(transport, transport.lastSent(), {
      toolResult: { structuredContent: { rows: [1, 2] } },
    });

    await expect(connected).resolves.toEqual({ rows: [1, 2] });
  });

  it("resolves render data from a top-level `structuredContent`", async () => {
    const { host, transport } = makeHost();
    const connected = host.connect();
    respondTo(transport, transport.lastSent(), { structuredContent: { n: 3 } });

    await expect(connected).resolves.toEqual({ n: 3 });
  });

  it("falls back to a ui/render-data notification when the initialize response has no data", async () => {
    const { host, transport } = makeHost();
    const connected = host.connect();
    respondTo(transport, transport.lastSent(), { capabilities: {} });
    transport.receive({
      jsonrpc: "2.0",
      method: Messages.renderData,
      params: { structuredContent: { late: true } },
    });

    await expect(connected).resolves.toEqual({ late: true });
  });

  it("accepts render-data notification params directly when there is no structuredContent wrapper", async () => {
    const { host, transport } = makeHost();
    const connected = host.connect();
    transport.receive({
      jsonrpc: "2.0",
      method: Messages.renderData,
      params: { bare: "params" },
    });

    await expect(connected).resolves.toEqual({ bare: "params" });
  });

  it("keeps the first data that arrives when both channels deliver", async () => {
    const { host, transport } = makeHost();
    const connected = host.connect();
    transport.receive({
      jsonrpc: "2.0",
      method: Messages.renderData,
      params: { structuredContent: { source: "notification" } },
    });
    respondTo(transport, transport.lastSent(), {
      renderData: { source: "response" },
    });

    await expect(connected).resolves.toEqual({ source: "notification" });
  });

  it("rejects when initialize fails and no notification arrived", async () => {
    const { host, transport } = makeHost();
    const connected = host.connect();
    transport.receive({
      jsonrpc: "2.0",
      id: transport.lastSent().id,
      error: { code: -32000, message: "host says no" },
    });

    await expect(connected).rejects.toThrow("host says no");
  });
});

describe("WidgetHost.callTool", () => {
  it("sends tools/call with name and arguments and returns structuredContent", async () => {
    const { host, transport } = makeHost();
    const pending = host.callTool<{ ok: boolean }>("fetch_page", { limit: 5 });

    const request = transport.lastSent();
    expect(request.method).toBe(Messages.callTool);
    expect(request.params).toEqual({ name: "fetch_page", arguments: { limit: 5 } });

    respondTo(transport, request, { structuredContent: { ok: true } });
    await expect(pending).resolves.toEqual({ ok: true });
  });

  it("throws the text content when the tool result isError", async () => {
    const { host, transport } = makeHost();
    const pending = host.callTool("fetch_page", {});

    respondTo(transport, transport.lastSent(), {
      isError: true,
      content: [{ type: "text", text: "no such table" }],
    });

    await expect(pending).rejects.toThrow("no such table");
  });

  it("throws a generic message when an isError result has no text content", async () => {
    const { host, transport } = makeHost();
    const pending = host.callTool("fetch_page", {});

    respondTo(transport, transport.lastSent(), { isError: true });

    await expect(pending).rejects.toThrow(/tool call failed/i);
  });
});

describe("WidgetHost.updateModelContext", () => {
  it("fires a ui/update-model-context notification", () => {
    const { host, transport } = makeHost();
    host.updateModelContext("user opened record urn:osa:x:rec:1");

    expect(transport.sent).toEqual([
      {
        jsonrpc: "2.0",
        method: Messages.updateModelContext,
        params: { text: "user opened record urn:osa:x:rec:1" },
      },
    ]);
  });

  it("truncates oversized summaries to stay under ~200 chars", () => {
    const { host, transport } = makeHost();
    host.updateModelContext("x".repeat(500));

    const sent = transport.sent[0] as { params: { text: string } };
    expect(sent.params.text.length).toBeLessThanOrEqual(200);
    expect(sent.params.text.endsWith("…")).toBe(true);
  });

  it("is fire-and-forget: no response is expected or tracked", () => {
    const { host, transport } = makeHost();
    host.updateModelContext("summary");
    const sent = transport.sent[0] as Record<string, unknown>;
    expect(sent["id"]).toBeUndefined();
  });
});
