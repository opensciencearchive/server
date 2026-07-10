import { describe, expect, it, vi } from "vitest";

import {
  JsonRpcEndpoint,
  type JsonRpcRequest,
  type RpcTransport,
} from "./jsonrpc";

/** In-memory transport standing in for window.parent.postMessage. */
export class FakeTransport implements RpcTransport {
  sent: unknown[] = [];
  private handlers: ((data: unknown) => void)[] = [];

  post(message: unknown): void {
    this.sent.push(message);
  }

  onMessage(handler: (data: unknown) => void): void {
    this.handlers.push(handler);
  }

  /** Simulate a message arriving from the host. */
  receive(data: unknown): void {
    for (const handler of this.handlers) handler(data);
  }

  lastSent(): JsonRpcRequest {
    return this.sent[this.sent.length - 1] as JsonRpcRequest;
  }
}

describe("JsonRpcEndpoint", () => {
  it("sends requests with incrementing ids", () => {
    const transport = new FakeTransport();
    const rpc = new JsonRpcEndpoint(transport);
    void rpc.request("a");
    void rpc.request("b", { x: 1 });

    expect(transport.sent).toEqual([
      { jsonrpc: "2.0", id: 1, method: "a", params: undefined },
      { jsonrpc: "2.0", id: 2, method: "b", params: { x: 1 } },
    ]);
  });

  it("matches responses to requests by id, even out of order", async () => {
    const transport = new FakeTransport();
    const rpc = new JsonRpcEndpoint(transport);
    const first = rpc.request("one");
    const second = rpc.request("two");

    transport.receive({ jsonrpc: "2.0", id: 2, result: "second-result" });
    transport.receive({ jsonrpc: "2.0", id: 1, result: "first-result" });

    await expect(first).resolves.toBe("first-result");
    await expect(second).resolves.toBe("second-result");
  });

  it("rejects the request when the response carries an error", async () => {
    const transport = new FakeTransport();
    const rpc = new JsonRpcEndpoint(transport);
    const pending = rpc.request("boom");

    transport.receive({
      jsonrpc: "2.0",
      id: 1,
      error: { code: -32000, message: "it broke" },
    });

    await expect(pending).rejects.toThrow("it broke");
  });

  it("sends notifications without an id", () => {
    const transport = new FakeTransport();
    const rpc = new JsonRpcEndpoint(transport);
    rpc.notify("ui/update-model-context", { text: "hi" });

    expect(transport.sent).toEqual([
      {
        jsonrpc: "2.0",
        method: "ui/update-model-context",
        params: { text: "hi" },
      },
    ]);
  });

  it("dispatches incoming notifications to registered handlers", () => {
    const transport = new FakeTransport();
    const rpc = new JsonRpcEndpoint(transport);
    const handler = vi.fn();
    rpc.onNotification("ui/render-data", handler);

    transport.receive({
      jsonrpc: "2.0",
      method: "ui/render-data",
      params: { structuredContent: { rows: [] } },
    });

    expect(handler).toHaveBeenCalledWith({ structuredContent: { rows: [] } });
  });

  it("ignores messages that are not JSON-RPC 2.0", () => {
    const transport = new FakeTransport();
    const rpc = new JsonRpcEndpoint(transport);
    const handler = vi.fn();
    rpc.onNotification("anything", handler);

    transport.receive("plain string");
    transport.receive({ some: "object" });
    transport.receive(null);

    expect(handler).not.toHaveBeenCalled();
    expect(transport.sent).toEqual([]);
  });

  it("answers host-to-iframe requests via registered request handlers", async () => {
    const transport = new FakeTransport();
    const rpc = new JsonRpcEndpoint(transport);
    rpc.onRequest("ping", (params) => ({ echoed: params }));

    transport.receive({ jsonrpc: "2.0", id: 7, method: "ping", params: "yo" });
    await vi.waitFor(() => expect(transport.sent).toHaveLength(1));

    expect(transport.sent[0]).toEqual({
      jsonrpc: "2.0",
      id: 7,
      result: { echoed: "yo" },
    });
  });

  it("responds with method-not-found for unknown host requests", async () => {
    const transport = new FakeTransport();
    const rpc = new JsonRpcEndpoint(transport);
    expect(rpc).toBeDefined();

    transport.receive({ jsonrpc: "2.0", id: 9, method: "nope" });
    await vi.waitFor(() => expect(transport.sent).toHaveLength(1));

    const response = transport.sent[0] as Record<string, unknown>;
    expect(response["id"]).toBe(9);
    expect(response["error"]).toMatchObject({ code: -32601 });
  });
});
