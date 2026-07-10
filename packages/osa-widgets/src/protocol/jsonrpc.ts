/**
 * Minimal JSON-RPC 2.0 framing over window.parent.postMessage.
 *
 * This module and host.ts together form the single compatibility seam
 * between OSA widgets and MCP Apps hosts. The MCP Apps spec is young and
 * host implementations differ in detail, so every wire-shape assumption
 * must live here — never inside widget components.
 */

export interface JsonRpcRequest {
  jsonrpc: "2.0";
  id: number;
  method: string;
  params?: unknown;
}

export interface JsonRpcErrorShape {
  code: number;
  message: string;
  data?: unknown;
}

export interface JsonRpcResponse {
  jsonrpc: "2.0";
  id: number;
  result?: unknown;
  error?: JsonRpcErrorShape;
}

export interface JsonRpcNotification {
  jsonrpc: "2.0";
  method: string;
  params?: unknown;
}

export type NotificationHandler = (params: unknown) => void;
export type RequestHandler = (params: unknown) => unknown | Promise<unknown>;

/** Transport abstraction so the framing is unit-testable without an iframe. */
export interface RpcTransport {
  post(message: unknown): void;
  onMessage(handler: (data: unknown) => void): void;
}

/** Production transport: talk to the embedding host across the iframe boundary. */
export function windowTransport(win: Window): RpcTransport {
  return {
    post: (message) => win.parent.postMessage(message, "*"),
    onMessage: (handler) =>
      win.addEventListener("message", (event: MessageEvent) => handler(event.data)),
  };
}

interface PendingRequest {
  resolve: (value: unknown) => void;
  reject: (reason: Error) => void;
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null;
}

export class JsonRpcEndpoint {
  private nextId = 1;
  private readonly pending = new Map<number, PendingRequest>();
  private readonly notificationHandlers = new Map<string, NotificationHandler[]>();
  private readonly requestHandlers = new Map<string, RequestHandler>();

  constructor(private readonly transport: RpcTransport) {
    transport.onMessage((data) => this.dispatch(data));
  }

  request(method: string, params?: unknown): Promise<unknown> {
    const id = this.nextId++;
    const message: JsonRpcRequest = { jsonrpc: "2.0", id, method, params };
    return new Promise((resolve, reject) => {
      this.pending.set(id, { resolve, reject });
      this.transport.post(message);
    });
  }

  notify(method: string, params?: unknown): void {
    const message: JsonRpcNotification = { jsonrpc: "2.0", method, params };
    this.transport.post(message);
  }

  onNotification(method: string, handler: NotificationHandler): void {
    const handlers = this.notificationHandlers.get(method) ?? [];
    handlers.push(handler);
    this.notificationHandlers.set(method, handlers);
  }

  onRequest(method: string, handler: RequestHandler): void {
    this.requestHandlers.set(method, handler);
  }

  private dispatch(data: unknown): void {
    // Anything without the JSON-RPC envelope is host/browser noise; ignore it.
    if (!isRecord(data) || data["jsonrpc"] !== "2.0") return;
    const method = data["method"];
    const id = data["id"];
    if (typeof method === "string") {
      if (typeof id === "number") {
        void this.answerRequest(id, method, data["params"]);
      } else {
        for (const handler of this.notificationHandlers.get(method) ?? []) {
          handler(data["params"]);
        }
      }
      return;
    }
    if (typeof id === "number") this.settleResponse(id, data);
  }

  private settleResponse(id: number, data: Record<string, unknown>): void {
    const entry = this.pending.get(id);
    if (!entry) return;
    this.pending.delete(id);
    const error = data["error"];
    if (isRecord(error)) {
      const message = typeof error["message"] === "string" ? error["message"] : "JSON-RPC error";
      entry.reject(new Error(message));
      return;
    }
    entry.resolve(data["result"]);
  }

  private async answerRequest(id: number, method: string, params: unknown): Promise<void> {
    const handler = this.requestHandlers.get(method);
    if (!handler) {
      this.transport.post({
        jsonrpc: "2.0",
        id,
        error: { code: -32601, message: `Method not found: ${method}` },
      } satisfies JsonRpcResponse);
      return;
    }
    try {
      const result = await handler(params);
      this.transport.post({ jsonrpc: "2.0", id, result } satisfies JsonRpcResponse);
    } catch (err) {
      this.transport.post({
        jsonrpc: "2.0",
        id,
        error: { code: -32000, message: err instanceof Error ? err.message : String(err) },
      } satisfies JsonRpcResponse);
    }
  }
}
