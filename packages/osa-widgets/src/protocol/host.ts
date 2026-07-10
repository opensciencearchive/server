/**
 * WidgetHost: the widget-side view of an MCP Apps host.
 *
 * Compatibility seam — every host-facing message name and payload shape
 * assumption lives in this module (plus the framing in jsonrpc.ts). MCP Apps
 * is a young spec and hosts vary in where they put the initial render data,
 * so `connect()` is deliberately tolerant: it accepts data from either the
 * ui/initialize response or a later ui/render-data notification.
 */

import { JsonRpcEndpoint, windowTransport } from "./jsonrpc";

export const PROTOCOL_VERSION = "2026-01-26";

/** All host-facing JSON-RPC method names, in one place. */
export const Messages = {
  initialize: "ui/initialize",
  renderData: "ui/render-data",
  callTool: "tools/call",
  updateModelContext: "ui/update-model-context",
} as const;

/** Keep model-context summaries short — they land in the model's context window. */
const MODEL_CONTEXT_MAX_CHARS = 200;

/** The surface widget components depend on (narrow, easy to fake in tests). */
export interface WidgetHostApi {
  callTool<T>(name: string, args: object): Promise<T>;
  updateModelContext(text: string): void;
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null;
}

/**
 * Hosts have shipped the initial render data under several keys while the
 * MCP Apps spec stabilizes; probe the known ones in order of specificity.
 */
function extractInitializeData(result: unknown): unknown {
  if (!isRecord(result)) return undefined;
  if ("renderData" in result) return result["renderData"];
  const toolResult = result["toolResult"];
  if (isRecord(toolResult) && "structuredContent" in toolResult) {
    return toolResult["structuredContent"];
  }
  if ("structuredContent" in result) return result["structuredContent"];
  return undefined;
}

function extractRenderData(params: unknown): unknown {
  if (isRecord(params) && "structuredContent" in params) {
    return params["structuredContent"];
  }
  return params;
}

function extractErrorText(result: Record<string, unknown>): string {
  const content = result["content"];
  if (Array.isArray(content)) {
    for (const item of content) {
      if (isRecord(item) && typeof item["text"] === "string") return item["text"];
    }
  }
  return "Tool call failed";
}

export class WidgetHost implements WidgetHostApi {
  constructor(private readonly rpc: JsonRpcEndpoint) {}

  static fromWindow(win: Window): WidgetHost {
    return new WidgetHost(new JsonRpcEndpoint(windowTransport(win)));
  }

  /**
   * Handshake with the host and resolve the widget's initial render data,
   * from whichever channel delivers it first (see module docstring).
   */
  connect(): Promise<unknown> {
    return new Promise((resolve, reject) => {
      let settled = false;
      const settle = (data: unknown) => {
        if (!settled) {
          settled = true;
          resolve(data);
        }
      };
      this.rpc.onNotification(Messages.renderData, (params) => {
        settle(extractRenderData(params));
      });
      this.rpc
        .request(Messages.initialize, { protocolVersion: PROTOCOL_VERSION })
        .then((result) => {
          const data = extractInitializeData(result);
          if (data !== undefined) settle(data);
          // else: keep waiting for the ui/render-data notification.
        })
        .catch((err: unknown) => {
          if (!settled) {
            settled = true;
            reject(err instanceof Error ? err : new Error(String(err)));
          }
        });
    });
  }

  async callTool<T>(name: string, args: object): Promise<T> {
    const result = await this.rpc.request(Messages.callTool, { name, arguments: args });
    if (isRecord(result) && result["isError"] === true) {
      throw new Error(extractErrorText(result));
    }
    return (isRecord(result) ? result["structuredContent"] : undefined) as T;
  }

  updateModelContext(text: string): void {
    const short =
      text.length > MODEL_CONTEXT_MAX_CHARS
        ? `${text.slice(0, MODEL_CONTEXT_MAX_CHARS - 1)}…`
        : text;
    this.rpc.notify(Messages.updateModelContext, { text: short });
  }
}
