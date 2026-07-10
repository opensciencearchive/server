/**
 * WidgetHost: the widget-side view of an MCP Apps host.
 *
 * Compatibility seam. The MCP Apps host↔iframe handshake (initialize, then a
 * `ui/notifications/tool-result` notification carrying the CallToolResult) is
 * a young, moving contract, so rather than hand-track it we delegate to the
 * official `@modelcontextprotocol/ext-apps` `App` — the reference client that
 * Claude and other hosts are built against. This module is the ONLY place the
 * SDK is referenced; widgets depend only on the narrow `WidgetHostApi` below.
 */

import { App, PostMessageTransport } from "@modelcontextprotocol/ext-apps";
import type { CallToolResult } from "@modelcontextprotocol/sdk/types.js";

/** Keep model-context summaries short — they land in the model's context window. */
const MODEL_CONTEXT_MAX_CHARS = 200;

/** The surface widget components depend on (narrow, easy to fake in tests). */
export interface WidgetHostApi {
  callTool<T>(name: string, args: object): Promise<T>;
  updateModelContext(text: string): void;
}

function errorText(result: CallToolResult): string {
  for (const block of result.content ?? []) {
    if (block.type === "text" && typeof block.text === "string") return block.text;
  }
  return "Tool call failed";
}

export class WidgetHost implements WidgetHostApi {
  constructor(private readonly app: App) {}

  static fromWindow(win: Window): WidgetHost {
    // A View posts to, and listens on, its embedding host (the parent frame).
    const parent = win.parent;
    const app = new App({ name: "osa-widgets", version: "0.1.0" }, {});
    const host = new WidgetHost(app);
    host._transport = new PostMessageTransport(parent, parent);
    return host;
  }

  private _transport?: PostMessageTransport;

  /**
   * Complete the handshake and resolve the widget's initial render data — the
   * `structuredContent` of the tool result the host pushes after connecting.
   *
   * The result notification can arrive before OR after `connect()` resolves,
   * so the handler is registered first and owns the resolution; `connect()`
   * only needs to not reject.
   */
  connect(): Promise<unknown> {
    return new Promise<unknown>((resolve, reject) => {
      let settled = false;
      const settle = (data: unknown) => {
        if (!settled) {
          settled = true;
          resolve(data);
        }
      };
      this.app.ontoolresult = (params: CallToolResult) => {
        settle(params.structuredContent ?? params);
      };
      this.app.connect(this._transport).catch((err: unknown) => {
        if (!settled) {
          settled = true;
          reject(err instanceof Error ? err : new Error(String(err)));
        }
      });
    });
  }

  async callTool<T>(name: string, args: object): Promise<T> {
    const result = await this.app.callServerTool({ name, arguments: args as Record<string, unknown> });
    if (result.isError) throw new Error(errorText(result));
    return result.structuredContent as T;
  }

  updateModelContext(text: string): void {
    const short =
      text.length > MODEL_CONTEXT_MAX_CHARS
        ? `${text.slice(0, MODEL_CONTEXT_MAX_CHARS - 1)}…`
        : text;
    // Fire-and-forget: a context hint must never block or crash the widget.
    void this.app.updateModelContext({ content: [{ type: "text", text: short }] }).catch(() => {});
  }
}
