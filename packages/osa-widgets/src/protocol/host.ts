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

import { viewError, viewLog } from "./log";

/** Keep model-context summaries short — they land in the model's context window. */
const MODEL_CONTEXT_MAX_CHARS = 200;

/**
 * How long to wait for the host to deliver the initial tool result before
 * giving up. The host runs the tool BEFORE it renders the widget, so the
 * result is ready by the time we connect — a lapse this long means the host
 * loaded the bundle but never sent `ui/notifications/tool-result`.
 */
const CONNECT_TIMEOUT_MS = 15_000;

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
  connect(timeoutMs: number = CONNECT_TIMEOUT_MS): Promise<unknown> {
    return new Promise<unknown>((resolve, reject) => {
      let settled = false;
      const timer = setTimeout(() => {
        if (settled) return;
        settled = true;
        const err = new Error(
          `No tool result within ${timeoutMs}ms. The host loaded the widget but ` +
            "never delivered ui/notifications/tool-result — the handshake did not complete.",
        );
        viewError("host", err.message);
        reject(err);
      }, timeoutMs);
      const settle = (data: unknown) => {
        if (settled) return;
        settled = true;
        clearTimeout(timer);
        resolve(data);
      };
      // A tool-input notification (the arguments) is purely diagnostic here —
      // it confirms the host is talking to us even before the result lands.
      this.app.ontoolinput = (params) => viewLog("host", "tool-input received", params);
      this.app.ontoolresult = (params: CallToolResult) => {
        viewLog("host", "tool-result received", {
          isError: params.isError ?? false,
          hasStructured: params.structuredContent != null,
          keys: params.structuredContent ? Object.keys(params.structuredContent) : [],
        });
        settle(params.structuredContent ?? params);
      };
      viewLog("host", "connecting via PostMessageTransport…");
      this.app
        .connect(this._transport)
        .then(() => viewLog("host", "ui/initialize handshake complete; awaiting tool-result"))
        .catch((err: unknown) => {
          if (settled) return;
          settled = true;
          clearTimeout(timer);
          viewError("host", "connect failed", err);
          reject(err instanceof Error ? err : new Error(String(err)));
        });
    });
  }

  async callTool<T>(name: string, args: object): Promise<T> {
    viewLog("host", `callTool ${name}`, args);
    const result = await this.app.callServerTool({ name, arguments: args as Record<string, unknown> });
    if (result.isError) {
      const message = errorText(result);
      viewError("host", `callTool ${name} returned an error`, message);
      throw new Error(message);
    }
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
