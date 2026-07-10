/** Shared widget bootstrap: connect to the host, then render the widget. */

import { Component, useEffect, useState, type ErrorInfo, type ReactElement, type ReactNode } from "react";
import { createRoot } from "react-dom/client";

import { viewError, viewLog } from "../protocol/log";
import { WidgetHost, type WidgetHostApi } from "../protocol/host";

type RenderWidget<T> = (data: T, host: WidgetHostApi) => ReactElement;

type BootState<T> =
  | { status: "connecting" }
  | { status: "error"; message: string }
  | { status: "ready"; data: T };

/**
 * Catches render-time crashes (bad data shape, chart-lib errors) so the widget
 * shows the failure instead of a blank frame — the single most common reason a
 * loaded bundle renders "nothing".
 */
class WidgetErrorBoundary extends Component<
  { scope: string; children: ReactNode },
  { error: Error | null }
> {
  state: { error: Error | null } = { error: null };

  static getDerivedStateFromError(error: Error) {
    return { error };
  }

  componentDidCatch(error: Error, info: ErrorInfo) {
    viewError(this.props.scope, "widget render crashed", error, info.componentStack);
  }

  render(): ReactNode {
    if (this.state.error) {
      return (
        <div className="widget-status widget-error">
          Widget crashed while rendering: {this.state.error.message}
        </div>
      );
    }
    return this.props.children;
  }
}

export function Bootstrap<T>({
  scope,
  host,
  render,
}: {
  scope: string;
  host: WidgetHost;
  render: RenderWidget<T>;
}): ReactElement {
  const [state, setState] = useState<BootState<T>>({ status: "connecting" });

  useEffect(() => {
    let cancelled = false;
    viewLog(scope, "connecting to host…");
    host.connect().then(
      (data) => {
        if (cancelled) return;
        viewLog(scope, "render data received", data);
        setState({ status: "ready", data: data as T });
      },
      (err: unknown) => {
        if (cancelled) return;
        const message = err instanceof Error ? err.message : String(err);
        viewError(scope, "failed to obtain render data", message);
        setState({ status: "error", message });
      },
    );
    return () => {
      cancelled = true;
    };
  }, [scope, host, render]);

  if (state.status === "connecting") {
    return <div className="widget-status">Connecting to host…</div>;
  }
  if (state.status === "error") {
    return <div className="widget-status widget-error">Failed to load: {state.message}</div>;
  }
  return <WidgetErrorBoundary scope={scope}>{render(state.data, host)}</WidgetErrorBoundary>;
}

export function StandaloneNotice(): ReactElement {
  return (
    <div className="widget-status">
      This is an OSA MCP Apps widget. It has no data of its own — an MCP Apps host
      (Claude, Goose, …) renders it inside a sandboxed iframe and delivers the tool
      result over postMessage. Connect the host to the node&apos;s <code>/mcp</code>{" "}
      endpoint instead of opening this file directly.
    </div>
  );
}

export function mountWidget<T>(scope: string, render: RenderWidget<T>): void {
  viewLog(scope, "mounting");
  const container = document.getElementById("root");
  if (!container) throw new Error("Widget HTML is missing the #root container");
  // Any throw during setup (SDK/transport construction, React root creation)
  // would otherwise leave a blank iframe with no clue. Paint the failure into
  // #root directly — this path can't rely on React having mounted.
  try {
    const root = createRoot(container);
    // Opened standalone (no embedding host), window.parent === window: our own
    // ui/initialize would echo straight back and be answered -32601 by our own
    // endpoint. Explain the situation instead of surfacing that self-reply.
    if (window.parent === window) {
      viewLog(scope, "no embedding host (opened standalone)");
      root.render(<StandaloneNotice />);
      return;
    }
    const host = WidgetHost.fromWindow(window);
    root.render(<Bootstrap scope={scope} host={host} render={render} />);
  } catch (err) {
    const message = err instanceof Error ? err.message : String(err);
    viewError(scope, "failed to mount", err);
    container.textContent = `Widget failed to start: ${message}`;
    container.className = "widget-status widget-error";
  }
}
