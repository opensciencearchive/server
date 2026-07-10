/** Shared widget bootstrap: connect to the host, then render the widget. */

import { useEffect, useState, type ReactElement } from "react";
import { createRoot } from "react-dom/client";

import { WidgetHost, type WidgetHostApi } from "../protocol/host";

type RenderWidget<T> = (data: T, host: WidgetHostApi) => ReactElement;

type BootState<T> =
  | { status: "loading" }
  | { status: "error"; message: string }
  | { status: "ready"; data: T };

export function Bootstrap<T>({
  host,
  render,
}: {
  host: WidgetHost;
  render: RenderWidget<T>;
}): ReactElement {
  const [state, setState] = useState<BootState<T>>({ status: "loading" });

  useEffect(() => {
    let cancelled = false;
    host.connect().then(
      (data) => {
        if (!cancelled) setState({ status: "ready", data: data as T });
      },
      (err: unknown) => {
        if (!cancelled) {
          setState({
            status: "error",
            message: err instanceof Error ? err.message : String(err),
          });
        }
      },
    );
    return () => {
      cancelled = true;
    };
  }, [host, render]);

  if (state.status === "loading") return <div className="widget-status">Loading…</div>;
  if (state.status === "error") {
    return <div className="widget-status widget-error">Failed to load: {state.message}</div>;
  }
  return render(state.data, host);
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

export function mountWidget<T>(render: RenderWidget<T>): void {
  const container = document.getElementById("root");
  if (!container) throw new Error("Widget HTML is missing the #root container");
  const root = createRoot(container);
  // Opened standalone (no embedding host), window.parent === window: our own
  // ui/initialize would echo straight back and be answered -32601 by our own
  // endpoint. Explain the situation instead of surfacing that self-reply.
  if (window.parent === window) {
    root.render(<StandaloneNotice />);
    return;
  }
  const host = WidgetHost.fromWindow(window);
  root.render(<Bootstrap host={host} render={render} />);
}
