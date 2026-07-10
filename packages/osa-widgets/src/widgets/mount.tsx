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

export function mountWidget<T>(render: RenderWidget<T>): void {
  const container = document.getElementById("root");
  if (!container) throw new Error("Widget HTML is missing the #root container");
  const host = WidgetHost.fromWindow(window);
  createRoot(container).render(<Bootstrap host={host} render={render} />);
}
