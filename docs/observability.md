# Observability (#158)

OSA exports metrics, structured logs, and distributed traces through a single
OpenTelemetry pipeline (built on the bundled Logfire SDK). This guide is for
operators running a self-hosted node: how to scrape or push telemetry, the full
metric catalog, the health/readiness contract, and how request traces flow
through the async outbox.

## Overview

A running node emits three OTel signals:

- **Metrics** — an `osa_*` family of counters, histograms, and gauges (full
  table below), plus the auto-instrumented `http.server.request.duration`
  histogram for every HTTP request.
- **Logs** — the stdlib root logger is bridged into the pipeline, so ordinary
  `logger.info(...)` calls become structured log records.
- **Traces** — spans for HTTP requests and worker dispatch, linked across the
  outbox (see [Traces](#traces)).

There are three ways to consume this, and they compose — you can scrape *and*
push at the same time:

1. **Scrape `GET /metrics`** (Prometheus text format). Enabled by default; a
   Prometheus server pulls metrics on its own schedule. This is the pull model
   and covers metrics only (not logs or traces).
2. **Push OTLP to your own collector.** Set
   `OSA_OBSERVABILITY__OTLP_ENDPOINT` (and optionally
   `OSA_OBSERVABILITY__OTLP_TOKEN`) and the node pushes all three signals
   (metrics, logs, traces) over OTLP/HTTP to a collector you run — an
   OpenTelemetry Collector, Grafana Alloy, Grafana Cloud, Honeycomb, etc.
3. **Managed hosting.** On a platform-hosted node, the `OSA_OBSERVABILITY__*`
   env vars are injected for you and telemetry lands in the platform's
   collector. No action required; the same knobs documented here still apply if
   you need to override behavior.

## Configuration reference

All settings live under the `OSA_OBSERVABILITY__*` nested-env prefix (the `__`
double-underscore is the nested delimiter; these also map to an `observability:`
block in a YAML config file).

| Env var | Type | Default | Meaning |
|---|---|---|---|
| `OSA_OBSERVABILITY__OTLP_ENDPOINT` | URL string \| unset | unset | Base OTLP/HTTP endpoint. When set, metrics, logs, and traces are pushed to `{endpoint}/v1/{signal}`. Unset disables push export entirely. |
| `OSA_OBSERVABILITY__OTLP_TOKEN` | secret string \| unset | unset | Bearer token sent as the `Authorization: Bearer <token>` header on every OTLP export. Treated as a secret — never logged. |
| `OSA_OBSERVABILITY__PROMETHEUS_ENABLED` | bool | `true` | Serve `GET /metrics` for Prometheus pull scraping. Set `false` to disable the endpoint (it then returns 404). |
| `OSA_OBSERVABILITY__TRACE_SAMPLE_RATE` | float in `[0.0, 1.0]` | `1.0` | Head sampling rate for traces. `1.0` samples everything; `0.1` keeps ~10%; `0.0` disables tracing. |

### Identity labels — `OTEL_RESOURCE_ATTRIBUTES`

Node identity is **not** an OSA-specific setting. It rides the standard
OpenTelemetry `OTEL_RESOURCE_ATTRIBUTES` env var, which the pipeline reads at
startup and stamps onto every metric, log, and span as resource attributes. Use
it to label telemetry with which node produced it:

```bash
OTEL_RESOURCE_ATTRIBUTES=osa.node_domain=archive.example.edu,deployment.environment=production
```

The `service.name` (from the node name) and `service.version` (the OSA release)
are set automatically — you do not need to add them here.

## Quick starts

### Scrape with Prometheus (default)

Nothing to configure on the OSA side — `/metrics` is served unversioned at the
root by default. Point your Prometheus at it (the server listens on `:8000`):

```yaml
# prometheus.yml
scrape_configs:
  - job_name: osa
    metrics_path: /metrics
    static_configs:
      - targets: ["osa-host:8000"]
```

`/metrics` renders standard Prometheus exposition text (`CONTENT_TYPE_LATEST`)
and exposes only OSA's own collector — not the process/platform collectors — so
you get a clean `osa_*` surface plus `http_server_request_duration_*`.

### Push OTLP to a collector

Set the endpoint (and a token if your collector requires one) in the node's
environment. Export is **OTLP/HTTP with protobuf encoding**; the per-signal
paths (`/v1/traces`, `/v1/metrics`, `/v1/logs`) are appended automatically, so
give the **base** endpoint only.

Local OpenTelemetry Collector:

```bash
OSA_OBSERVABILITY__OTLP_ENDPOINT=http://otel-collector:4318
```

Grafana Cloud (or any authenticated OTLP/HTTP backend):

```bash
OSA_OBSERVABILITY__OTLP_ENDPOINT=https://otlp-gateway-prod-us-central-0.grafana.net/otlp
OSA_OBSERVABILITY__OTLP_TOKEN=glc_eyJ...      # sent as: Authorization: Bearer glc_eyJ...
```

Both consumption modes can run together: keep `/metrics` for a local Prometheus
while also pushing traces and logs to a remote collector.

## Metric reference

Every metric name is owned by exactly one emitter, and every label value is
drawn from a bounded enum (below), so time-series cardinality stays finite. The
`_total` suffix marks counters in Prometheus.

### Hook execution

| Metric | Type | Labels | Meaning |
|---|---|---|---|
| `osa_hook_runs_total` | counter | `hook`, `status` | Completed hook executions, by hook and terminal run status. |
| `osa_hook_run_duration_seconds` | histogram | `hook`, `status` | Wall-clock duration of hook executions. |
| `osa_hook_failures_total` | counter | `hook`, `kind`, `decision` | Hook failures, by observed cause (`kind`) and the policy decision taken (`decision`). |
| `osa_hook_oom_retries_total` | counter | `hook` | Out-of-memory retries (memory bumps) performed. Only incremented when a run actually bumped memory. |

### Ingest

| Metric | Type | Labels | Meaning |
|---|---|---|---|
| `osa_ingest_batches_total` | counter | `outcome`, `kind` | Ingest batches reaching a terminal state. `outcome` ∈ `completed`\|`failed`; `kind` is the failure cause on failed batches, else `none`. |
| `osa_records_published_total` | counter | — | Records published by completed ingest batches. |
| `osa_ingest_runs_total` | counter | `status`, `kind` | Ingest runs reaching a terminal status. `kind` is the failure cause on failed runs, else `none`. |

### Outbox / event delivery

| Metric | Type | Labels | Meaning |
|---|---|---|---|
| `osa_deliveries_total` | counter | `consumer_group`, `status` | Terminal outbox dispatch-attempt outcomes, by consumer group and delivery status. |
| `osa_dispatch_duration_seconds` | histogram | `consumer_group` | Wall-clock duration of a single delivery dispatch. |

### Outbox / worker gauges (periodic sampler)

Sampled every 15 seconds by a background task in the worker pool. On PostgreSQL
these reflect live queue depth and pool pressure; the DB-pool gauges are skipped
on SQLite (no connection pool to sample).

| Metric | Type | Labels | Meaning |
|---|---|---|---|
| `osa_outbox_lag_seconds` | gauge | — | Age of the oldest *eligible* pending delivery (claimable now). `0` when the queue is empty. |
| `osa_outbox_pending` | gauge | `consumer_group` | Pending (unclaimed) deliveries per consumer group. |
| `osa_outbox_failed` | gauge | `consumer_group` | Failed deliveries per consumer group. |
| `osa_db_pool_checked_out` | gauge | — | Connections currently checked out of the SQLAlchemy pool. |
| `osa_db_pool_size` | gauge | — | Configured base size of the connection pool. |
| `osa_db_pool_overflow` | gauge | — | Overflow connections open beyond the base pool size. |
| `osa_workers_busy` | gauge | — | Worker loops currently processing a batch. |
| `osa_workers_total` | gauge | — | Total worker loops in the pool. |

### API edge

| Metric | Type | Labels | Meaning |
|---|---|---|---|
| `osa_unhandled_errors_total` | counter | — | Unhandled exceptions that reached the global error handler (surfaced as 500s). |

### HTTP (auto-instrumented)

| Metric | Type | Labels | Meaning |
|---|---|---|---|
| `http.server.request.duration` | histogram | OTel HTTP semconv (method, route, status code, …) | Request latency for every HTTP route. Emitted by the FastAPI instrumentation — no OSA code. |

### Label vocabularies

Label values come straight from these enums:

- **`status`** on `osa_hook_runs_total` / `osa_hook_run_duration_seconds`
  (`HookRunStatus`): `passed`, `warnings`, `failed`, `error`.
- **`kind`** on hook and ingest failures (`FailureKind`): `image_pull`, `rbac`,
  `config`, `oom`, `timeout`, `upstream`, `hook_exit`, `runtime`, `unknown`. On
  ingest metrics, `none` additionally marks the no-failure case.
- **`decision`** on `osa_hook_failures_total` (`DecisionKind`): `retry`,
  `retry_with_more_memory`, `give_up`, `abort_run`.
- **`status`** on `osa_ingest_runs_total` (`IngestStatus`): `pending`,
  `running`, `completed`, `failed`.
- **`outcome`** on `osa_ingest_batches_total`: `completed`, `failed`.
- **`status`** on `osa_deliveries_total` (`DeliveryStatus`): `pending`,
  `claimed`, `delivered`, `failed`, `skipped`.
- **`consumer_group`** on delivery metrics: the event-handler class name that
  processed the delivery — a fixed set of registered handlers (e.g. `RunHooks`,
  `PublishBatch`, `RunIngester`, `ConvertDepositionToRecord`).
- **`hook`**: the hook's registered name.

## Health endpoints

Two endpoints under the versioned API, both unauthenticated and excluded from
request tracing:

### `GET /api/v1/health` — liveness

Cheap liveness probe. Always returns `200` if the process is up:

```json
{ "status": "ok", "version": "1.4.2" }
```

`version` is the running OSA release (the same value stamped onto telemetry as
`service.version`).

### `GET /api/v1/ready` — readiness

Component-structured readiness. Checks the database (`SELECT 1`), the worker
pool (running with no dead workers), and the configured runner (Kubernetes
health check; reported `unchecked` for the local OCI runner). Returns `200` when
every component is healthy, `503` when any is degraded. Individual check
failures become a component error — they never surface as a 500.

```json
{
  "status": "ready",
  "version": "1.4.2",
  "components": {
    "db": { "status": "ok", "detail": null },
    "workers": { "status": "ok", "detail": null },
    "runner": { "status": "unchecked", "detail": "oci runner is not health-checked" }
  }
}
```

Each component's `status` is one of `ok`, `error`, or `unchecked`. A `degraded`
top-level status (HTTP 503) means at least one component reports `error` — wire
this endpoint to your orchestrator's readiness probe so a node with a dead DB or
worker pool is pulled from rotation.

## Traces

A trace follows one logical operation across the async boundary that the outbox
introduces. An inbound HTTP request opens a request span; when its handler
appends an event to the outbox, the request's trace context (a W3C
`traceparent`) is captured and stored on the delivery row. Later — possibly in a
different worker, out of band — the worker claims that delivery and opens a
`worker.dispatch` span, and any hook or child spans produced while handling the
event nest under it. Because events appended *during* handling capture the
dispatch span as *their* trace context in turn, multi-hop chains
(request → RunHooks → PublishBatch → …) fall out automatically: each hop links
to the previous one.

The join is modeled with **span links, not parent/child**. A single worker poll
claims a *batch* of deliveries that can originate from many different requests,
so there is no single parent to attach to — instead the dispatch span carries a
link to each originating span. NULL trace context (events created outside any
request, e.g. from the CLI or a cron loop) simply produces no link, and a
malformed `traceparent` is skipped with a warning without blocking delivery.
Sampling is head-based via `OSA_OBSERVABILITY__TRACE_SAMPLE_RATE` — the decision
is made once at the root and honored consistently down the chain.

## Security notes

- **`/metrics` is unauthenticated.** The Prometheus endpoint has no auth gate by
  design (Prometheus scraping convention). If the node is reachable from an
  untrusted network, either disable it
  (`OSA_OBSERVABILITY__PROMETHEUS_ENABLED=false`) and push OTLP to your own
  collector instead, or shield `/metrics` at your reverse proxy / ingress (allow
  only your Prometheus source, or require an auth header at the proxy). OSA's
  metric labels are bounded enums and carry no record contents, but queue depths
  and error counts are still operational information you may not want public.
- **The OTLP token is a secret.** `OSA_OBSERVABILITY__OTLP_TOKEN` is stored as a
  secret and only ever leaves the process as the `Authorization: Bearer` header
  on OTLP requests — it is never written to logs (telemetry startup logs the
  endpoint, never the token). Supply it via your secret manager / injected env,
  not in a checked-in config file.
