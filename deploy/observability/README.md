# OSA observability — dashboard & alert rules

Importable monitoring config for a self-hosted OSA node. These consume the
metrics the node already exposes — they add **no** server-side behaviour. See
[`docs/observability.md`](../../docs/observability.md) for the full metric
reference, label vocabularies, health contract, and trace model.

> This ships importable config, **not** a running stack. Bundling
> Prometheus/Grafana services into `docker-compose.yml` is intentionally out of
> scope — point your existing monitoring at the node.

## Prerequisites

The node must expose metrics. Either scrape the Prometheus endpoint
(`prometheus_enabled` is on by default):

```
GET /metrics        # Prometheus exposition, root-mounted
```

or set `OSA_OBSERVABILITY__OTLP_ENDPOINT` to push metrics to your own collector.
Metric names below follow the OpenTelemetry Prometheus exporter's normalization
(dots → underscores, unit suffix appended).

## `grafana-dashboard.json`

Import into Grafana: **Dashboards → New → Import → Upload JSON file**, then pick
your Prometheus data source when prompted (the dashboard uses a `$datasource`
variable — no data-source UID is hardcoded). A `$hook` variable filters the
per-hook panels.

Rows: **Node health & API edge**, **Outbox & queue health**, **Workflow
stages**, **Hook execution**, **Ingest throughput**, **Runtime resources**.

## `alert-rules.yml`

Prometheus alerting rules. Reference the file from your Prometheus config:

```yaml
# prometheus.yml
rule_files:
  - /etc/prometheus/osa-alert-rules.yml
```

Validate before loading:

```bash
promtool check rules deploy/observability/alert-rules.yml
```

Thresholds and `for:` durations are **starting points** — tune them to your
node's traffic and SLOs before paging on them.

## Reading the metrics correctly (#160)

Two footguns are baked into the rules and panels so you don't hit them:

- **`osa_dispatch_duration_seconds` times a whole workflow**, not a single hop —
  a `ProcessBatch` dispatch can span minutes to hours. For per-stage latency,
  read the `workflow.stage` spans, not this histogram.
- **`osa_deliveries_total{status="failed"}` counts failed *attempts*, not
  terminal failures.** One retry budget spans the whole workflow and
  redeliveries fast-forward past cleared stages, so attempt-failures inflate.
  For "did a run/batch actually fail", read the domain counters
  (`osa_ingest_runs_total{status="failed"}`,
  `osa_ingest_batches_total{outcome="failed"}`).

## Security

`/metrics` is unauthenticated by design. If the node is reachable from an
untrusted network, disable it and push OTLP instead, or shield it at your
reverse proxy — see the security notes in `docs/observability.md`.
