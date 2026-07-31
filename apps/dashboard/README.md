# Amacrin Console (`apps/dashboard`)

The management UI for OSA Cloud — Next.js 16 (App Router) + React 19, deployed on
Vercel at `https://console.amacrin.com`. It is an API client of the platform API at
`https://api.amacrin.com`; no server-side secrets live here.

## Commands

```bash
pnpm install
pnpm dev        # dev server (reads NEXT_PUBLIC_API_MODE; AMACRIN_API_URL at runtime)
pnpm test       # vitest (watch); pnpm test:run for one-shot
pnpm typecheck  # tsc --noEmit
pnpm lint       # eslint (incl. layer-boundary rules)
pnpm build      # next build
```

## Data modes (`NEXT_PUBLIC_API_MODE`)

- `real` (default) — talk to the control plane through the same-origin BFF proxy
  (`/api/amacrin`), which forwards to `AMACRIN_API_URL` server-side.
- `mock` — fully in-memory services, no network. Used for Vercel preview demos and
  the CI `next build`. Walk the whole app without a backend or Google credentials.
- `msw` — real services with Mock Service Worker intercepting requests against the
  fixtures in `src/mocks/`.

## Architecture

Layered, one-way `app → features → api → domain`, enforced by ESLint boundary rules:

- `src/app/` — routes only (route groups `(public)` / `(app)`), thin pages that
  compose features.
- `src/features/` — one directory per domain concept: components + TanStack Query
  hooks + query-key factories.
- `src/api/` — the only layer that does HTTP. Wire DTO schemas + decoders
  (`api/amacrin/wire/`) are an anti-corruption layer: wire shapes never leak past
  the decoders. `AmacrinService` (cloud) and `OSAService` (tenant) are interfaces
  with real + mock implementations.
- `src/domain/` — pure types and functions; imports nothing else. Status machines
  are discriminated unions (`ArchiveStatus`, `DeploymentStatus`, `BuildStatus`, …).
- `src/ui/` — dumb primitives (CSS Modules over `src/styles/tokens.css`).

### Real vs mock data

Everything in the design is built. Data with **no backing API yet** is typed
`Mocked<T>` (`src/domain/mocked.ts`) and rendered with a `<SampleDataChip/>`. This
covers all tenant-instance data (records, validation, usage — the dashboard has no
auth path to tenant OSA instances yet) plus a few platform-shaped gaps (builds
list, deployment history, org members). When a real endpoint ships, delete the
`Mocked<…>` wrapper from the method signature — every affected call site becomes a
compile error, so the migration is mechanical.

`AmacrinService`'s non-`Mocked` methods map 1:1 to routes on the Rust server; that
mapping is the contract the decoders and MSW handlers mirror.
