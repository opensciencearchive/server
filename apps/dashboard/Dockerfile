# syntax=docker/dockerfile:1

# Stage 1: Dependencies
FROM node:22-alpine AS deps

RUN corepack enable && corepack prepare pnpm@11.5.1 --activate

WORKDIR /app

# pnpm-workspace.yaml carries the build-script approvals (allowBuilds); without
# it pnpm 11 fails the install with ERR_PNPM_IGNORED_BUILDS.
COPY package.json pnpm-lock.yaml pnpm-workspace.yaml ./
RUN --mount=type=cache,target=/root/.local/share/pnpm/store \
    pnpm install --frozen-lockfile

# Stage 2: Builder
FROM node:22-alpine AS builder

RUN corepack enable && corepack prepare pnpm@11.5.1 --activate

WORKDIR /app

COPY --from=deps /app/node_modules ./node_modules
COPY . .

# Build-time (NEXT_PUBLIC_*) flags are inlined into the client bundle, so they
# must be set here. The self-hosted image is a fixed identity: project-level
# pages only, real data via the same-origin BFF proxy.
ARG NEXT_PUBLIC_IS_PLATFORM=false
ARG NEXT_PUBLIC_API_MODE=real
ENV NEXT_PUBLIC_IS_PLATFORM=${NEXT_PUBLIC_IS_PLATFORM}
ENV NEXT_PUBLIC_API_MODE=${NEXT_PUBLIC_API_MODE}

RUN pnpm build

# Stage 3: Runtime
FROM node:22-alpine AS runtime

WORKDIR /app

# Create non-root user
RUN addgroup --system --gid 1001 nodejs && \
    adduser --system --uid 1001 nextjs

# Copy built standalone output
COPY --from=builder /app/public ./public
COPY --from=builder --chown=nextjs:nodejs /app/.next/standalone ./
COPY --from=builder --chown=nextjs:nodejs /app/.next/static ./.next/static

USER nextjs

EXPOSE 3000

ENV PORT=3000
ENV HOSTNAME="0.0.0.0"

# Server-only config is read at RUNTIME by the Node route handlers — never baked
# into the image. Self-host: OSA_API_URL, JWT_SECRET, SESSION_SECRET, DASHBOARD_*.
# Platform (#185): AMACRIN_API_URL (in-cluster control-plane base) + optional
# AMACRIN_PUBLIC_URL (browser-facing OAuth origin), SESSION_SECRET.

CMD ["node", "server.js"]
