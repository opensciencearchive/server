# Releasing OSA Server

Open Science Archive (OSA) server images are published to
`ghcr.io/opensciencearchive/osa`. Two kinds of tag exist:

- **`sha-<short>`** — every merge to `main` produces one. Auditable but
  not semantically meaningful. Useful for pinning to a specific commit
  during development.
- **`vX.Y.Z`** — produced when a GitHub Release is published with a
  semver-shaped tag. This is the tag downstream deployments should pin.

There is intentionally **no floating `:latest` tag**. OSA is consumed via
a compose file with a pinned `OSA_IMAGE_TAG`, not via ad-hoc `docker pull`.

## Cutting a release

1. Ensure `main` is green — release-triggered builds do not gate on CI,
   so a bad commit will publish a bad image. Check
   `gh run list --workflow ci.yml --branch main --limit 1` first.

2. Pick the next semver from the last release:
   ```sh
   gh release list --limit 1
   ```
   Bump patch for fixes, minor for backwards-compatible features, major
   for breaking changes.

3. Sync `server/pyproject.toml`'s `version` field to match the chosen
   tag (without the `v` prefix). Commit on `main`.

4. Cut the release from `main`:
   ```sh
   gh release create vX.Y.Z \
     --target main \
     --title "vX.Y.Z" \
     --generate-notes
   ```

5. Watch the image build:
   ```sh
   gh run watch
   ```
   The image lands at `ghcr.io/opensciencearchive/osa:vX.Y.Z` within
   ~5 minutes. The full commit SHA is embedded in the image's
   `org.opencontainers.image.revision` label for traceability.

6. Update downstream deployments (cultivarium pilot, etc.) to bump
   `OSA_IMAGE_TAG` to the new version.

## Verifying a published image

```sh
docker inspect ghcr.io/opensciencearchive/osa:vX.Y.Z \
  --format '{{json .Config.Labels}}' | jq
```

You should see `org.opencontainers.image.version=vX.Y.Z` and a
`revision` matching the commit you released from.
