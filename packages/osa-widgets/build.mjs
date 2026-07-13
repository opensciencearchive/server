/**
 * Build script: bundle each widget entry into a single self-contained HTML
 * document under dist/. Widgets render inside a sandboxed iframe with a
 * default-deny CSP (no external network), so ALL JS and CSS must be inlined.
 */

import { build } from "esbuild";
import { mkdir, readFile, writeFile } from "node:fs/promises";
import path from "node:path";
import { fileURLToPath } from "node:url";

const root = path.dirname(fileURLToPath(import.meta.url));
const distDir = path.join(root, "dist");

const WIDGETS = ["dataset-overview", "table", "chart", "record", "filter-panel"];

const TITLES = {
  "dataset-overview": "OSA Datasets",
  table: "OSA Table",
  chart: "OSA Chart",
  record: "OSA Record",
  "filter-panel": "OSA Filters",
};

/**
 * A literal "</script>" inside the inlined bundle would terminate the HTML
 * script element early and break the document; escape the closing tag.
 */
function escapeScript(js) {
  return js.replaceAll("</script", "<\\/script");
}

function htmlDocument(name, css, js) {
  return `<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>${TITLES[name]}</title>
<style>
${css}
</style>
</head>
<body>
<div id="root"></div>
<script>
${escapeScript(js)}
</script>
</body>
</html>
`;
}

/** The sandbox CSP denies all network fetches; a bundle that references an
 *  http(s) URL via src=/href= would render broken. Fail the build instead. */
function assertSelfContained(name, html) {
  const external = html.match(/(?:src|href)\s*=\s*["']https?:\/\/[^"']*/gi);
  if (external) {
    throw new Error(
      `dist/${name}.html references external resources: ${external.join(", ")}`,
    );
  }
}

async function bundleWidget(name, css) {
  const entry = path.join(root, "src", "widgets", name, "index.tsx");
  const result = await build({
    entryPoints: [entry],
    bundle: true,
    write: false,
    format: "iife",
    minify: true,
    target: "es2020",
    jsx: "automatic",
    loader: { ".css": "text" },
    define: { "process.env.NODE_ENV": '"production"' },
    legalComments: "none",
  });
  const js = result.outputFiles[0].text;
  const html = htmlDocument(name, css, js);
  assertSelfContained(name, html);
  const outPath = path.join(distDir, `${name}.html`);
  await writeFile(outPath, html, "utf8");
  return { name, bytes: Buffer.byteLength(html, "utf8") };
}

const css = await readFile(path.join(root, "src", "styles", "base.css"), "utf8");
await mkdir(distDir, { recursive: true });

const sizes = [];
for (const name of WIDGETS) {
  sizes.push(await bundleWidget(name, css));
}

console.log("\nBundle sizes:");
for (const { name, bytes } of sizes) {
  console.log(`  dist/${name}.html  ${(bytes / 1024).toFixed(1)} KB`);
}
