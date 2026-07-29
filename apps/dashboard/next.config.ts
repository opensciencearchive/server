import type { NextConfig } from "next";

const nextConfig: NextConfig = {
  // Emit a self-contained Node server (`.next/standalone/server.js`) so the
  // dashboard ships as one container image and runs as a Node BFF (route
  // handlers hold the archive token server-side). See issue #173.
  output: "standalone",
};

export default nextConfig;
