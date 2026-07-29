import nextCoreWebVitals from "eslint-config-next/core-web-vitals";
import nextTypescript from "eslint-config-next/typescript";

/**
 * Layer boundaries (one-way): app → features → api → domain.
 * `ui/` is importable from anywhere above it but imports nothing from src
 * except itself. Wire DTO schemas (`api/amacrin/wire`) and MSW fixtures
 * never leak outside their owning layer.
 */
const layerZones = [
  // domain is pure: it imports nothing else under src/
  { target: "./src/domain", from: "./src/app" },
  { target: "./src/domain", from: "./src/features" },
  { target: "./src/domain", from: "./src/api" },
  { target: "./src/domain", from: "./src/ui" },
  { target: "./src/domain", from: "./src/mocks" },
  // ui primitives are dumb: no data layers
  { target: "./src/ui", from: "./src/app" },
  { target: "./src/ui", from: "./src/features" },
  { target: "./src/ui", from: "./src/api" },
  { target: "./src/ui", from: "./src/domain" },
  { target: "./src/ui", from: "./src/mocks" },
  // api never reaches up
  { target: "./src/api", from: "./src/app" },
  { target: "./src/api", from: "./src/features" },
  // features never reach up into routes
  { target: "./src/features", from: "./src/app" },
  // wire DTO shapes stay quarantined inside api/
  {
    target: [
      "./src/app",
      "./src/features",
      "./src/domain",
      "./src/ui",
      "./src/mocks",
    ],
    from: "./src/api/amacrin/wire",
  },
  // MSW fixtures (wire-shaped) stay inside mocks/
  {
    target: [
      "./src/app",
      "./src/features",
      "./src/domain",
      "./src/ui",
      "./src/api",
    ],
    from: "./src/mocks/fixtures",
  },
];

const eslintConfig = [
  ...nextCoreWebVitals,
  ...nextTypescript,
  {
    files: ["src/**/*.{ts,tsx}"],
    rules: {
      "import/no-restricted-paths": ["error", { zones: layerZones }],
    },
  },
  {
    // Tests may cross layers (fixtures, MSW server, factories).
    files: ["src/**/*.test.{ts,tsx}", "src/test/**", "src/mocks/**"],
    rules: {
      "import/no-restricted-paths": "off",
    },
  },
  {
    ignores: [".next/**", "node_modules/**", "next-env.d.ts"],
  },
];

export default eslintConfig;
