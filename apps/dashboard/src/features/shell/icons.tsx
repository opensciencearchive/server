/** Minimal 15×15 stroke icons for the sidebar, in the mockup's line style. */

function Icon({ children }: { children: React.ReactNode }) {
  return (
    <svg
      width="15"
      height="15"
      viewBox="0 0 15 15"
      fill="none"
      stroke="currentColor"
      strokeWidth="1.2"
      strokeLinecap="round"
      strokeLinejoin="round"
      aria-hidden
    >
      {children}
    </svg>
  );
}

export const icons = {
  organisations: (
    <Icon>
      <circle cx="5" cy="5" r="2.2" />
      <circle cx="10.5" cy="6.5" r="1.8" />
      <path d="M1.5 12.5c0-2 1.6-3.4 3.5-3.4s3.5 1.4 3.5 3.4M9.5 12.5c0-1.7 1-2.9 2.8-2.9 .5 0 .9.1 1.2.3" />
    </Icon>
  ),
  account: (
    <Icon>
      <circle cx="7.5" cy="4.8" r="2.4" />
      <path d="M2.8 12.8c0-2.4 2.1-4 4.7-4s4.7 1.6 4.7 4" />
    </Icon>
  ),
  overview: (
    <Icon>
      <path d="M2 6.5 7.5 2 13 6.5V13H9.5V9.5h-4V13H2Z" />
    </Icon>
  ),
  records: (
    <Icon>
      <ellipse cx="7.5" cy="3.5" rx="5" ry="2" />
      <path d="M2.5 3.5v8c0 1.1 2.2 2 5 2s5-.9 5-2v-8M2.5 7.5c0 1.1 2.2 2 5 2s5-.9 5-2" />
    </Icon>
  ),
  agents: (
    <Icon>
      <path d="M7 1.8l1.2 3.5 3.5 1.2-3.5 1.2L7 11.2 5.8 7.7 2.3 6.5l3.5-1.2z" />
      <path d="M11.6 9.6l.5 1.5 1.5.5-1.5.5-.5 1.5-.5-1.5-1.5-.5 1.5-.5z" />
    </Icon>
  ),
  hooks: (
    <Icon>
      <path d="M1.5 7.5h2.5l1.5-4 2.5 8 1.5-4h4" />
    </Icon>
  ),
  ingesters: (
    <Icon>
      <path d="M7.5 2v7M4.5 6.5l3 3 3-3M2.5 12.5h10" />
    </Icon>
  ),
  builds: (
    <Icon>
      <path d="M7.5 1.8 13 4.5v6L7.5 13.2 2 10.5v-6Z" />
      <path d="M2 4.5l5.5 2.7L13 4.5M7.5 7.2v6" />
    </Icon>
  ),
  observability: (
    <Icon>
      <path d="M2 13V8.5M5.7 13V5M9.3 13V9.8M13 13V3" />
    </Icon>
  ),
  authentication: (
    <Icon>
      <rect x="3" y="6.5" width="9" height="6" rx="1" />
      <path d="M5 6.5V4.8a2.5 2.5 0 0 1 5 0v1.7" />
    </Icon>
  ),
  settings: (
    <Icon>
      <circle cx="7.5" cy="7.5" r="2" />
      <path d="M7.5 1.5v2M7.5 11.5v2M1.5 7.5h2M11.5 7.5h2M3.3 3.3l1.4 1.4M10.3 10.3l1.4 1.4M11.7 3.3l-1.4 1.4M4.7 10.3l-1.4 1.4" />
    </Icon>
  ),
  members: (
    <Icon>
      <circle cx="5.2" cy="5" r="2.2" />
      <path d="M1.5 12.5c0-2 1.7-3.5 3.7-3.5s3.7 1.5 3.7 3.5" />
      <path d="M10 3.2a2.2 2.2 0 0 1 0 3.9M11.2 9.2c1.4.5 2.3 1.7 2.3 3.3" />
    </Icon>
  ),
  archives: (
    <Icon>
      <rect x="2" y="2.5" width="11" height="3.5" rx="0.8" />
      <path d="M3 6v5.5a1 1 0 0 0 1 1h7a1 1 0 0 0 1-1V6M6 8.5h3" />
    </Icon>
  ),
} satisfies Record<string, React.ReactNode>;

export type IconName = keyof typeof icons;
