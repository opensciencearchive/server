import type { Metadata } from "next";
import { DM_Serif_Display, Inter, JetBrains_Mono } from "next/font/google";

import "./globals.css";

import { Providers } from "./providers";

const displayFont = DM_Serif_Display({
  weight: "400",
  style: ["normal", "italic"],
  subsets: ["latin"],
  variable: "--next-font-display",
});

const bodyFont = Inter({
  subsets: ["latin"],
  variable: "--next-font-body",
});

const monoFont = JetBrains_Mono({
  subsets: ["latin"],
  variable: "--next-font-mono",
});

export const metadata: Metadata = {
  title: {
    default: "Amacrin Console",
    template: "%s · Amacrin Console",
  },
  description: "Manage hosted OSA archives on Amacrin Cloud.",
};

export default function RootLayout({
  children,
}: Readonly<{ children: React.ReactNode }>) {
  return (
    <html
      lang="en"
      className={`${displayFont.variable} ${bodyFont.variable} ${monoFont.variable}`}
    >
      <body>
        <Providers>{children}</Providers>
      </body>
    </html>
  );
}
