import type { Metadata } from "next";

import "./globals.css";

export const metadata: Metadata = {
  title: "hb-ecommerce-lakehouse — pipeline observability",
  description:
    "Live throughput, data freshness, quarantine breakdown and pipeline status for a serverless AWS lakehouse. Built by Hemanjali Buchireddy.",
};

export default function RootLayout({ children }: { children: React.ReactNode }) {
  return (
    <html lang="en">
      <body className="min-h-screen antialiased">{children}</body>
    </html>
  );
}
