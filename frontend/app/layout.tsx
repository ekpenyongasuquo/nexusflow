import type { Metadata } from "next";
import "./globals.css";

export const metadata: Metadata = {
  title: "NexusFlow — Autonomous Decision Intelligence",
  description: "Autonomous Decision Intelligence for the Distributed Enterprise",
};

export default function RootLayout({
  children,
}: {
  children: React.ReactNode;
}) {
  return (
    <html lang="en">
      <body className="bg-slate-950 text-slate-100 min-h-screen font-sans antialiased">
        {children}
      </body>
    </html>
  );
}
