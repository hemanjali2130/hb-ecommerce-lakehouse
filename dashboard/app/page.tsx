import Dashboard from "@/components/Dashboard";

// Never prerendered at build time — this page reports live pipeline state, and a
// build-time snapshot would show whatever was true when it was deployed.
export const dynamic = "force-dynamic";

export default function Page() {
  return <Dashboard />;
}
