import { Suspense } from "react";
import DashboardShell from "@/components/layout/DashboardShell";
import { DashboardProvider } from "@/components/dashboard/DashboardContext";

export default function Page() {
  return (
    <Suspense fallback={null}>
      <DashboardProvider>
        <DashboardShell />
      </DashboardProvider>
    </Suspense>
  );
}
