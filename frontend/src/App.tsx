// src/App.tsx
import { Toaster } from "@/components/ui/toaster";
import { Toaster as Sonner } from "@/components/ui/sonner";
import { TooltipProvider } from "@/components/ui/tooltip";
import { SidebarProvider, SidebarTrigger } from "@/components/ui/sidebar";
import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { BrowserRouter, Routes, Route, Outlet } from "react-router-dom";
import { AppSidebar } from "./components/AppSidebar";
import Dashboard from "./pages/Dashboard";
import CreateTask from "./components/CreateTask";
import TaskHistory from "./pages/TaskHistory";
import Exports from "./pages/Exports";
import NotFound from "./pages/NotFound";
import { ThemeProvider } from "./components/theme-provider";
import { ModeToggle } from "./components/toggle";
import ProtectedRoute from "./auth/ProtectedRoute";
import { AuthProvider, useAuth } from "./auth/AuthProvider";
import { CreateTaskDialogProvider } from "@/providers/create-task-dialog-provider";
import Login from "./pages/Login";
import { Button } from "./components/ui/button";
import { useNavigate, useLocation } from "react-router-dom";
const queryClient = new QueryClient();

function ProtectedLayout() {
  const nav = useNavigate();
  const { logout } = useAuth();
  const handleLogout = async () => {
    try {
      await logout();
      // nav("/login", { replace: true });
    } catch (e: any) {
      console.error(e?.response?.data?.detail ?? "Logout failed");
    }
  }
  return (
    <div className="flex w-full min-h-screen">
      <AppSidebar />

      <div className="flex-1 flex flex-col">
        <header
          className="
        border-b bg-card/50 backdrop-blur-sm
        flex flex-wrap items-center gap-3 sm:gap-4
        px-3 sm:px-6 py-2
      "
        >
          {/* Left: sidebar trigger + brand */}
          <div className="flex items-center gap-2 sm:gap-3 min-w-0">
            <SidebarTrigger className="-ml-1 hidden xs:inline-flex" />
            <div className="flex items-center gap-2 min-w-0">
              <div className="w-6 h-6 bg-gradient-primary rounded-md shrink-0" />
              <span className="font-semibold text-foreground truncate">
                Automation Hub
              </span>
            </div>
          </div>

          {/* Right: actions (mode + logout) — push right on >=sm */}
          <div className="ml-auto flex items-center gap-2 sm:gap-3">
            <ModeToggle />
            <Button
              onClick={handleLogout}
              size="sm"
              className="h-8 px-3"
            >
              Log Out
            </Button>
          </div>
        </header>

        <main className="flex-1 p-3 sm:p-6">
          <Outlet />
        </main>
      </div>
    </div>

  );
}

export default function App() {
  return (
    <ThemeProvider defaultTheme="system" storageKey="ui-theme">
      <CreateTaskDialogProvider>
        <QueryClientProvider client={queryClient}>
          <TooltipProvider>
            <Toaster />
            <Sonner />
            <BrowserRouter>
              <AuthProvider>
                <SidebarProvider>
                  <Routes>
                    {/* Public */}
                    <Route path="/login" element={<Login />} />

                    {/* Private */}
                    <Route element={<ProtectedRoute />}>
                      <Route element={<ProtectedLayout />}>
                        {/* index === "/"  -> if not logged, ProtectedRoute redirects to /login */}
                        <Route index element={<Dashboard />} />
                        <Route path="/history" element={<TaskHistory />} />
                        <Route path="/exports" element={<Exports />} />
                        <Route path="*" element={<NotFound />} />
                      </Route>
                    </Route>
                  </Routes>
                </SidebarProvider>
              </AuthProvider>
            </BrowserRouter>
          </TooltipProvider>
        </QueryClientProvider>
      </CreateTaskDialogProvider>
    </ThemeProvider>
  );
}
