import { LayoutDashboard, Plus, FileText, Download, Settings } from "lucide-react";
import { NavLink } from "react-router-dom";
import {
  Sidebar, SidebarContent, SidebarGroup, SidebarGroupContent,
  SidebarGroupLabel, SidebarMenu, SidebarMenuButton, SidebarMenuItem, useSidebar,
} from "@/components/ui/sidebar";
import { useCreateTaskDialog } from "@/providers/create-task-dialog-provider";

const items = [
  { title: "Dashboard", url: "/", icon: LayoutDashboard, type: "link" as const },
  { title: "Create Task", icon: Plus, type: "action" as const }, // <- action, no url
  { title: "Task History", url: "/history", icon: FileText, type: "link" as const },
  { title: "Exports", url: "/exports", icon: Download, type: "link" as const },
];

export function AppSidebar() {
  const { state } = useSidebar();
  const isCollapsed = state === "collapsed";
  const { openCreateTask } = useCreateTaskDialog();

  const getNavCls = ({ isActive }: { isActive: boolean }) =>
    isActive
      ? "bg-primary text-primary-foreground font-medium"
      : "hover:bg-accent/50 text-muted-foreground hover:text-foreground";

  return (
    <Sidebar className={isCollapsed ? "w-14" : "w-64"} collapsible="icon">
      <SidebarContent className="bg-card border-r">
        <div className="p-4 border-b">
          <h2 className={`font-semibold text-lg text-foreground ${isCollapsed ? 'hidden' : 'block'}`}>
            Admin Dashboard
          </h2>
          {isCollapsed && <div className="w-6 h-6 bg-primary rounded-md" />}
        </div>

        <SidebarGroup>
          <SidebarGroupLabel className={isCollapsed ? 'hidden' : 'block'}>
            Main Navigation
          </SidebarGroupLabel>
          <SidebarGroupContent>
            <SidebarMenu className="space-y-1">
              {items.map((item) => (
                <SidebarMenuItem key={item.title}>
                  {item.type === "link" ? (
                    <SidebarMenuButton asChild className="w-full">
                      <NavLink
                        to={item.url!}
                        end
                        className={({ isActive }) =>
                          `flex items-center px-3 py-2 rounded-md transition-colors ${getNavCls({ isActive })}`
                        }
                      >
                        <item.icon className={`h-4 w-4 ${isCollapsed ? '' : 'mr-3'}`} />
                        {!isCollapsed && <span>{item.title}</span>}
                      </NavLink>
                    </SidebarMenuButton>
                  ) : (
                    <SidebarMenuButton
                      className="w-full flex items-center px-3 py-2 rounded-md transition-colors hover:bg-accent/50 text-muted-foreground hover:text-foreground"
                      onClick={openCreateTask}
                    >
                      <item.icon className={`h-4 w-4 ${isCollapsed ? '' : 'mr-3'}`} />
                      {!isCollapsed && <span>{item.title}</span>}
                    </SidebarMenuButton>
                  )}
                </SidebarMenuItem>
              ))}
            </SidebarMenu>
          </SidebarGroupContent>
        </SidebarGroup>
      </SidebarContent>
    </Sidebar>
  );
}
