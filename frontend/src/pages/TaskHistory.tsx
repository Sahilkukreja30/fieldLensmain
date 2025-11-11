import { useState, useEffect, useMemo } from "react";
import { Button } from "@/components/ui/button";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Input } from "@/components/ui/input";
import { Badge } from "@/components/ui/badge";
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from "@/components/ui/select";
import { ArrowLeft, Search, Filter } from "lucide-react";
import { useNavigate } from "react-router-dom";
import { fetchJobs, type BackendJob } from "@/lib/api";

type TaskStatus = "PENDING" | "IN_PROGRESS" | "DONE" | "FAILED";
type StatusFilter = "all" | "completed" | "failed" | "pending" | "processing";

const statusFilterMap: Record<Exclude<StatusFilter, "all">, TaskStatus> = {
  completed: "DONE",
  failed: "FAILED",
  pending: "PENDING",
  processing: "IN_PROGRESS",
};
const statusClasses: Record<TaskStatus, string> = {
  PENDING: "bg-warning text-warning-foreground",
  IN_PROGRESS: "bg-info text-info-foreground",
  DONE: "bg-success text-success-foreground",
  FAILED: "bg-destructive text-destructive-foreground",
};
function prettyStatus(s: TaskStatus) {
  switch (s) {
    case "PENDING": return "Pending";
    case "IN_PROGRESS": return "In Progress";
    case "DONE": return "Completed";
    case "FAILED": return "Failed";
  }
}

export default function TaskHistory() {
  const [jobs, setJobs] = useState<BackendJob[]>([]);
  const [loading, setLoading] = useState(false);
  const [err, setErr] = useState<string | null>(null);
  const [statusFilter, setStatusFilter] = useState<StatusFilter>("all");
  const [searchQuery, setSearchQuery] = useState("");
  const navigate = useNavigate();

  useEffect(() => {
    (async () => {
      setLoading(true);
      setErr(null);
      try {
        const data = await fetchJobs();
        setJobs(data);
      } catch (e: any) {
        setErr(e?.message ?? "Failed to load jobs");
      } finally {
        setLoading(false);
      }
    })();
  }, []);

  const uiTasks = useMemo(() => {
    const toIsoCreated = (j: BackendJob) => {
      if (j.createdAt) return j.createdAt;
      try {
        const secs = parseInt(j.id.slice(0, 8), 16);
        return new Date(secs * 1000).toISOString();
      } catch {
        return new Date().toISOString();
      }
    };
    const toUpper = (s: string) => (s ? s.toUpperCase() : s);

    return jobs.map((j) => ({
      id: j.id,
      title: `Job • ${j.workerPhone}`,
      phoneNumber: j.workerPhone,
      status: toUpper(j.status) as TaskStatus,
      createdAt: toIsoCreated(j),
      siteId: (j as any)?.siteId,
      sectors: (j as any)?.sectors as number[] | undefined,
    }));
  }, [jobs]);

  const filteredTasks = useMemo(() => {
    let list = uiTasks;
    if (statusFilter !== "all") {
      const wantedStatus: TaskStatus = statusFilterMap[statusFilter as Exclude<StatusFilter, "all">];
      list = list.filter((t) => t.status === wantedStatus);
    }
    const q = searchQuery.trim().toLowerCase();
    if (q) {
      list = list.filter(
        (t) =>
          t.title.toLowerCase().includes(q) ||
          t.phoneNumber.toLowerCase().includes(q) ||
          (t.siteId || "").toLowerCase().includes(q) ||
          t.id.toLowerCase().includes(q)
      );
    }
    return list;
  }, [uiTasks, searchQuery, statusFilter]);

  return (
    <div className="space-y-6">
      <div className="flex flex-col gap-3 sm:flex-row sm:items-center sm:gap-4">
        <Button variant="ghost" size="sm" onClick={() => navigate("/")} className="gap-2 w-fit">
          <ArrowLeft className="w-4 h-4" />
          Back to Dashboard
        </Button>
        <div>
          <h1 className="text-2xl font-semibold text-foreground">Task History</h1>
          <p className="text-muted-foreground">View completed and past site+sector jobs</p>
        </div>
      </div>

      <Card>
        <CardHeader>
          <CardTitle className="flex items-center gap-2">
            <Filter className="w-5 h-5" />
            Filters
          </CardTitle>
        </CardHeader>
        <CardContent>
          <div className="grid grid-cols-1 gap-3 md:grid-cols-3">
            <div className="relative md:col-span-2">
              <Search className="absolute left-3 top-3 h-4 w-4 text-muted-foreground" />
              <Input
                placeholder="Search by job ID, phone or site ID..."
                value={searchQuery}
                onChange={(e) => setSearchQuery(e.target.value)}
                className="pl-10"
              />
            </div>
            <Select value={statusFilter} onValueChange={(v) => setStatusFilter(v as StatusFilter)}>
              <SelectTrigger className="w-full">
                <SelectValue placeholder="Filter by status" />
              </SelectTrigger>
              <SelectContent>
                <SelectItem value="all">All Status</SelectItem>
                <SelectItem value="completed">Done</SelectItem>
                <SelectItem value="failed">Failed</SelectItem>
                <SelectItem value="pending">Pending</SelectItem>
                <SelectItem value="processing">In Progress</SelectItem>
              </SelectContent>
            </Select>
          </div>
        </CardContent>
      </Card>

      <Card>
        <CardHeader>
          <CardTitle>Job History ({filteredTasks.length} jobs)</CardTitle>
        </CardHeader>
        <CardContent>
          <div className="space-y-4">
            {filteredTasks.map((task) => (
              <div key={task.id} className="border rounded-lg p-4 hover:bg-accent/50 transition-colors">
                <div className="flex w-full items-start gap-3 sm:items-center">
                  <h3 className="font-medium text-foreground min-w-0 truncate">{task.title}</h3>
                  <Badge className={`${statusClasses[task.status]} ml-auto`}>
                    {prettyStatus(task.status)}
                  </Badge>
                </div>

                <div className="mt-3 grid grid-cols-1 gap-2 text-sm text-muted-foreground sm:grid-cols-2 lg:grid-cols-4">
                  <div className="break-all">
                    <span className="font-medium">Job Id:</span> {task.id}
                  </div>
                  <div className="break-all">
                    <span className="font-medium">Phone:</span> {task.phoneNumber}
                  </div>
                  <div className="break-all">
                    <span className="font-medium">Site ID:</span> {task.siteId ?? "—"}
                  </div>
                  <div className="col-span-1 sm:col-auto">
                    <span className="font-medium">Created:</span> {new Date(task.createdAt).toLocaleDateString()}
                  </div>
                  {Array.isArray(task.sectors) && task.sectors.length > 0 && (
                    <div className="col-span-1 sm:col-auto">
                      <span className="font-medium">Sectors:</span> {task.sectors.sort((a,b)=>a-b).join(", ")}
                    </div>
                  )}
                </div>
              </div>
            ))}

            {filteredTasks.length === 0 && (
              <div className="text-center py-12">
                <p className="text-muted-foreground">No tasks found matching your criteria.</p>
              </div>
            )}
          </div>
        </CardContent>
      </Card>
    </div>
  );
}
