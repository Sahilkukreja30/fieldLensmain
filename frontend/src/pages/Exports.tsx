import { useState, useEffect, useMemo } from "react";
import { Button } from "@/components/ui/button";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Badge } from "@/components/ui/badge";
import { ArrowLeft, Download, Archive, Calendar, FileText } from "lucide-react";
import { useNavigate } from "react-router-dom";
import { BackendJob, fetchJobs,downloadJobZip } from "@/lib/api";
import { useToast } from "@/hooks/use-toast";
type TaskStatus = "DONE";


export default function Exports() {
  const navigate = useNavigate();

  const [jobs, setJobs] = useState<BackendJob[]>([]);
  const [loading, setLoading] = useState(false);
  const [err, setErr] = useState<string | null>(null);

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
      // fallback from Mongo ObjectId
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
    }));
  }, [jobs]);

  const filteredTasks = useMemo(() => {
    return uiTasks.filter((t) => t.status === "DONE");
  }, [uiTasks]);

  const [downloads] = useState<Record<string, boolean>>({});

  const { toast } = useToast();
  const [downloading, setDownloading] = useState(false);

  const handleExport = async (taskId: string) => {
    setDownloading(true);
    toast({ title: "Export started", description: `Preparing ZIP for ${taskId}...` });
    try {
      await downloadJobZip(taskId);
      toast({ title: "Export complete", description: "ZIP downloaded." });
    } catch (e: any) {
      toast({
        title: "Export failed",
        description: e?.message ?? "Unknown error",
        variant: "destructive",
      });
    } finally {
      setDownloading(false);
    }
  };

  return (
    <div className="space-y-6">
      {/* Header */}
      <div className="flex items-center gap-4">
        <Button variant="ghost" size="sm" onClick={() => navigate("/")} className="gap-2">
          <ArrowLeft className="w-4 h-4" />
          Back to Dashboard
        </Button>
        <div>
          <h1 className="text-2xl font-semibold text-foreground">Export Files</h1>
          <p className="text-muted-foreground">Download and manage exported task files</p>
        </div>
      </div>

      {/* Errors / Loading (optional simple states) */}
      {err && (
        <Card>
          <CardContent className="py-4 text-destructive">{err}</CardContent>
        </Card>
      )}
      {loading && (
        <Card>
          <CardContent className="py-4 text-muted-foreground">Loading exports…</CardContent>
        </Card>
      )}

      <Card>
        <CardHeader>
          <CardTitle>Available Exports</CardTitle>
        </CardHeader>
        <CardContent>
          <div className="space-y-4">
            {filteredTasks.map((exportItem) => (
              <div
                key={exportItem.id}
                className="border rounded-lg p-4 hover:bg-accent/50 transition-colors"
              >
                <div className="flex items-start justify-between">
                  <div className="space-y-2">
                    <div className="flex items-center gap-3">
                      <h3 className="font-medium text-foreground">{exportItem.title}</h3>
                      <Badge className="bg-success text-success-foreground">
                        Completed
                      </Badge>
                    </div>

                    <div className="grid grid-cols-2 md:grid-cols-4 gap-4 text-sm text-muted-foreground">
                      <div>
                        <span className="font-medium">JOB ID:</span> {exportItem.id}
                      </div>
                    </div>

                    <div className="flex items-center gap-2 text-sm text-muted-foreground">
                      <Calendar className="w-4 h-4" />
                      <span>
                        Created: {new Date(exportItem.createdAt).toLocaleDateString()}
                      </span>
                    </div>
                  </div>

                  <div className="flex items-center gap-2">
                    {/* Only DONE jobs are in the list, but keep the guard */}
                    {exportItem.status === "DONE" && (
                      <Button
                        onClick={() => handleExport(exportItem.id)}
                        className="gap-2"
                        disabled={downloads[exportItem.id]}
                      >
                        <Download className="w-4 h-4" />
                        {downloads[exportItem.id] ? "Downloading..." : "Download ZIP"}
                      </Button>
                    )}
                  </div>
                </div>
              </div>
            ))}

            {filteredTasks.length === 0 && !loading && (
              <div className="text-center py-12">
                <Archive className="w-12 h-12 text-muted-foreground mx-auto mb-4" />
                <p className="text-muted-foreground">No exports available yet.</p>
                <p className="text-sm text-muted-foreground mt-1">
                  Completed tasks will appear here for download.
                </p>
              </div>
            )}
          </div>
        </CardContent>
      </Card>
    </div>
  );
}
