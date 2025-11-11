// src/components/FilePreviewModal.tsx
import React, { useEffect, useMemo, useState } from "react";
import { Dialog, DialogContent, DialogHeader, DialogTitle } from "@/components/ui/dialog";
import { Tabs, TabsContent, TabsList, TabsTrigger } from "@/components/ui/tabs";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from "@/components/ui/select";
import { Download, Edit3, Image as ImageIcon, ImageOff, Archive } from "lucide-react";

import {
  fetchJobDetail,
  downloadJobXlsx,
  downloadJobXlsxWithImages,
  // NEW sector-wise helpers (ensure these exist in api.ts)
  downloadSectorWorkbook,
  downloadJobZip,
  type JobDetail,
  type PhotoItem,
  api,
} from "@/lib/api";

type Props = {
  isOpen: boolean;
  taskId: string;
  onClose: () => void;
};

type SectorBlock = {
  sector: number;
  requiredTypes?: string[];
  currentIndex?: number;
  status?: string;
};

function isRecord(v: unknown): v is Record<string, unknown> {
  return !!v && typeof v === "object" && !Array.isArray(v);
}

/** Build a fetchable URL for a private S3 object via backend redirect. */
function resolvePhotoUrl(p: PhotoItem | undefined | null): string | undefined {
  if (!p) return undefined;
  const raw = (p as any).s3Url || (p as any).s3Key || "";
  if (!raw) return undefined;
  if (/^https?:\/\//i.test(raw)) return raw; // already public URL
  const base = (api.defaults.baseURL || import.meta.env.VITE_API_URL || "").replace(/\/api\/?$/, "");
  return `${base}/api/photos/${encodeURIComponent((p as any).id)}/raw`;
}

/** Prefer explicit photo.sector; else parse from key/url using `sec{n}_`. */
function getPhotoSector(p: any): number | null {
  if (typeof p?.sector === "number" && Number.isFinite(p.sector)) return p.sector;
  const src: string = String(p?.s3Key || p?.s3Url || "");
  if (!src) return null;
  const m = src.toLowerCase().match(/(?:^|[\/_.-])sec(\d+)(?:[_\/.-]|$)/i);
  if (m && m[1]) {
    const n = Number(m[1]);
    return Number.isFinite(n) ? n : null;
  }
  return null;
}

export default function FilePreviewModal({ isOpen, taskId, onClose }: Props) {
  const [data, setData] = useState<JobDetail | null>(null);
  const [loading, setLoading] = useState(false);
  const [err, setErr] = useState<string | null>(null);

  const [imageNames, setImageNames] = useState<Record<string, string>>({});
  const [editingImage, setEditingImage] = useState<string | null>(null);

  useEffect(() => {
    if (!isOpen || !taskId) return;
    setLoading(true);
    setErr(null);
    setData(null);
    setImageNames({});
    setEditingImage(null);

    fetchJobDetail(taskId)
      .then((res) => setData(res))
      .catch((e: any) => setErr(e?.message ?? "Failed to load job"))
      .finally(() => setLoading(false));
  }, [isOpen, taskId]);

  /** Sectors on job */
  const sectorsFromJob = useMemo<number[]>(() => {
    const j: any = data?.job;
    if (!j) return [];
    if (Array.isArray(j.sectors) && j.sectors.length) {
      return j.sectors
        .map((b: any) => Number((b && (b.sector ?? b)) ?? NaN))
        .filter((n) => Number.isFinite(n))
        .sort((a, b) => a - b);
    }
    if (isRecord(j.sectorJobs)) {
      return Object.keys(j.sectorJobs)
        .map((k) => Number(k))
        .filter((n) => Number.isFinite(n))
        .sort((a, b) => a - b);
    }
    if (typeof j.sector === "number") return [j.sector];
    return [];
  }, [data?.job]);

  const [selectedSector, setSelectedSector] = useState<number | null>(null);
  useEffect(() => {
    if (!isOpen) return;
    if (sectorsFromJob.length === 0) setSelectedSector(null);
    else setSelectedSector((prev) => (prev != null && sectorsFromJob.includes(prev) ? prev : sectorsFromJob[0]));
  }, [isOpen, sectorsFromJob.join(",")]);

  /** Sector card for header/excel */
  const sectorBlock: SectorBlock | null = useMemo(() => {
    const j: any = data?.job;
    if (!j) return null;

    if (selectedSector != null && Array.isArray(j.sectors)) {
      const found = j.sectors.find((b: any) => Number(b?.sector) === Number(selectedSector));
      if (found && isRecord(found)) {
        return {
          sector: Number(found.sector),
          requiredTypes: Array.isArray(found.requiredTypes) ? found.requiredTypes : undefined,
          currentIndex: typeof found.currentIndex === "number" ? found.currentIndex : undefined,
          status: typeof found.status === "string" ? found.status : undefined,
        };
      }
    }

    if (selectedSector == null && (j.requiredTypes || j.currentIndex != null)) {
      return {
        sector: typeof j.sector === "number" ? j.sector : 0,
        requiredTypes: Array.isArray(j.requiredTypes) ? j.requiredTypes : undefined,
        currentIndex: typeof j.currentIndex === "number" ? j.currentIndex : undefined,
        status: typeof j.status === "string" ? j.status : undefined,
      };
    }

    return selectedSector != null
      ? { sector: selectedSector, requiredTypes: Array.isArray(j.requiredTypes) ? j.requiredTypes : undefined }
      : null;
  }, [data?.job, selectedSector]);

  /** Map: sector -> (type -> latest PhotoItem) */
  const latestByTypeForSector = useMemo(() => {
    const out = new Map<number, Map<string, PhotoItem>>();
    const photos = Array.isArray(data?.photos) ? (data!.photos as PhotoItem[]) : [];
    for (const p of photos) {
      const t = (p.type || "").toUpperCase();
      const sec = getPhotoSector(p);
      if (!Number.isFinite(sec)) continue;
      if (!out.has(sec!)) out.set(sec!, new Map());
      out.get(sec!)!.set(t, p);
    }
    return out;
  }, [data?.photos]);

  function getLatestForType(t: string): PhotoItem | undefined {
    const key = String(t || "").toUpperCase();
    if (selectedSector != null) {
      const mapForThisSector = latestByTypeForSector.get(selectedSector);
      return mapForThisSector?.get(key);
    }
    for (const [, m] of latestByTypeForSector) {
      const hit = m.get(key);
      if (hit) return hit;
    }
    return undefined;
  }

  /** Excel rows */
  const rows = useMemo(() => {
    const j: any = data?.job ?? {};
    const created = j.createdAt ?? "—";
    const totalPhotos = Array.isArray(data?.photos) ? String(data?.photos.length) : "0";

    const rt = sectorBlock?.requiredTypes;
    const idx = sectorBlock?.currentIndex;

    const reqTypesDisplay =
      Array.isArray(rt) && rt.length
        ? rt.join(", ")
        : Array.isArray(j.requiredTypes)
        ? j.requiredTypes.join(", ")
        : "—";

    const sectorDisplay =
      sectorBlock?.sector != null
        ? String(sectorBlock.sector)
        : typeof j.sector === "number"
        ? String(j.sector)
        : "—";

    const statusDisplay = (sectorBlock?.status ?? j.status ?? "—") as string;

    return [
      { label: "Job ID", value: taskId },
      { label: "Worker Phone", value: j.workerPhone ?? "—" },
      { label: "Site ID", value: j.siteId ?? "—" },
      { label: "Status", value: statusDisplay },
      { label: "Sector", value: sectorDisplay },
      { label: "Required Types", value: reqTypesDisplay },
      {
        label: "Current Index",
        value:
          typeof idx === "number"
            ? String(idx)
            : typeof j.currentIndex === "number"
            ? String(j.currentIndex)
            : "—",
      },
      { label: "Total Photos", value: totalPhotos },
      { label: "Created At", value: created },
    ];
  }, [data?.job, data?.photos, sectorBlock, taskId]);

  const handleImageNameEdit = (id: string, value: string) => {
    setImageNames((prev) => ({ ...prev, [id]: value.trim() }));
    setEditingImage(null);
  };

  const requiredTypesForGrid = useMemo<string[]>(() => {
    if (Array.isArray(sectorBlock?.requiredTypes) && sectorBlock!.requiredTypes!.length) {
      return sectorBlock!.requiredTypes!;
    }
    const j: any = data?.job;
    if (Array.isArray(j?.requiredTypes) && j.requiredTypes.length) return j.requiredTypes;
    return [];
  }, [sectorBlock?.requiredTypes, data?.job]);

  const canSectorDownload = selectedSector != null && Number.isFinite(selectedSector);

  return (
    <Dialog open={isOpen} onOpenChange={onClose}>
      <DialogContent aria-describedby={undefined} className="max-w-6xl h-[80vh] p-0 overflow-hidden">
        <DialogHeader className="px-6 pt-6">
          <DialogTitle className="flex items-center gap-2">
            <ImageIcon className="w-5 h-5" />
            Job Preview — {taskId}
          </DialogTitle>
        </DialogHeader>

        <Tabs defaultValue="images" className="flex-1 flex flex-col min-h-0">
          <div className="px-6 pb-3 flex items-center justify-between gap-3">
            <TabsList className="grid w-full max-w-xs grid-cols-2">
              <TabsTrigger value="images">Images</TabsTrigger>
              <TabsTrigger value="excel">Excel</TabsTrigger>
            </TabsList>

            {sectorsFromJob.length > 1 && (
              <Select
                value={selectedSector != null ? String(selectedSector) : undefined}
                onValueChange={(v) => setSelectedSector(Number(v))}
              >
                <SelectTrigger className="w-44">
                  <SelectValue placeholder="Select sector" />
                </SelectTrigger>
                <SelectContent>
                  {sectorsFromJob.map((s) => (
                    <SelectItem key={s} value={String(s)}>
                      Sector {s}
                    </SelectItem>
                  ))}
                </SelectContent>
              </Select>
            )}
          </div>

          {/* IMAGES */}
          <TabsContent value="images" className="flex-1 min-h-0">
            <div className="h-full overflow-y-auto px-6 pb-6">
              {loading && (
                <div className="rounded-lg border bg-muted p-6 text-muted-foreground">Loading details…</div>
              )}
              {err && (
                <div className="rounded-lg border border-red-200 bg-red-50 p-6 text-red-700">{err}</div>
              )}

              {!loading && !err && (
                <>
                  {requiredTypesForGrid.length > 0 ? (
                    <div className="grid grid-cols-2 md:grid-cols-4 lg:grid-cols-6 gap-4">
                      {requiredTypesForGrid.map((t) => {
                        const photo = getLatestForType(t);
                        const imgUrl = resolvePhotoUrl(photo);
                        const caption = String(t || "").replace(/_/g, " ").toUpperCase();

                        return (
                          <figure key={t} className="shrink-0 w-32">
                            <div className="relative group">
                              {imgUrl ? (
                                <img
                                  src={imgUrl}
                                  alt={caption}
                                  className="w-32 h-32 object-cover rounded-md border"
                                  loading="lazy"
                                />
                              ) : (
                                <div className="w-32 h-32 rounded-md border bg-muted/40 flex items-center justify-center">
                                  <ImageOff className="w-6 h-6 opacity-60" />
                                </div>
                              )}
                              {photo && (
                                <div className="absolute inset-0 bg-black/50 opacity-0 group-hover:opacity-100 transition-opacity rounded-md flex items-center justify-center">
                                  <Button
                                    size="sm"
                                    variant="secondary"
                                    onClick={() => setEditingImage((photo as any).id)}
                                  >
                                    <Edit3 className="w-3 h-3" />
                                  </Button>
                                </div>
                              )}
                            </div>

                            {photo && editingImage === (photo as any).id ? (
                              <Input
                                defaultValue={imageNames[(photo as any).id] || caption}
                                onBlur={(e) => handleImageNameEdit((photo as any).id, e.target.value)}
                                onKeyDown={(e) => {
                                  if (e.key === "Enter") {
                                    handleImageNameEdit((photo as any).id, (e.target as HTMLInputElement).value);
                                  }
                                }}
                                className="mt-1 h-7 text-xs"
                                autoFocus
                              />
                            ) : (
                              <figcaption
                                className="mt-1 text-xs text-muted-foreground truncate"
                                title={imgUrl || caption}
                              >
                                {photo ? imageNames[(photo as any).id] || caption : `${caption} (missing)`}
                              </figcaption>
                            )}
                          </figure>
                        );
                      })}
                    </div>
                  ) : Array.isArray(data?.photos) && data!.photos.length > 0 ? (
                    <div className="grid grid-cols-2 md:grid-cols-4 lg:grid-cols-6 gap-4">
                      {data!.photos.map((image: any) => {
                        const imgUrl = resolvePhotoUrl(image);
                        const caption = (image.type || "").toUpperCase();
                        return (
                          <figure key={image.id} className="shrink-0 w-32">
                            <div className="relative group">
                              {imgUrl ? (
                                <img
                                  src={imgUrl}
                                  alt={caption}
                                  className="w-32 h-32 object-cover rounded-md border"
                                  loading="lazy"
                                />
                              ) : (
                                <div className="w-32 h-32 rounded-md border bg-muted/40 flex items-center justify-center">
                                  <ImageOff className="w-6 h-6 opacity-60" />
                                </div>
                              )}
                              <div className="absolute inset-0 bg-black/50 opacity-0 group-hover:opacity-100 transition-opacity rounded-md flex items-center justify-center">
                                <Button size="sm" variant="secondary" onClick={() => setEditingImage(image.id)}>
                                  <Edit3 className="w-3 h-3" />
                                </Button>
                              </div>
                            </div>
                            {editingImage === image.id ? (
                              <Input
                                defaultValue={imageNames[image.id] || caption}
                                onBlur={(e) => handleImageNameEdit(image.id, e.target.value)}
                                onKeyDown={(e) => {
                                  if (e.key === "Enter") {
                                    handleImageNameEdit(image.id, (e.target as HTMLInputElement).value);
                                  }
                                }}
                                className="mt-1 h-7 text-xs"
                                autoFocus
                              />
                            ) : (
                              <figcaption className="mt-1 text-xs text-muted-foreground truncate" title={imgUrl}>
                                {imageNames[image.id] || caption}
                              </figcaption>
                            )}
                          </figure>
                        );
                      })}
                    </div>
                  ) : (
                    <div className="rounded-lg border bg-muted p-6 text-muted-foreground">No photos yet.</div>
                  )}
                </>
              )}
            </div>
          </TabsContent>

          {/* EXCEL */}
          <TabsContent value="excel" className="flex-1 min-h-0">
            <div className="h-full overflow-y-auto px-6 pb-6 space-y-4">
              <div className="flex items-center justify-between flex-wrap gap-3">
                <h3 className="font-medium">Job Report Data</h3>
                <div className="flex items-center gap-2 flex-wrap">
                  {/* Whole Job */}
                  <Button size="sm" variant="outline" onClick={() => downloadJobXlsx(taskId)} disabled={!data || loading}>
                    <Download className="w-4 h-4 mr-2" />
                    Excel (All)
                  </Button>
                  <Button size="sm" variant="outline" onClick={() => downloadJobXlsxWithImages(taskId)} disabled={!data || loading}>
                    <Download className="w-4 h-4 mr-2" />
                    Excel + Images (All)
                  </Button>

                  {/* Sector-wise — enabled only when a sector is selected */}
                  <Button
                    size="sm"
                    onClick={() => selectedSector != null && downloadSectorWorkbook()}
                    disabled={!data || loading || !canSectorDownload}
                  >
                    <Download className="w-4 h-4 mr-2" />
                    Excel (Sector)
                  </Button>
                </div>
              </div>

              <div className="border rounded-lg overflow-hidden">
                <table className="w-full">
                  <thead className="bg-muted">
                    <tr>
                      <th className="text-left p-3 font-medium">Label</th>
                      <th className="text-left p-3 font-medium">Value</th>
                    </tr>
                  </thead>
                  <tbody>
                    {rows.map((row, idx) => (
                      <tr key={idx} className="border-t">
                        <td className="p-3 font-medium">{row.label}</td>
                        <td className="p-3">
                          {typeof row.value === "string"
                            ? row.value
                            : Array.isArray(row.value)
                            ? row.value.join(", ")
                            : row.value != null
                            ? String(row.value)
                            : "—"}
                        </td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
            </div>
          </TabsContent>
        </Tabs>

        {/* Footer */}
        <div className="flex justify-end gap-2 pt-4 px-6 pb-6 border-t">
          <Button variant="outline" onClick={onClose}>
            Close
          </Button>
        </div>
      </DialogContent>
    </Dialog>
  );
}
