from pydantic import BaseModel, Field
from typing import List, Optional, Literal, Dict, Any

PhotoType = str

class SectorProgress(BaseModel):
    sector: int
    requiredTypes: List[PhotoType]
    currentIndex: int = 0
    status: Literal["PENDING", "IN_PROGRESS", "DONE"] = "PENDING"

class CreateJob(BaseModel):
    workerPhone: str
    siteId: str
    sector: int

class JobOut(BaseModel):
    id: str
    workerPhone: str
    siteId: str
    sectors: List[SectorProgress]
    status: Literal["PENDING", "IN_PROGRESS", "DONE"]
    createdAt: Optional[str] = None
    # Useful aggregates for UI/export (optional, job-level rollups)
    macId: Optional[str] = None
    rsnId: Optional[str] = None
    azimuthDeg: Optional[float] = None

class PhotoOut(BaseModel):
    id: str
    jobId: str
    sector: int
    type: PhotoType
    s3Url: str
    fields: Dict[str, Any]
    checks: Dict[str, Any]
    status: str
    reason: List[str]
