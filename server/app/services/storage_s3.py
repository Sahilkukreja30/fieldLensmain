import os, io, uuid, time
from typing import Optional

# --- envs ---
def _as_bool(val: Optional[str]) -> bool:
    if val is None:
        return False
    return val.strip().lower() in {"1", "true", "yes", "y"}

USE_LOCAL = _as_bool(os.getenv("USE_LOCAL_STORAGE", "0"))
BUCKET    = os.getenv("S3_BUCKET", "")
REGION    = os.getenv("AWS_REGION", "ap-south-1")
LOCAL_DIR = os.getenv("LOCAL_STORAGE_DIR", "/tmp/_local_uploads")

# In S3 mode, init boto3 client
if not USE_LOCAL:
    import boto3
    from botocore.client import Config
    s3 = boto3.client(
        "s3",
        region_name=REGION,
        config=Config(signature_version="s3v4"),
    )

def put_bytes(key: str, data: bytes) -> str:
    """
    Store raw bytes. Returns a locator (s3://bucket/key or local file:// path).
    """
    if USE_LOCAL:
        path = os.path.join(LOCAL_DIR, key)
        os.makedirs(os.path.dirname(path), exist_ok=True)
        with open(path, "wb") as f:
            f.write(data)
        return f"file://{os.path.abspath(path)}"
    else:
        if not BUCKET:
            raise RuntimeError("S3_BUCKET not configured")
        s3.put_object(
            Bucket=BUCKET,
            Key=key,
            Body=data,
            ContentType="image/jpeg",
        )
        return f"s3://{BUCKET}/{key}"

def presign_url(key: str, expires: int = 3600) -> str:
    """
    Return a URL suitable for <img src> and server-side downloads.
    Local: served by FastAPI /uploads static mount.
    S3:    AWS presigned HTTPS URL.
    """
    if USE_LOCAL:
        return f"/uploads/{key}"
    else:
        if not BUCKET:
            raise RuntimeError("S3_BUCKET not configured")
        return s3.generate_presigned_url(
            "get_object",
            Params={"Bucket": BUCKET, "Key": key},
            ExpiresIn=expires,
        )

def new_image_key(job_id: str, kind: str, ext: str = "jpg", sector: int | None = None) -> str:
    """
    Generate a consistent storage key. We keep a timestamp + short uid prefix
    so the ZIP/export can reconstruct logical names and keep original ext.
    """
    ts = int(time.time() * 1000)
    uid = uuid.uuid4().hex[:8]
    logical = f"sec{sector}_{kind.lower()}.{ext}" if sector else f"{kind.lower()}.{ext}"
    return f"jobs/{job_id}/raw/{ts}-{uid}-{logical}"

def get_bytes(key: str) -> bytes:
    """
    Fetch raw bytes by key.
    """
    if USE_LOCAL:
        path = os.path.join(LOCAL_DIR, key)
        with open(path, "rb") as f:
            return f.read()
    else:
        if not BUCKET:
            raise RuntimeError("S3_BUCKET not configured")
        obj = s3.get_object(Bucket=BUCKET, Key=key)
        return obj["Body"].read()
