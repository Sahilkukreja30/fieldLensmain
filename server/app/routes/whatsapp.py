# app/routes/whatsapp.py
import os
import traceback
from typing import Tuple, List, Optional, Dict, Any

import cv2
import httpx
from fastapi import APIRouter, Depends, Request, Response, Form, File, UploadFile, BackgroundTasks
from fastapi.responses import JSONResponse, PlainTextResponse
from twilio.twiml.messaging_response import MessagingResponse

from app.deps import get_db
from app.services.validate import run_pipeline
from app.services.imaging import load_bgr
from app.services.storage_s3 import new_image_key, put_bytes
from app.utils import (
    normalize_phone,
    type_prompt,
    type_example_url,
    is_validated_type,   # kept for compatibility (your pipeline uses it)
    twilio_client,       # Twilio REST client if configured
    TWILIO_WHATSAPP_FROM # whatsapp:from number (e.g. "whatsapp:+1415....")
)

router = APIRouter()

# ---------------------------
# Small helpers (sector-aware)
# ---------------------------

def sector_by_id(sectors: List[Dict[str, Any]] | None, sid: int) -> Optional[Dict[str, Any]]:
    if not sectors:
        return None
    for s in sectors:
        try:
            if int(s.get("sector")) == int(sid):
                return s
        except Exception:
            continue
    return None

def _current_expected_type_for_sector(job: Dict[str, Any], sid: int) -> Optional[str]:
    """Return the next required type for the given sector in this job."""
    sec = sector_by_id(job.get("sectors"), sid)
    if not sec:
        return None
    idx = int(sec.get("currentIndex", 0) or 0)
    req = sec.get("requiredTypes", []) or []
    if 0 <= idx < len(req):
        return req[idx]
    return None

def _pick_active_sector(job: Dict[str, Any]) -> Optional[int]:
    """
    Choose the 'active' sector:
      - first sector that is not DONE and has remaining requiredTypes.
    """
    for s in (job.get("sectors") or []):
        st = (s.get("status") or "").upper()
        req = s.get("requiredTypes") or []
        idx = int(s.get("currentIndex", 0) or 0)
        if st != "DONE" and idx < len(req):
            try:
                return int(s.get("sector"))
            except Exception:
                continue
    return None

def all_sectors_done(sectors: List[Dict[str, Any]] | None) -> bool:
    if not sectors:
        return False
    for s in sectors:
        st = (s.get("status") or "").upper()
        req = s.get("requiredTypes") or []
        idx = int(s.get("currentIndex", 0) or 0)
        if st != "DONE" and idx < len(req):
            return False
    return True

def _downscale_for_ocr(bgr, max_side: int = 1280):
    """Keep aspect; limit longest side to max_side for faster OCR."""
    h, w = bgr.shape[:2]
    m = max(h, w)
    if m <= max_side:
        return bgr
    scale = max_side / float(m)
    nh, nw = int(h * scale), int(w * scale)
    return cv2.resize(bgr, (nw, nh), interpolation=cv2.INTER_AREA)

# ---------------------------
# Twilio / media utilities
# ---------------------------

TWILIO_ACCOUNT_SID = os.getenv("TWILIO_ACCOUNT_SID")
TWILIO_AUTH_TOKEN  = os.getenv("TWILIO_AUTH_TOKEN")

async def _fetch_media(url: str) -> bytes:
    if not TWILIO_ACCOUNT_SID or not TWILIO_AUTH_TOKEN:
        raise RuntimeError("Twilio auth not configured.")
    async with httpx.AsyncClient(
        auth=(TWILIO_ACCOUNT_SID, TWILIO_AUTH_TOKEN),
        timeout=30,
        follow_redirects=True,
    ) as client:
        r = await client.get(url)
        r.raise_for_status()
        return r.content

def build_twiml_reply(body_text: str, media_urls: Optional[List[str] | str] = None) -> Response:
    resp = MessagingResponse()
    msg = resp.message(body_text)
    if isinstance(media_urls, str):
        media_urls = [media_urls]
    if media_urls:
        for m in media_urls:
            if m and m.lower().startswith(("http://", "https://")):
                msg.media(m)
    xml = str(resp)
    print("[TWIML OUT]\n", xml)
    return Response(content=xml, media_type="application/xml")

def _safe_example_list(example_url: Optional[str]) -> Optional[List[str]]:
    if not example_url:
        return None
    s = example_url.strip()
    return [s] if s.lower().startswith(("http://", "https://")) else None

# ---------------------------
# Background processor
# ---------------------------

def _process_and_notify(
    db,
    worker_number: str,
    job_id: str,
    active_sector: int,
    result_type_hint: Optional[str],
    image_bytes: bytes
):
    """
    Runs validation, updates DB/job for the given sector, and proactively
    notifies the worker with next prompt or retake via Twilio REST (if configured).
    """
    try:
        # 1) Reload fresh job
        job = db.jobs.find_one({"_id": job_id})
        if not job:
            print("[BG] Job missing; abort.")
            return

        # 2) Expected type for this sector
        expected = _current_expected_type_for_sector(job, int(active_sector))

        # 3) Decode + downscale (speed)
        img = load_bgr(image_bytes)
        if img is None:
            raise ValueError("decode_failed")
        img_small = _downscale_for_ocr(img)

        # 4) Previous phashes for THIS sector & THIS expected type
        prev_phashes = [
            p.get("phash")
            for p in db.photos.find(
                {
                    "jobId": str(job["_id"]),
                    "sector": int(active_sector),
                    "type": (expected or "").upper(),
                    "status": {"$in": ["PASS", "FAIL"]},
                },
                {"phash": 1}
            )
            if p.get("phash")
        ]

        # 5) Validate
        result = run_pipeline(
            img_small,
            job_ctx={"expectedType": expected},
            existing_phashes=prev_phashes
        )

        # Promote important fields to job-level (optional, matches your UI/export)
        fields = result.get("fields") or {}
        updates: Dict[str, Any] = {}
        if fields.get("macId"):
            updates["macId"] = fields["macId"]
        if fields.get("rsn"):
            updates["rsnId"] = fields["rsn"]
        if fields.get("azimuthDeg") is not None:
            updates["azimuthDeg"] = fields["azimuthDeg"]
        if updates:
            db.jobs.update_one({"_id": job["_id"]}, {"$set": updates})

        result_type = (result.get("type") or expected or "LABELLING").upper()

        # 6) Update last inserted photo (the one saved in webhook)
        last_photo = db.photos.find_one({"jobId": str(job["_id"])}, sort=[("_id", -1)])
        if last_photo:
            db.photos.update_one(
                {"_id": last_photo["_id"]},
                {"$set": {
                    "type": result_type,
                    "phash": result.get("phash"),
                    "ocrText": result.get("ocrText"),
                    "fields": result.get("fields") or {},
                    "checks": result.get("checks") or {},
                    "status": result.get("status"),
                    "reason": result.get("reason") or [],
                }}
            )

        # 7) Advance THIS sector only (no top-level currentIndex!)
        status = (result.get("status") or "").upper()
        if status == "PASS" and expected and result_type == expected:
            db.jobs.update_one(
                {"_id": job["_id"], "sectors.sector": int(active_sector)},
                {"$inc": {"sectors.$.currentIndex": 1}}
            )
            job = db.jobs.find_one({"_id": job["_id"]})

            # If the sector finished, mark it DONE
            sec_doc = sector_by_id(job.get("sectors", []), int(active_sector))
            if sec_doc:
                idx = int(sec_doc.get("currentIndex", 0))
                req = sec_doc.get("requiredTypes", [])
                if idx >= len(req):
                    db.jobs.update_one(
                        {"_id": job["_id"], "sectors.sector": int(active_sector)},
                        {"$set": {"sectors.$.status": "DONE"}}
                    )
                    job = db.jobs.find_one({"_id": job["_id"]})

        # If all sectors done, mark whole job DONE
        if all_sectors_done(job.get("sectors")):
            db.jobs.update_one({"_id": job["_id"]}, {"$set": {"status": "DONE"}})
            job = db.jobs.find_one({"_id": job["_id"]})

        # 8) Compose outbound message (next instruction or retake)
        text = ""
        media = None
        if (result.get("status") or "").upper() == "PASS":
            # Recompute next expected for same sector
            next_expected = _current_expected_type_for_sector(job, int(active_sector))
            if next_expected is None:
                text = (
                    "✅ Received and verified. Sector complete.\n"
                    "✅ सेक्टर पूरा हो गया। धन्यवाद!"
                )
                # If all sectors are done, optionally add a final line
                if (job.get("status") or "").upper() == "DONE":
                    text += "\n✅ All assigned sectors complete for this site."
            else:
                prompt, example = type_prompt(next_expected), type_example_url(next_expected)
                text = f"✅ {result_type} verified.\nNext: {prompt}\nअब अगली फोटो भेजें।"
                media = example
        else:
            # Retake path
            fallback_type = expected or result_type
            prompt, example = type_prompt(fallback_type), type_example_url(fallback_type)
            reasons = "; ".join(result.get("reason") or []) or "needs retake"
            text = (
                f"❌ {result_type} failed: {reasons}.\n"
                f"Please retake and resend.\n{prompt}\n"
                f"कृपया दोबारा साफ फोटो भेजें।"
            )
            media = example

        # 9) Send proactive WhatsApp message (if REST client configured)
        if twilio_client and TWILIO_WHATSAPP_FROM:
            to_number = worker_number if worker_number.startswith("whatsapp:") else f"whatsapp:{worker_number}"
            kwargs = {"from_": TWILIO_WHATSAPP_FROM, "to": to_number, "body": text}
            if media and media.lower().startswith(("http://", "https://")):
                kwargs["media_url"] = [media]
            msg = twilio_client.messages.create(**kwargs)
            print(f"[BG] Notified worker, SID={msg.sid}")
        else:
            print("[BG] Twilio REST not configured; outbound message skipped.")
            print("[BG] Would have sent:", text)

    except Exception as e:
        print("[BG] Pipeline/notify error:", repr(e))
        traceback.print_exc()

# ---------------------------
# Webhook
# ---------------------------

@router.post("/whatsapp/webhook")
async def whatsapp_webhook(request: Request, background: BackgroundTasks, db=Depends(get_db)):
    """
    WhatsApp webhook (Twilio). Handles both text prompts and image uploads.
    - Multi-sector: chooses an active sector and processes per-sector steps.
    - Saves photo immediately to storage (S3/local) before background validation.
    """
    # Parse body (Twilio sends form-encoded)
    try:
        form = await request.form()
    except Exception:
        try:
            _ = await request.json()
            return PlainTextResponse("Unsupported content-type", status_code=415)
        except Exception:
            return PlainTextResponse("Bad Request", status_code=400)

    from_param  = form.get("From") or form.get("WaId") or ""
    from_num    = normalize_phone(from_param)
    media_count = int(form.get("NumMedia") or 0)
    print(f"[INCOMING] From: {from_param} Normalized: {from_num} NumMedia: {media_count}")

    # Find an in-flight job for this worker
    job = db.jobs.find_one({
        "workerPhone": from_num,
        "status": {"$in": ["PENDING", "IN_PROGRESS"]}
    })
    if not job:
        return build_twiml_reply(
            "No active job assigned yet. Please contact your supervisor.\n"
            "कोई सक्रिय जॉब असाइन नहीं है। कृपया सुपरवाइज़र से संपर्क करें।"
        )

    # Start the job if it was pending
    if job.get("status") == "PENDING":
        db.jobs.update_one({"_id": job["_id"]}, {"$set": {"status": "IN_PROGRESS"}})
        job = db.jobs.find_one({"_id": job["_id"]})

    # Pick an active sector to work on
    active_sector = _pick_active_sector(job)
    if active_sector is None:
        # Nothing left to do — mark job done and reply
        db.jobs.update_one({"_id": job["_id"]}, {"$set": {"status": "DONE"}})
        return build_twiml_reply(
            "✅ All assigned sectors complete for this site.\n"
            "✅ सभी सेक्टर पूरे हो गए हैं। धन्यवाद!"
        )

    expected = _current_expected_type_for_sector(job, int(active_sector))

    # If text-only, (re)prompt with example
    if media_count == 0:
        fallback = expected or "LABELLING"
        prompt, example = type_prompt(fallback), type_example_url(fallback)
        return build_twiml_reply(
            f"{prompt}\nSend 1 image at a time.\nएक समय में सिर्फ 1 फोटो भेजें।",
            media_urls=_safe_example_list(example),
        )

    # Ensure image content
    media_url    = form.get("MediaUrl0")
    content_type = form.get("MediaContentType0", "")
    if not media_url or not content_type.startswith("image/"):
        fallback = expected or "LABELLING"
        prompt, example = type_prompt(fallback), type_example_url(fallback)
        return build_twiml_reply(
            f"Please send a valid image. {prompt}\nकृपया सही इमेज भेजें।",
            media_urls=_safe_example_list(example),
        )

    # Fetch image bytes from Twilio
    try:
        data = await _fetch_media(media_url)
    except Exception as e:
        print("[WHATSAPP] Media fetch error:", repr(e))
        fallback = expected or "LABELLING"
        prompt, example = type_prompt(fallback), type_example_url(fallback)
        return build_twiml_reply(
            f"❌ Could not download the image. Please resend.\n"
            f"इमेज डाउनलोड नहीं हो सकी, दोबारा भेजें।\n{prompt}",
            media_urls=_safe_example_list(example),
        )

    # Persist original image right away (S3/local) for reliability
    try:
        result_hint = (expected or "LABELLING").upper()
        key = new_image_key(str(job["_id"]), f"s{active_sector}_{result_hint.lower()}", "jpg")
        # put_bytes should return a URL or None; we store both key and url if available
        put_result = put_bytes(key, data)
        s3_url = put_result if isinstance(put_result, str) else None

        db.photos.insert_one({
            "jobId": str(job["_id"]),
            "sector": int(active_sector),
            "type": result_hint,          # will be replaced by actual detected type in BG
            "s3Key": key,
            "s3Url": s3_url,              # optional if your storage returns URL
            "phash": None,
            "ocrText": None,
            "fields": {},
            "checks": {},
            "status": "PROCESSING",
            "reason": [],
        })
    except Exception as e:
        print("[STORAGE/DB] initial save error:", repr(e))
        return build_twiml_reply(
            "❌ Could not save the image. Please resend later.\n"
            "इमेज सेव नहीं हो पाई, बाद में दोबारा भेजें।"
        )

    # Kick background validation → sector-aware update → proactive notify
    background.add_task(
        _process_and_notify,
        db,
        from_num,
        job["_id"],
        int(active_sector),
        result_hint,
        data
    )

    # Immediate ACK (stay under 15s)
    return build_twiml_reply(
        "📥 Got the photo. Processing… please wait for the next instruction.\n"
        "📥 फोटो मिल गई। प्रोसेस हो रही है — अगला निर्देश जल्दी मिलेगा।"
    )


# ---------------------------
# Debug: direct upload (no WhatsApp)
# ---------------------------

@router.post("/debug/upload")
async def debug_upload(
    workerPhone: str = Form(...),
    siteId: str = Form(...),
    sector: int = Form(...),
    file: UploadFile = File(...),
    db=Depends(get_db),
):
    """
    Convenience route for testing the validation pipeline without WhatsApp.
    Ensures a minimal job exists (per workerPhone + siteId) and a sector entry;
    runs pipeline; saves photo; advances currentIndex for that sector on PASS.
    """
    # Find or create a job
    job = db.jobs.find_one({
        "workerPhone": workerPhone,
        "siteId": siteId,
        "status": {"$in": ["PENDING", "IN_PROGRESS"]}
    })
    if not job:
        job = {
            "siteId": siteId,
            "workerPhone": workerPhone,
            "status": "IN_PROGRESS",
            "sectors": [{
                "sector": int(sector),
                "requiredTypes": ["LABELLING", "AZIMUTH"],
                "currentIndex": 0,
                "status": "IN_PROGRESS",
            }]
        }
        ins = db.jobs.insert_one(job)
        job["_id"] = ins.inserted_id
    else:
        # ensure sector exists
        if sector_by_id(job.get("sectors"), int(sector)) is None:
            db.jobs.update_one(
                {"_id": job["_id"]},
                {"$push": {"sectors": {
                    "sector": int(sector),
                    "requiredTypes": ["LABELLING", "AZIMUTH"],
                    "currentIndex": 0,
                    "status": "IN_PROGRESS",
                }}}
            )
            job = db.jobs.find_one({"_id": job["_id"]})

    # active sector is the requested one
    expected = _current_expected_type_for_sector(job, int(sector))

    data = await file.read()
    try:
        img = load_bgr(data)
        if img is None:
            raise ValueError("Could not decode image.")
    except Exception as e:
        return JSONResponse({"error": f"decode_failed: {repr(e)}"}, status_code=400)

    # Prior phashes (same sector + expected type)
    prev_phashes = [
        p.get("phash")
        for p in db.photos.find(
            {
                "jobId": str(job["_id"]),
                "sector": int(sector),
                "type": (expected or "").upper(),
                "status": {"$in": ["PASS", "FAIL"]},
            },
            {"phash": 1}
        )
        if p.get("phash")
    ]

    # Run pipeline
    try:
        result = run_pipeline(
            img,
            job_ctx={"expectedType": expected},
            existing_phashes=prev_phashes
        )

        # Optional: promote fields to job
        fields = result.get("fields") or {}
        updates: Dict[str, Any] = {}
        if fields.get("macId"):
            updates["macId"] = fields["macId"]
        if fields.get("rsn"):
            updates["rsnId"] = fields["rsn"]
        if fields.get("azimuthDeg") is not None:
            updates["azimuthDeg"] = fields["azimuthDeg"]
        if updates:
            db.jobs.update_one({"_id": job["_id"]}, {"$set": updates})

    except Exception as e:
        traceback.print_exc()
        return JSONResponse({"error": f"pipeline_crashed: {repr(e)}"}, status_code=500)

    # Save to storage
    result_type = (result.get("type") or expected or "LABELLING").upper()
    try:
        key = new_image_key(str(job["_id"]), f"s{sector}_{result_type.lower()}", "jpg")
        put_result = put_bytes(key, data)
        s3_url = put_result if isinstance(put_result, str) else None

        db.photos.insert_one({
            "jobId": str(job["_id"]),
            "sector": int(sector),
            "type": result_type,
            "s3Key": key,
            "s3Url": s3_url,
            "phash": result.get("phash"),
            "ocrText": result.get("ocrText"),
            "fields": result.get("fields") or {},
            "checks": result.get("checks") or {},
            "status": result.get("status"),
            "reason": result.get("reason") or [],
        })
    except Exception as e:
        traceback.print_exc()
        return JSONResponse({"error": f"save_failed: {repr(e)}"}, status_code=500)

    # Advance this sector only on exact PASS
    if (result.get("status") or "").upper() == "PASS" and expected and result_type == expected:
        db.jobs.update_one(
            {"_id": job["_id"], "sectors.sector": int(sector)},
            {"$inc": {"sectors.$.currentIndex": 1}}
        )
        job = db.jobs.find_one({"_id": job["_id"]})
        sec_doc = sector_by_id(job.get("sectors", []), int(sector))
        if sec_doc:
            idx = int(sec_doc.get("currentIndex", 0))
            req = sec_doc.get("requiredTypes", [])
            if idx >= len(req):
                db.jobs.update_one(
                    {"_id": job["_id"], "sectors.sector": int(sector)},
                    {"$set": {"sectors.$.status": "DONE"}}
                )
                job = db.jobs.find_one({"_id": job["_id"]})

    # If all sectors done, mark whole job DONE
    if all_sectors_done(job.get("sectors")):
        db.jobs.update_one({"_id": job["_id"]}, {"$set": {"status": "DONE"}})

    return JSONResponse({
        "jobId": str(job["_id"]),
        "sector": int(sector),
        "type": result_type,
        "status": result.get("status"),
        "reason": result.get("reason") or [],
        "fields": result.get("fields") or {},
    })
