# app/services/validate.py
from typing import Dict, List
from app.services.imaging import variance_of_laplacian, largest_quadrilateral_skew_deg
from app.services.ocr import ocr_text_block, ocr_single_line, extract_label_fields, extract_azimuth
from app.services.classify import classify
from app.services.dedupe import phash, hamming

DEFAULTS = {
    "blur_min": 140.0,
    "dup_hamming_max": 5,
    "label_skew_max": 20.0,
}

LABEL_TYPES = {"LABELLING", "LABEL"}  # fallback if some jobs still say LABEL
AZIMUTH_TYPES = {"AZIMUTH"}


def run_pipeline(img, job_ctx, existing_phashes: list[str]) -> Dict:
    """
    job_ctx = {
      "expectedType": "LABELLING"|"AZIMUTH"|... (or None),
      "thresholds": { ... } (optional)
    }
    """
    th = {**DEFAULTS, **(job_ctx.get("thresholds") or {})}
    issues: List[str] = []
    checks: Dict = {}

    # 1) Blur (always)
    blur = variance_of_laplacian(img)
    checks["blurScore"] = blur
    if blur < th["blur_min"]:
        issues.append("Image is blurry")

    # 2) Type
    ptype = (job_ctx.get("expectedType") or "").upper()
    if not ptype:
        # Only classify when caller did not tell us the type
        ptype = classify(img, ocr_hint=None)

    # 3) Duplicate check (INFO ONLY now — does NOT fail)
    cur_phash = phash(img)
    is_dup = any(hamming(cur_phash, prev) <= th["dup_hamming_max"] for prev in existing_phashes)
    checks["isDuplicate"] = is_dup
    # IMPORTANT: we no longer append a failure for duplicates.
    # This lets you resend the same image in the same chat without touching whatsapp.py.

    fields: Dict = {}
    skew = None

    # 4) Only do *extra* work the type needs
    if ptype in LABEL_TYPES:
        # geometric skew
        skew = largest_quadrilateral_skew_deg(img)
        checks["skewDeg"] = skew

        # OCR ONLY for labels (MAC/RSN)
        has_ids = False
        if "Image is blurry" not in issues:  # optional short-circuit
            # Keep exactly what you had (simple & working for RSN)
            text_block = ocr_text_block(img)
            text_line  = ocr_single_line(img)
            # Preserve a line break so MAC-line heuristics work better downstream
            ocr_hint   = (text_block + "\n" + text_line).strip()
            fields     = extract_label_fields(ocr_hint)
            has_ids    = bool(fields.get("macId")) or bool(fields.get("rsn"))
            checks["hasLabelIds"] = bool(has_ids)

        # skew is a *warning* if IDs are readable; hard-fail only if both skew high *and* no IDs
        if skew is not None and skew > th["label_skew_max"]:
            if not has_ids:
                issues.append("Label angle too skewed")
            else:
                checks["skewWarn"] = True  # informational, not a failure

        if not has_ids:
            issues.append("Could not read MAC/RSN from label")

    elif ptype in AZIMUTH_TYPES:
        # OCR ONLY for azimuth (angle)
        if "Image is blurry" not in issues:
            text_block = ocr_text_block(img)
            fields = extract_azimuth(text_block)
            if not fields.get("azimuthDeg"):
                issues.append("Could not read compass/azimuth value")

    else:
        # Other types: **no OCR** by default (fast path)
        pass

    status = "PASS" if not issues else "FAIL"
    return {
        "type": ptype,
        "phash": cur_phash,
        "ocrText": None,   # keep minimal
        "fields": fields,
        "checks": checks,
        "status": status,
        "reason": issues,
    }
