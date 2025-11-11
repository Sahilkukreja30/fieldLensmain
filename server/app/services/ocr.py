# app/services/ocr.py
from __future__ import annotations
import os
import re
from typing import Dict, Optional, Tuple, Any, List
import cv2
import numpy as np
from functools import lru_cache

# --- Prefer pytesseract, fallback to EasyOCR ---
try:
    import pytesseract
except Exception:
    pytesseract = None

# Where to store OCR models/caches on HF Spaces (writable)
EASYOCR_DIR = os.getenv("EASYOCR_DIR", "/tmp/.easyocr")
# Hint Torch to cache in /tmp as well (safe if already set)
os.environ.setdefault("TORCH_HOME", os.getenv("TORCH_HOME", "/tmp/torch"))

def _ensure_dir(p: str):
    try:
        os.makedirs(p, exist_ok=True)
    except Exception as e:
        print("[OCR] Could not create dir:", p, repr(e))

@lru_cache(maxsize=1)
def _easyocr():
    """
    Build (and cache) a single EasyOCR Reader instance.
    On HF Spaces the filesystem is read-only except /tmp,
    so we place models under /tmp/.easyocr and torch cache under TORCH_HOME.
    """
    _ensure_dir(EASYOCR_DIR)
    _ensure_dir(os.path.join(EASYOCR_DIR, "user_network"))

    # Lazy import so startup doesn't explode if deps still installing
    import easyocr

    languages = ["en"]  # add "hi" if your labels sometimes include Hindi
    print("[OCR] Initializing EasyOCR with model dir:", EASYOCR_DIR)
    reader = easyocr.Reader(
        lang_list=languages,
        gpu=False,
        download_enabled=True,
        model_storage_directory=EASYOCR_DIR,
        user_network_directory=os.path.join(EASYOCR_DIR, "user_network"),
    )
    return reader

# ---------- Low-level imaging ----------
def load_bgr_from_path(path: str) -> np.ndarray:
    img = cv2.imread(path)
    if img is None:
        raise ValueError(f"Could not read image: {path}")
    return img

def laplacian_blur_score(bgr: np.ndarray) -> float:
    """Higher is sharper. Typical threshold ~100–200; tune per device."""
    gray = cv2.cvtColor(bgr, cv2.COLOR_BGR2GRAY)
    return float(cv2.Laplacian(gray, cv2.CV_64F).var())

# ---------- OCR ----------
def ocr_text(bgr: np.ndarray) -> str:
    """
    Generic OCR: try pytesseract first; if missing/fails, fallback to EasyOCR.
    """
    rgb = cv2.cvtColor(bgr, cv2.COLOR_BGR2RGB)

    # 1) Try pytesseract
    if pytesseract is not None:
        try:
            txt = pytesseract.image_to_string(rgb)
            return txt or ""
        except Exception:
            pass  # fallback

    # 2) Fallback to EasyOCR
    reader = _easyocr()
    lines = reader.readtext(rgb, detail=0) or []
    return "\n".join(lines)

def ocr_lines_easy(bgr: np.ndarray) -> List[str]:
    """
    Explicit EasyOCR line mode (matches your working Colab flow).
    IMPORTANT: feed RGB to EasyOCR.
    """
    reader = _easyocr()
    rgb = cv2.cvtColor(bgr, cv2.COLOR_BGR2RGB)
    return reader.readtext(rgb, detail=0) or []

# ---------- Text extract helpers ----------
# Base MAC (allow ":" "-" or "." separators)
MAC_RE = re.compile(r"\b([0-9A-Fa-f]{2}[:\-\.]){5}[0-9A-Fa-f]{2}\b")

MAC_KEYWORD = re.compile(
    r"\b(MAC(?:\s*ID)?|WLAN\s*MAC|WIFI\s*MAC|LAN\s*MAC|ETH(?:ERNET)?\s*MAC)\b[:\-\s]*([0-9A-Fa-f:\-\.\s]{12,64})",
    re.IGNORECASE
)
MAC_LINE_HINT = re.compile(
    r"\b(MAC(?:\s*ID)?|WLAN\s*MAC|WIFI\s*MAC|LAN\s*MAC|ETH(?:ERNET)?\s*MAC)\b",
    re.IGNORECASE
)

# Accept colon, hyphen, or bare 12 hex
MAC_PATTERNS = [
    re.compile(r"\b([0-9A-Fa-f]{2}[:\-\.]){5}[0-9A-Fa-f]{2}\b"),
    re.compile(r"\b[0-9A-Fa-f]{12}\b"),
]
HEX_PAIR = re.compile(r"[0-9A-Fa-f]{2}")
HEX_PAIR_BOUNDARY = re.compile(r"(?<![0-9A-Fa-f])[0-9A-Fa-f]{2}(?![0-9A-Fa-f])")

# Whitelist a few common OUIs if you have them; helps ranking but not required
OUI_PREFER = {
    "CC:54:FE",
}

RSN_LABELED_RE = re.compile(
    r"\b(?:RSN|S\/N|SERIAL|SRN|ASN)\s*[:#\-]?\s*([A-Z0-9\-]{6,24})\b",
    re.IGNORECASE,
)
RSN_TOKEN_RE = re.compile(r"[A-Z0-9\-]{6,24}")
RSN_STOPWORDS = {
    "COMMODITY", "INDIA", "MADEININDIA", "WARRANTY", "MODEL",
    "MANUFACTURED", "EXPIRY", "BATCH", "ADDRESS", "CONTACT",
    "SUPPORT", "SERVICE", "HELPLINE", "SERIES", "PRODUCT",
    "POWER", "VOLT", "AMPS", "HERTZ", "DATE", "CODE", "EAN"
}

# --- NEW: fix common OCR confusions before parsing MACs ---
def _cleanup_hexish(s: str) -> str:
    """
    Replace visually similar chars and drop non-hex/separators.
    O->0, Q->0, I|L->1, S->5, B->8, Z->2.
    """
    t = (s or "").upper()
    trans = str.maketrans({
        "O": "0",
        "Q": "0",
        "I": "1",
        "L": "1",
        "S": "5",
        "B": "8",
        "Z": "2",
    })
    t = t.translate(trans)
    # keep only hex digits and common separators
    t = re.sub(r"[^0-9A-F:\-\.]", "", t)
    return t

def _normalize_mac(raw: str) -> Optional[str]:
    """
    Turn a messy mac-like string into CC:54:FE:AA:BB:CC by sliding a 6-pair window.
    Works for:
      - 'CC:54:FE:E3:26:F8'
      - 'CC54FEE326F8'
      - 'CC-54-FE-E3-26-F8'
      - 'CC 54 FE E3 26 F8'
    """
    if not raw:
        return None
    cleaned = _cleanup_hexish(raw)
    pairs = HEX_PAIR.findall(cleaned)
    if len(pairs) < 6:
        return None

    best = None
    best_score = -1.0
    # Slide over all 6-pair windows and score them
    for i in range(0, len(pairs) - 5):
        window = pairs[i:i+6]
        mac = ":".join(p.upper() for p in window)
        # basic scoring: prefer OUIs we know + windows where original had separators
        sep_bonus = 1 if (":" in cleaned or "-" in cleaned or "." in cleaned) else 0
        oui_bonus = 2 if mac[:8] in OUI_PREFER else 0
        digitful = sum(any(ch.isdigit() for ch in p) for p in window) / 6.0
        score = sep_bonus + oui_bonus + digitful
        if score > best_score:
            best, best_score = mac, score
    return best

def _extract_mac_from_lines(lines: List[str]) -> Optional[str]:
    """Prefer 'MAC ...' lines, then strict patterns anywhere, then spaced pairs after keyword."""
    candidates: List[str] = []

    # A) keyword-anchored extraction (most reliable)
    for ln in (lines or []):
        U = (ln or "").upper()
        if "EAN" in U:
            continue
        mac_line = MAC_LINE_HINT.search(ln)
        if not mac_line:
            continue

        # 1) Try 'MAC ... <value>' tail
        m = MAC_KEYWORD.search(ln)
        if m:
            norm = _normalize_mac(m.group(2))
            if norm:
                candidates.append(norm)

        # 2) Try strict patterns on the same line
        for pat in MAC_PATTERNS:
            mm = pat.search(ln)
            if mm:
                norm = _normalize_mac(mm.group(0))
                if norm:
                    candidates.append(norm)

        # 3) Last resort on this line: spaced pairs after the keyword
        tail = ln[mac_line.end():]
        pairs = HEX_PAIR_BOUNDARY.findall(_cleanup_hexish(tail))
        for i in range(0, max(0, len(pairs) - 5)):
            cand = ":".join(pairs[i:i+6])
            norm = _normalize_mac(cand)
            if norm:
                candidates.append(norm)

    # B) strict patterns anywhere (if nothing from keyword lines)
    if not candidates:
        for ln in (lines or []):
            U = (ln or "").upper()
            if "EAN" in U:
                continue
            for pat in MAC_PATTERNS:
                mm = pat.search(ln)
                if mm:
                    norm = _normalize_mac(mm.group(0))
                    if norm:
                        candidates.append(norm)

    if not candidates:
        return None

    # Rank: prefer known OUIs, then ones that already have colons
    def rank(mac: str) -> tuple[int, int]:
        return (1 if mac[:8] in OUI_PREFER else 0, 1 if ":" in mac else 0)

    candidates.sort(key=rank, reverse=True)
    return candidates[0]

def extract_mac(text: str) -> Optional[str]:
    """Global regex over the whole text as a fallback."""
    m = MAC_RE.search(_cleanup_hexish(text or ""))
    return _normalize_mac(m.group(0)) if m else None

def _is_probable_rsn(token: str) -> bool:
    s = (token or "").strip().upper()
    if len(s) < 8 or len(s) > 24:
        return False
    if MAC_RE.match(s):
        return False
    if s in RSN_STOPWORDS:
        return False
    return sum(ch.isdigit() for ch in s) >= 3

def extract_rsn(text: str, lines: Optional[List[str]] = None) -> Optional[str]:
    if not text:
        return None
    m = RSN_LABELED_RE.search(text)
    if m:
        cand = m.group(1).upper()
        if _is_probable_rsn(cand):
            return cand
    if lines:
        for ln in lines:
            m2 = RSN_LABELED_RE.search(ln)
            if m2:
                cand = m2.group(1).upper()
                if _is_probable_rsn(cand):
                    return cand
    tokens = [t.upper() for t in RSN_TOKEN_RE.findall((text or "").upper())]
    candidates = [t for t in tokens if _is_probable_rsn(t)]
    if lines:
        for ln in lines:
            tokens_l = [t.upper() for t in RSN_TOKEN_RE.findall(ln.upper())]
            candidates.extend([t for t in tokens_l if _is_probable_rsn(t)])
    if not candidates:
        return None
    def score(tok: str) -> tuple[int, int]:
        return (sum(ch.isdigit() for ch in tok), len(tok))
    candidates.sort(key=score, reverse=True)
    return candidates[0]

ANGLE_RE = re.compile(
    r"\b(?P<deg>\d{1,3})\s*°?\s*(?P<dir>N|NE|E|SE|S|SW|W|NW)?\b",
    re.IGNORECASE,
)

def extract_angle(text: str) -> Tuple[Optional[int], Optional[str]]:
    if not text:
        return (None, None)
    fallback = None
    for m in ANGLE_RE.finditer(text):
        deg = int(m.group("deg"))
        if 0 <= deg <= 360:
            direction = (m.group("dir") or "").upper() or None
            if direction:
                return (deg, direction)
            if fallback is None:
                fallback = (deg, None)
    return fallback if fallback else (None, None)

# ---------- TEAM API shims expected by validate.py ----------
def _prefer_easyocr_lines(img: np.ndarray) -> list[str]:
    try:
        rgb = cv2.cvtColor(img, cv2.COLOR_BGR2RGB)
        reader = _easyocr()
        lines = reader.readtext(rgb, detail=0) or []
        return [str(x).strip() for x in lines if str(x).strip()]
    except Exception:
        txt = ocr_text(img) or ""
        return [ln.strip() for ln in txt.splitlines() if ln.strip()]

def ocr_text_block(img: np.ndarray) -> str:
    lines = _prefer_easyocr_lines(img)
    # IMPORTANT: keep line breaks so label/keyword detection remains line-aware
    return "\n".join(lines).strip()

def ocr_single_line(img: np.ndarray) -> str:
    lines = _prefer_easyocr_lines(img)
    if not lines:
        return ""
    return max(lines, key=len)

def extract_label_fields(text: str) -> Dict[str, Optional[str]]:
    """
    Unchanged signature. More robust MAC extraction inside.
    Prefers MAC keyword lines; falls back to global; fixes OCR confusions.
    """
    # Keep lines for line-aware MAC keyword checks
    lines = [ln.strip() for ln in (text or "").splitlines() if ln.strip()]

    mac = _extract_mac_from_lines(lines) if lines else None
    if not mac:
        # global fallback across the whole text after cleanup
        mac = extract_mac(text)

    rsn = extract_rsn(text, lines=lines)

    return {"macId": mac, "rsn": rsn}

def extract_azimuth(text: str) -> Dict[str, Optional[object]]:
    deg, ddir = extract_angle(text or "")
    return {"azimuthDeg": deg, "azimuthDir": ddir}
