# app/services/imaging.py (Final Production Version)

import cv2
import numpy as np
from PIL import Image, UnidentifiedImageError
import io

def load_bgr(data: bytes) -> np.ndarray | None:
    """
    Robustly decodes image bytes into a BGR numpy array for OpenCV.
    It uses the lenient Pillow library and ensures data types and memory
    layout are correct for OpenCV compatibility.
    """
    try:
        image_pil = Image.open(io.BytesIO(data))
        image_pil = image_pil.convert('RGB')
        
        image_rgb = np.array(image_pil, dtype=np.uint8)
        
        # Ensure the memory layout is C-contiguous for OpenCV
        image_rgb_contiguous = np.ascontiguousarray(image_rgb)
        
        # Convert RGB to BGR for OpenCV
        image_bgr = cv2.cvtColor(image_rgb_contiguous, cv2.COLOR_RGB2BGR)
        
        return image_bgr
        
    except Exception:
        return None

# --- Other Image Analysis Functions ---
def to_gray(img):
    return cv2.cvtColor(img, cv2.COLOR_BGR2GRAY)

def variance_of_laplacian(img):
    gray = to_gray(img)
    return float(cv2.Laplacian(gray, cv2.CV_64F).var())

def largest_quadrilateral_skew_deg(img) -> float | None:
    gray = to_gray(img)
    gray = cv2.bilateralFilter(gray, 5, 75, 75)
    edges = cv2.Canny(gray, 50, 150)
    cnts, _ = cv2.findContours(edges, cv2.RETR_LIST, cv2.CHAIN_APPROX_SIMPLE)
    quad = None
    area_best = 0
    for c in cnts:
        peri = cv2.arcLength(c, True)
        approx = cv2.approxPolyDP(c, 0.03 * peri, True)
        if len(approx) == 4:
            area = cv2.contourArea(approx)
            if area > area_best:
                area_best = area
                quad = approx
    if quad is None:
        return None
    pts = quad.reshape(-1, 2).astype(np.float32)

    def angle(p, q):
        v = q - p
        ang = np.degrees(np.arctan2(v[1], v[0]))
        return abs(((ang + 90) % 90) - 45)

    a = angle(pts[0], pts[1]); b = angle(pts[1], pts[2])
    c = angle(pts[2], pts[3]); d = angle(pts[3], pts[0])
    return float(np.mean([a, b, c, d]))

def has_big_circle(img) -> bool:
    g = to_gray(img)
    g = cv2.medianBlur(g, 5)
    circles = cv2.HoughCircles(
        g, cv2.HOUGH_GRADIENT, dp=1.2, minDist=80,
        param1=80, param2=40, minRadius=40, maxRadius=0
    )
    return circles is not None

def crop_label_region(bgr: np.ndarray, max_pad: int = 20) -> np.ndarray:
    """
    Heuristic crop: find the largest text/edge-dense region and return it.
    Falls back to the original image if nothing useful is found.
    """
    if bgr is None or bgr.size == 0:
        return bgr
    gray = cv2.cvtColor(bgr, cv2.COLOR_BGR2GRAY)
    gray = cv2.GaussianBlur(gray, (5, 5), 0)

    # Strong edges → contours
    thr = cv2.adaptiveThreshold(
        gray, 255, cv2.ADAPTIVE_THRESH_GAUSSIAN_C,
        cv2.THRESH_BINARY_INV, 31, 11
    )
    contours, _ = cv2.findContours(thr, cv2.RETR_EXTERNAL, cv2.CHAIN_APPROX_SIMPLE)
    if not contours:
        return bgr

    # Choose the largest reasonable contour (ignore very small)
    H, W = bgr.shape[:2]
    area_min = (H * W) * 0.03
    biggest = max(contours, key=cv2.contourArea)
    if cv2.contourArea(biggest) < area_min:
        return bgr

    x, y, w, h = cv2.boundingRect(biggest)
    x0 = max(0, x - max_pad); y0 = max(0, y - max_pad)
    x1 = min(W, x + w + max_pad); y1 = min(H, y + h + max_pad)
    roi = bgr[y0:y1, x0:x1]
    return roi if roi.size > 0 else bgr
    