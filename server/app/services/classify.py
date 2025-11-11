from typing import Literal, Optional

from app.services.imaging import has_big_circle
from app.utils import DEGREE_RE

PhotoType = Literal["LABELLING", "AZIMUTH"]


def classify(img, ocr_hint: Optional[str] = None) -> PhotoType:
    """
    Heuristic:
      - If OCR hint contains degree-ish tokens → AZIMUTH
      - Else if big circle → AZIMUTH
      - Else → LABELLING
    """
    if ocr_hint and DEGREE_RE.search(ocr_hint):
        return "AZIMUTH"
    if has_big_circle(img):
        return "AZIMUTH"
    return "LABELLING"
