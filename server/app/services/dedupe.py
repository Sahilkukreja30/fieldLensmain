import cv2
import numpy as np


def phash(img) -> str:
    # Perceptual hash via DCT (8x8 of 32x32)
    resized = cv2.resize(img, (32, 32))
    gray = cv2.cvtColor(resized, cv2.COLOR_BGR2GRAY)
    dct = cv2.dct(np.float32(gray))
    dct_low = dct[:8, :8]
    med = np.median(dct_low)
    bits = (dct_low > med).flatten()
    return ''.join('1' if b else '0' for b in bits)


def hamming(a: str, b: str) -> int:
    return sum(ch1 != ch2 for ch1, ch2 in zip(a, b))
