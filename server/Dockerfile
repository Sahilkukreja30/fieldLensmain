# ===== Base =====
FROM python:3.11-slim

ENV PIP_NO_CACHE_DIR=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    HF_HOME=/tmp/.cache/huggingface \
    EASYOCR_DIR=/tmp/.easyocr \
    TORCH_HOME=/tmp/torch \
    LOCAL_STORAGE_DIR=/tmp/uploads \
    PORT=7860

# System deps for opencv/easyocr/pytesseract
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential gcc \
    libgl1 libglib2.0-0 libsm6 libxext6 libxrender1 \
    tesseract-ocr curl \
 && rm -rf /var/lib/apt/lists/*

# ===== App =====
WORKDIR /workspace

# Python deps
COPY requirements.txt ./requirements.txt
RUN pip install -r requirements.txt

# Source code
# Your repo should have an `app/` folder at the root (app/main.py, app/routes, app/services, etc.)
COPY app ./app

# Create uploads directory (backed by /tmp so it's writable)
RUN mkdir -p ${LOCAL_STORAGE_DIR}

EXPOSE 7860

# Healthcheck to surface failures in the HF UI
HEALTHCHECK --interval=30s --timeout=5s --retries=3 \
  CMD curl -fsS http://127.0.0.1:${PORT}/health || exit 1

# Log a banner then run uvicorn on the port HF provides
CMD bash -lc 'echo "===== Application Startup at $(date -u +%F\ %T) ====="; uvicorn app.main:app --host 0.0.0.0 --port ${PORT}'
