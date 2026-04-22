FROM python:3.11-slim

# System dependencies for OCR + PDF processing
RUN apt-get update && apt-get install -y --no-install-recommends \
    tesseract-ocr \
    poppler-utils \
    libglib2.0-0 \
    libsm6 \
    libxext6 \
    libxrender-dev \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

# Railway injects PORT env var
EXPOSE ${PORT}

# Shell form so $PORT is expanded at runtime
CMD uvicorn server:app --host 0.0.0.0 --port $PORT
