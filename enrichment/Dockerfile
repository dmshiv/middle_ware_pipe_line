# =============================================================================
# ENRICHMENT SERVICE — Docker Image
# =============================================================================
FROM python:3.11-slim AS builder
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir --prefix=/install -r requirements.txt

FROM python:3.11-slim
WORKDIR /app
COPY --from=builder /install /usr/local
COPY . .

RUN useradd -m appuser && chown -R appuser /app
USER appuser

CMD ["python", "-u", "enrichment_service.py"]
