# =============================================================================
# TRANSACTION SIMULATOR — Docker Image
# =============================================================================
# Multi-stage build for small image size.
# Stage 1: Install Python dependencies
# Stage 2: Copy only what's needed (no pip cache, no build tools)
# =============================================================================

FROM python:3.11-slim AS builder
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir --prefix=/install -r requirements.txt

FROM python:3.11-slim
WORKDIR /app
COPY --from=builder /install /usr/local
COPY . .

# Run as non-root user (security best practice)
RUN useradd -m appuser && chown -R appuser /app
USER appuser

CMD ["python", "-u", "transaction_generator.py"]
