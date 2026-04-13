# ---------------------------------------------------------------------------
# Stage 1: dependency builder
# ---------------------------------------------------------------------------
FROM python:3.11-slim AS builder

WORKDIR /build

# Create an isolated venv so deps are easy to copy
RUN python -m venv /opt/venv
ENV PATH="/opt/venv/bin:$PATH"

# Install runtime deps only (no dev extras)
COPY pyproject.toml .
RUN pip install --no-cache-dir --upgrade pip \
 && pip install --no-cache-dir .

# ---------------------------------------------------------------------------
# Stage 2: lean runtime image
# ---------------------------------------------------------------------------
FROM python:3.11-slim

WORKDIR /app

# Bring in the venv from the builder
COPY --from=builder /opt/venv /opt/venv
ENV PATH="/opt/venv/bin:$PATH"

# Application source — imports use "from src.X" so the whole src/ tree must live
# at /app/src alongside the PYTHONPATH root /app.
COPY src/ src/

ENV PYTHONUNBUFFERED=1
ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONPATH=/app

# Expose the metrics / health API port
EXPOSE 9090

# Default entrypoint: FastAPI health + metrics server.
# The consumer deployment overrides this in its k8s args.
CMD ["uvicorn", "src.api:app", "--host", "0.0.0.0", "--port", "9090", "--workers", "1"]
