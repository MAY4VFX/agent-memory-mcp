FROM python:3.12-slim

ENV PYTHONUNBUFFERED=1

WORKDIR /app

# Install system deps for asyncpg and cryptg
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc libpq-dev && \
    rm -rf /var/lib/apt/lists/*

# Install Python deps first (cached layer — only rebuilds when pyproject.toml changes)
COPY pyproject.toml .
RUN pip install --no-cache-dir --proxy "http://192.168.2.140:8118" . 2>/dev/null || \
    pip install --no-cache-dir .

# Copy source code (changes here don't re-install deps)
COPY src/ src/
COPY alembic.ini .

# Re-install in editable-like mode so the package points to fresh source
RUN pip install --no-deps --no-cache-dir .

EXPOSE 8002

CMD ["sh", "-c", "alembic upgrade head && python -m agent_memory_mcp"]
