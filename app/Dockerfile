FROM python:3.12-slim

ENV PYTHONUNBUFFERED=1

WORKDIR /app

# Install system deps for asyncpg and cryptg
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc libpq-dev && \
    rm -rf /var/lib/apt/lists/*

# Install Python deps first (cached — only rebuilds when requirements.txt changes)
COPY requirements.txt .
ENV PIP_PROXY=http://192.168.2.140:8118
RUN pip install --no-cache-dir --proxy "$PIP_PROXY" -r requirements.txt

# Copy source code + config (changes here don't re-install deps)
COPY pyproject.toml alembic.ini ./
COPY src/ src/
RUN pip install --no-deps --no-cache-dir .

EXPOSE 8002

CMD ["sh", "-c", "alembic upgrade head && python -m agent_memory_mcp"]
