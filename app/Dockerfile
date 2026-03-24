FROM python:3.12-slim

ENV PYTHONUNBUFFERED=1

WORKDIR /app

# Install system deps for asyncpg and cryptg
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc libpq-dev && \
    rm -rf /var/lib/apt/lists/*

COPY pyproject.toml .
COPY alembic.ini .
COPY src/ src/

# Use Privoxy proxy for pip if pypi.org is blocked
ENV PIP_PROXY=http://192.168.2.140:8118
RUN pip install --no-cache-dir --proxy "$PIP_PROXY" . ; \
    unset PIP_PROXY

EXPOSE 8002

CMD ["sh", "-c", "alembic upgrade head && python -m agent_memory_mcp"]
