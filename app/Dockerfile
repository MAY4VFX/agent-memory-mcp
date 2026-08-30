FROM python:3.12-slim

ENV PYTHONUNBUFFERED=1

WORKDIR /app

# Install system deps for asyncpg and cryptg.
# deb.debian.org (Fastly) is unroutable from the build host, so apt goes through
# the same local Privoxy that pip below already uses.
ENV APT_PROXY=http://192.168.2.140:8118
RUN printf 'Acquire::http::Proxy "%s";\n' "$APT_PROXY" > /etc/apt/apt.conf.d/01proxy && \
    apt-get update && apt-get install -y --no-install-recommends \
    gcc libpq-dev && \
    rm -rf /var/lib/apt/lists/* /etc/apt/apt.conf.d/01proxy

# Install Python deps first (cached — only rebuilds when requirements.txt changes)
COPY requirements.txt .
ENV PIP_PROXY=http://192.168.2.140:8118
RUN pip install --no-cache-dir --proxy "$PIP_PROXY" -r requirements.txt

# Copy source code + config (changes here don't re-install deps)
COPY pyproject.toml alembic.ini ./
COPY src/ src/
RUN pip install --no-deps --no-cache-dir .

EXPOSE 8002

# AMM_ROLE=listener → dedicated read-receipt listener (own loop, isolated from
# the bot). Anything else → the bot/API/scheduler process (runs migrations).
CMD ["sh", "-c", "if [ \"$AMM_ROLE\" = listener ]; then python -m agent_memory_mcp.listener_main; else alembic upgrade head && python -m agent_memory_mcp; fi"]
