FROM python:3.11-slim

COPY --from=ghcr.io/astral-sh/uv:0.11.32 /uv /uvx /bin/

ENV UV_COMPILE_BYTECODE=1
ENV UV_LINK_MODE=copy

WORKDIR /app

COPY pyproject.toml uv.lock ./
RUN uv sync --frozen --no-dev

COPY main.py main_boe_rates.py init_db.py ./
COPY lib_land_registry_data/ ./lib_land_registry_data/

ENV PATH="/app/.venv/bin:$PATH"
ENV PYTHONPATH=/app
ENV PYTHONUNBUFFERED=1

# the file log handler writes to the current working directory,
# so run from /app/logs to keep log files on the mounted volume
RUN mkdir -p /app/logs
WORKDIR /app/logs

CMD ["sh", "-c", "python /app/init_db.py && python /app/main.py"]
