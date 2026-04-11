FROM python:3.14-slim AS builder

COPY --from=ghcr.io/astral-sh/uv:latest /uv /uvx /bin/

WORKDIR /app

COPY pyproject.toml uv.lock ./

RUN uv sync --frozen --no-dev --no-install-project

FROM python:3.14-slim

WORKDIR /app

RUN useradd -m -r -s /bin/false appuser \
    && mkdir -p /data/artifacts /data/symbols \
    && chown -R appuser:appuser /app /data

COPY --from=builder /app/.venv /app/.venv

COPY app.py db.py releases.py reconciler.py ./

RUN chown -R appuser:appuser /app

USER appuser

EXPOSE 5000

CMD ["/app/.venv/bin/uvicorn", "app:app", "--host", "0.0.0.0", "--port", "5000"]
