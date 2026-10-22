FROM python:3.12-slim

RUN apt-get update && apt-get install -y --no-install-recommends \
        curl \
        git \
    && rm -rf /var/lib/apt/lists/*

COPY --from=ghcr.io/astral-sh/uv:latest /uv /usr/local/bin/uv

WORKDIR /app

# Install orchestration package
COPY pyproject.toml .
COPY orchestration/ ./orchestration/
RUN uv pip install --system --no-cache -e "." dagster-webserver

# Install dbt project dependencies and pre-build manifest
COPY transform/ ./transform/
RUN cd /app/transform/evd_transform && dbt deps --profiles-dir ..
RUN cd /app/transform/evd_transform && \
    PG_USER=dummy \
    PG_PASSWORD=dummy \
    dbt parse --profiles-dir .. --target dev --quiet

COPY workspace.yaml /app/workspace.yaml

ENV DAGSTER_HOME=/dagster_home
EXPOSE 3001
