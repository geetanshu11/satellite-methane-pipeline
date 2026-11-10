FROM python:3.12-slim AS base
ENV PYTHONUNBUFFERED=1 PYTHONDONTWRITEBYTECODE=1
WORKDIR /app
COPY pyproject.toml README.md requirements-lock.txt ./
COPY src ./src
RUN pip install --no-cache-dir -c requirements-lock.txt .
COPY config ./config
COPY demo ./demo
RUN mkdir -p /data /reports
ENTRYPOINT ["python", "-m", "methane"]
CMD ["--output", "/reports", "replay", "--source", "/archives"]

FROM base AS test
RUN pip install --no-cache-dir -c requirements-lock.txt '.[dev]'
COPY tests ./tests
COPY archives ./archives
COPY source_manifests ./source_manifests
ENTRYPOINT ["python", "-m", "pytest"]
CMD ["-q"]

FROM base AS runtime
