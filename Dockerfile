FROM python:3.12-slim AS builder
WORKDIR /build
RUN pip install --no-cache-dir uv==0.8.14
COPY pyproject.toml uv.lock* ./
COPY src ./src
RUN uv build --wheel && uv venv /opt/venv && uv pip install --python /opt/venv/bin/python dist/*.whl

FROM python:3.12-slim AS runtime
RUN groupadd --gid 10001 runner && useradd --uid 10001 --gid runner --no-create-home runner
COPY --from=builder /opt/venv /opt/venv
ENV PATH="/opt/venv/bin:$PATH" PYTHONUNBUFFERED=1
USER 10001:10001
EXPOSE 8080
HEALTHCHECK --interval=10s --timeout=3s --retries=5 \
  CMD ["python", "-c", "import urllib.request; urllib.request.urlopen('http://127.0.0.1:8080/healthz', timeout=2)"]
CMD ["uvicorn", "flow_runner.api:app", "--host", "0.0.0.0", "--port", "8080"]

