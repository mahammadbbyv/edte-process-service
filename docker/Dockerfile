FROM python:3.12-slim AS builder
WORKDIR /build
COPY pyproject.toml ./
COPY src src
COPY schema schema
RUN pip wheel --wheel-dir /wheels .

FROM python:3.12-slim
WORKDIR /app
COPY --from=builder /wheels /wheels
RUN pip install --no-cache-dir --no-index --find-links=/wheels edte-process-service && rm -rf /wheels
COPY examples examples
USER 65534:65534
EXPOSE 8000
CMD ["uvicorn", "src.main:app", "--host", "0.0.0.0", "--port", "8000"]
