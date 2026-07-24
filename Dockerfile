FROM python:3.12-slim

WORKDIR /app

COPY pyproject.toml README.md ./
COPY src ./src
COPY scripts ./scripts

RUN pip install --no-cache-dir --upgrade pip \
 && pip install --no-cache-dir .

ENV PYTHONUNBUFFERED=1
ENV LOG_LEVEL=INFO

CMD ["job-queue-coordinator"]
