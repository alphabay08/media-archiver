FROM python:3.11-slim

WORKDIR /app

RUN apt-get update && apt-get install -y --no-install-recommends \
    ffmpeg \
    curl \
    && rm -rf /var/lib/apt/lists/*

COPY hf_worker/requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt

COPY core/      /app/core/
COPY modules/   /app/modules/
COPY hf_worker/ /app/hf_worker/

RUN mkdir -p /app/data/tmp /app/logs \
    && chmod -R 777 /app/data /app/logs

ENV PYTHONUNBUFFERED=1
ENV PYTHONPATH=/app
ENV DATA_DIR=/app/data

EXPOSE 10000

CMD ["sh", "-c", "uvicorn hf_worker.app:app --host 0.0.0.0 --port ${PORT:-10000}"]
