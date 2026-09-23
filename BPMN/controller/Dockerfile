FROM python:3.11-slim

WORKDIR /app

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    CAMUNDA_URL=http://camunda:8080/engine-rest \
    CAMUNDA_USER=demo \
    CAMUNDA_PASSWORD=demo \
    WORKER_TOPIC=do-work \
    POLL_INTERVAL_SECONDS=2.0 \
    WORK_DURATION_SECONDS=1.5

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY main.py .

EXPOSE 8000

HEALTHCHECK CMD python -c "import urllib.request,sys; urllib.request.urlopen('http://localhost:8000/health',timeout=2)" || exit 1

CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000", "--log-level", "info"]
