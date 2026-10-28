FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    STATE_DIR=/state \
    INITIAL_SERVERS_FILE=/app/servers.json \
    DEFAULT_REQUESTS_FILE=/app/requests.csv

WORKDIR /app
COPY backend/requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r /app/requirements.txt

COPY backend/app /app/app
COPY servers.json /app/servers.json
COPY requests.csv /app/requests.csv
COPY validate_run.py /app/validate_run.py
COPY validate_run_shared.py /app/validate_run_shared.py

EXPOSE 8000
CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000", "--workers", "1"]
