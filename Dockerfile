# Dockerfile
FROM python:3.12-slim

WORKDIR /app
COPY *.py .
COPY requirements.txt .

RUN pip install --no-cache-dir -r requirements.txt

RUN mkdir -p /app/logs

ENTRYPOINT ["python", "main.py"]
CMD ["--cik-file", "/app/ciks.txt"]