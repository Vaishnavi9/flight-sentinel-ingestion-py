# syntax=docker/dockerfile:1
FROM python:3.11-slim
WORKDIR /app
COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt
COPY ingestion_py ./ingestion_py
COPY sample_data ./sample_data
ENV PYTHONUNBUFFERED=1
CMD ["python", "-m", "ingestion_py.main", "--csv", "/app/sample_data/flights_sample.csv"]
