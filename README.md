# Flight Sentinel – Ingestion (Python)

This is the **ingestion microservice** for the **Flight Sentinel** project.  
It **simulates live flight events** by reading from CSV files, normalizing the data, and publishing messages into **Apache Kafka**.  

The data generated here is consumed by the **Java Quarkus Delay API** service, which processes flight delays and exposes REST endpoints.

---

## 🚀 What this service does
1. Reads CSV files containing flight data (ID, carrier, origin, destination, scheduled/actual departure).
2. Normalizes and enriches the data:
   - Uppercases carrier and airport codes.
   - Converts timestamps to ISO-8601 UTC.
3. Publishes valid messages to Kafka (`flights.raw`).
4. Sends invalid/bad records to a **Dead Letter Queue** (`flights.dlq`).
5. Exposes **Prometheus metrics** at `http://localhost:9108/metrics`.

This service is **simulation-first**:  
- Initially, it ingests **sample CSV files**.  
- Later, the source can be swapped to a **live API** (e.g., aviation data, GPS/routing feeds like TomTom).  

---

## 🧩 Project Structure
    flight-sentinel-ingestion-py/
    ingestion_py/
    sources/ # data sources (CSV reader)
    transforms/ # normalizers/validators
    sinks/ # Kafka publisher
    dlq/ # Dead Letter Queue
    config.py # env + settings
    metrics.py # Prometheus metrics
    main.py # entrypoint
    sample_data/ # example CSV flight data
    requirements.txt
    docker/Dockerfile
    README.md

---

## 🔌 Relationship to Java Quarkus Delay API

This ingestion service is **producer**.  
The **Quarkus Delay API** (Java) is the **consumer**.

- **Producer (this project)**  
  - Publishes flight events into `flights.raw` topic.  
  - Ensures retries, DLQ, and monitoring.  

- **Consumer (Quarkus delay-api)**  
  - Subscribes to `flights.raw`.  
  - Stores data in Postgres.  
  - Exposes REST endpoints like `/delays/{flight_id}`.  
  - Exposes its own metrics and integrates with Grafana.  

**Together**, they form an end-to-end pipeline:
