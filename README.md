# Real-Time Flight Telemetry Data Pipeline

## Overview
This project demonstrates a complete, end-to-end Big Data streaming pipeline built to process and visualize flight telemetry data in real-time. The architecture ingests simulated flight metrics, processes them dynamically using PySpark micro-batching, and visualizes the enriched data on a live Grafana dashboard.

**Author:** Ram Sudan, B.Tech Artificial Intelligence and Data Science, VIPS-TC

## Architecture & Tech Stack
The pipeline follows a decoupled, robust architecture using the following technologies:
*   **Data Ingestion:** Python (Custom Producer)
*   **Message Broker:** Apache ActiveMQ (Dockerized)
*   **Stream Processing:** Apache Spark (PySpark)
*   **Time-Series Database:** InfluxDB (Dockerized)
*   **Data Visualization:** Grafana (Dockerized)

### Pipeline Flow
1.  **Producer (`flight_producer.py`):** Generates high-frequency, simulated flight data (altitude, speed in km/h, roll, and timestamps) and publishes it to the `raw_flight_data` queue in ActiveMQ.
2.  **Stream Processor (`pyspark_processor.py`):** A PySpark engine listens to the raw queue, batches the data, and performs on-the-fly transformations:
    *   Converts speed from kilometers per hour (km/h) to Knots.
    *   Evaluates altitude thresholds to assign a real-time `Flight Status` (`CRUISING` vs. `ASCENDING/DESCENDING`).
    *   Publishes the enriched data to the `processed_flight_data` queue.
3.  **Consumer (`pyspark_consumer.py`):** Subscribes to the processed queue and writes the time-series data points directly into InfluxDB.
4.  **Visualization:** Grafana connects to InfluxDB via a Flux query to display the live flight metrics, color-coded by the dynamically calculated flight status.

---

## Infrastructure & Credentials Reference

Below are the default connection details and credentials used across the containerized stack. Ensure these match your local configuration and environment variables.

### 1. Message Broker (Apache ActiveMQ)
*   **Web Console URL:** `http://localhost:8162`
*   **STOMP Wire Port:** `61614`
*   **Username:** `admin`
*   **Password:** `admin`

### 2. Time-Series Database (InfluxDB v2)
*   **Web UI URL:** `http://localhost:8087`
*   **Internal Network URL (for Grafana):** `http://pyspark_influxdb:8086`
*   **Organization:** `internship`
*   **Bucket Name:** `pyspark_data`
*   **Authentication Token:** `pyspark-secret-token`

### 3. Analytics & Visualization (Grafana)
*   **Web UI URL:** `http://localhost:3001`
*   **Default Username:** `admin`
*   **Default Password:** `adminpassword`

---

## Setup & Execution

### 1. Start the Infrastructure
Launch the ActiveMQ, InfluxDB, and Grafana containers:
```bash
docker compose up -d