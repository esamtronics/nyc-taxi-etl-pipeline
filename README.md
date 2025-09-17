# NYC Taxi Data ETL & Analytics Pipeline

This project implements a complete, containerized ETL (Extract, Transform, Load) pipeline using Apache Spark and Docker. It processes raw NYC Yellow Taxi trip data, enriches it with location details, and loads it into a PostgreSQL database, making it ready for analytics and business intelligence.

## Technology Stack

* **Containerization**: Docker, Docker Compose
* **Data Processing**: Apache Spark (with PySpark)
* **Database**: PostgreSQL
* **DB Management**: pgAdmin
* **Orchestration Script**: Bash

## Architecture & Workflow

The pipeline orchestrates several services using Docker Compose to create a reproducible environment. The data flow is as follows:

1.  **Extract**: A Spark job reads raw taxi trip data (`.parquet`) and location lookup data (`.csv`).
2.  **Transform**: In Spark, the trip data is joined with location data to replace IDs with readable names (Borough, Zone). Columns are cleaned and renamed for consistency.
3.  **Load**: The transformed DataFrame is written directly to a `nyc_taxi_trips` table in the PostgreSQL database via a JDBC connection.

## How to Run

### Prerequisites

* Docker
* Docker Compose

### Steps

1.  **Clone the repository:**
    ```bash
    git clone https://github.com/esamtronics/nyc-taxi-etl-pipeline.git
    cd nyc-taxi-etl-pipeline
    ```
2.  **Place data files:**
    * Add `yellow_tripdata_2024-01.parquet` to the `./data/` directory.
    * Add `location_lookup.csv` to the `./data/` directory.
3.  **Deploy the pipeline:**
    Execute the deployment script to build and run all services in the background.
    ```bash
    bash deploy.sh
    ```
4.  **Access the data:**
    * Connect to the PostgreSQL database on `localhost:5432`.
    * Use the pgAdmin service at `http://localhost:5050` to manage and query the database.

## Project Structure

* **`docker-compose.yml`**: The main configuration file that defines the Spark cluster, PostgreSQL database, and pgAdmin services.
* **`deploy.sh`**: A utility script to start the application stack using `docker-compose up -d`.
* **`spark-jobs/NYC_Taxi_ETL.py`**: The production-ready PySpark script containing all transformation logic.
* **`postgres-init/init.sql`**: An initialization script that automatically creates the database table on the first run.
* **`NYC_Taxi_ETL_and_Analytics_with_PySpark.ipynb`**: A Jupyter Notebook for interactive development, data exploration, and analysis.Please provide the complete README content as a single block of Markdown code that I can copy.

