# NYC Taxi Data Engineering Pipeline

An end-to-end data engineering project that orchestrates a data pipeline with Apache Airflow, versions datasets with DVC and MinIO, transforms data with dbt, and stores analytical models in DuckDB.

## Architecture

```text
NYC Taxi Parquet dataset
          |
          v
   DVC + MinIO (S3)
          |
          v
 Apache Airflow
   1. Pull dataset
   2. Validate Parquet file
   3. Run dbt build
          |
          v
        DuckDB
   Staging and mart models
```

## Technology stack

- **Apache Airflow** — workflow orchestration using LocalExecutor.
- **DVC** — dataset versioning and retrieval.
- **MinIO** — S3-compatible object storage for DVC.
- **dbt Core** — SQL transformations, model dependencies, and data tests.
- **DuckDB** — analytical database.
- **Docker Compose** — local environment and service orchestration.

## Pipeline

The `nyc_taxi_pipeline` DAG executes three sequential tasks:

1. `dvc_pull`: retrieves the versioned January 2025 Yellow Taxi dataset from MinIO.
2. `validate_parquet`: checks that the expected Parquet file exists and is not empty.
3. `dbt_build`: builds the dbt models and runs their associated tests.

The dbt project includes a staging model for Yellow Taxi trip data and a `taxi_summary` mart that aggregates trip metrics by payment type.

## Validation

The pipeline has been executed successfully in Airflow. The dbt models were built, and the configured data quality tests passed.

## Run locally

Prerequisites: Docker Desktop, Git, Python, and DVC with its S3 support.

1. Clone the repository.
2. Configure local MinIO credentials without committing secrets.
3. Start MinIO and initialize the DVC remote.
4. Build and start the Airflow environment with Docker Compose.
5. Open the Airflow UI at `http://localhost:8080`.
6. Trigger the `nyc_taxi_pipeline` DAG.

The Airflow environment uses the internal Docker hostname `minio:9000` to access MinIO. Host-side DVC commands use `localhost:9000`.

## Project structure

```text
.
├── config/dbt/          # dbt profile configuration
├── dags/                # Airflow DAGs
├── data/raw/            # DVC-managed source dataset
├── dbt/nyc_taxi/        # dbt models and tests
├── Dockerfile.airflow   # Airflow image with DVC and dbt
├── docker-compose.yml   # MinIO service
└── docker-compose.airflow.yml
```

## Security

Local credentials and environment files are excluded from version control. Do not commit `.env` or `.dvc/config.local`.

## Future improvements

- Add automated pipeline tests.
- Add data quality checks for additional business rules.
- Add CI to validate the dbt project and pipeline configuration.
- Document data lineage and model definitions in more detail.
