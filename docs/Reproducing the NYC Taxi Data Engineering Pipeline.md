# Reproducing the NYC Taxi Data Engineering Pipeline

This guide explains how to reproduce the NYC Taxi data engineering project from a fresh Git clone to a successful Apache Airflow pipeline run.

The project uses DVC and MinIO for dataset versioning and storage, dbt for SQL transformations, DuckDB as the analytical database, and Apache Airflow for orchestration.

## 1. Architecture

```text
NYC TLC Yellow Taxi Dataset
            |
            v
       Parquet file
            |
            v
       DVC + MinIO
   Dataset versioning
            |
            v
      Apache Airflow
            |
            +-- 1. dvc_pull
            |
            +-- 2. validate_parquet
            |
            +-- 3. dbt_build
                      |
                      v
                  dbt models
                      |
                      v
                    DuckDB
```

### Technology stack

* Docker Desktop and Docker Compose
* MinIO for S3-compatible object storage
* DVC for data versioning
* Apache Airflow 3.3.2 with LocalExecutor
* PostgreSQL 16 for Airflow metadata
* dbt Core 1.12.5
* dbt-duckdb 1.11.0
* DuckDB for analytical queries

## 2. Prerequisites

Install the following tools:

* Git
* Docker Desktop
* Python 3.12
* A terminal such as Bash or Zsh

Verify the installations:

```bash
git --version
python3 --version
docker --version
docker compose version
```

Make sure Docker Desktop is running before continuing.

## 3. Clone the repository

```bash
git clone https://github.com/isabellaguiardefaria/nyc-taxi-data-engineering.git

cd nyc-taxi-data-engineering
```

Check the repository:

```bash
git status
ls
```

The repository contains the pipeline code, Docker configuration, dbt project, Airflow DAG, and DVC metadata.

However, cloning the repository does not automatically download the Parquet dataset or provide access to the original MinIO instance.

## 4. Start MinIO

The root `docker-compose.yml` defines the MinIO service and its persistent volume.

Start MinIO:

```bash
docker compose up -d minio
```

Check the container:

```bash
docker compose ps
```

MinIO exposes two ports:

* S3 API: http://localhost:9000
* Web console: http://localhost:9001

For the original local development configuration, the Compose file uses the demo credentials `minioadmin` and `minioadmin`. Check the actual configuration before logging in, and do not reuse these credentials in a public or production environment.

Open the MinIO console and create a bucket named:

```text
dvc-storage
```

If the bucket already exists, reuse it.

**Important:** MinIO data is persisted in a Docker volume. Do not run `docker compose down -v` during normal setup, because it may delete persistent data.

## 5. Configure DVC on the host

Create a Python virtual environment:

```bash
python3 -m venv .venv
source .venv/bin/activate
```

Install DVC with S3 support:

```bash
python -m pip install --upgrade pip
python -m pip install "dvc[s3]==3.67.1"
```

Verify the installation:

```bash
dvc --version
```

The project has two DVC remotes because the MinIO endpoint depends on where the command runs.

| Remote         | Endpoint                | Used by           |
| -------------- | ----------------------- | ----------------- |
| `minio`        | `http://localhost:9000` | Host machine      |
| `minio_docker` | `http://minio:9000`     | Docker containers |

Inside Docker, `minio` resolves to the MinIO service through the Compose network. The hostname `localhost` inside a container would refer to that container itself, not the MinIO service.

### Configure local credentials

The repository intentionally excludes `.dvc/config.local` from version control.

For the local demo MinIO, configure the host remote:

```bash
dvc remote modify --local minio access_key_id minioadmin

dvc remote modify --local minio secret_access_key minioadmin
```

Configure the Docker remote as well:

```bash
dvc remote modify --local minio_docker access_key_id minioadmin

dvc remote modify --local minio_docker secret_access_key minioadmin
```

Use the credentials configured for your own MinIO instance if they differ.

Verify that the credentials file remains ignored:

```bash
git check-ignore .dvc/config.local
```

Do not commit this file.

## 6. Download and prepare the Parquet dataset

The project uses the NYC TLC Yellow Taxi dataset for January 2025.

Expected local path:

```text
data/raw/yellow_tripdata_2025-01.parquet
```

The original dataset was approximately 56.4 MiB.

### Download the dataset

If you are setting up your own MinIO bucket, download the public dataset:

```bash
mkdir -p data/raw
```

```bash
curl -fL \
  "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2025-01.parquet" \
  -o data/raw/yellow_tripdata_2025-01.parquet
```

Verify that the file exists and is not empty:

```bash
ls -lh data/raw/yellow_tripdata_2025-01.parquet

test -s data/raw/yellow_tripdata_2025-01.parquet \
  && echo "Parquet file found"
```

### Add the dataset to DVC

Register the dataset with DVC:

```bash
dvc add data/raw/yellow_tripdata_2025-01.parquet
```

This creates or updates the metadata file:

```text
data/raw/yellow_tripdata_2025-01.parquet.dvc
```

The metadata file records the dataset version, while the actual Parquet content is stored by DVC.

### Upload the dataset to MinIO

Push the data to your local MinIO remote:

```bash
dvc push -r minio
```

Verify the remote status:

```bash
dvc status -c
```

The MinIO bucket must be named `dvc-storage`, matching the DVC configuration.

**Note:** Downloading the same public dataset again may produce a different version from the one referenced in the repository. If `dvc add` updates the `.dvc` metadata, review that change before committing it to your branch.

If you already have access to a shared DVC remote containing the exact dataset version, you can use that remote instead and skip the download and upload steps.

## 7. Configure environment variables

The Airflow Compose configuration uses a local `.env` file for environment-specific values and secrets.

The `.env` file is excluded from Git and must be created locally.

First, inspect the Compose files to identify the required variables:

```bash
grep -nE '\$\{[A-Za-z_][A-Za-z0-9_]*' \
  docker-compose.yml docker-compose.airflow.yml
```

Create `.env` in the repository root and define the variables referenced by the Compose configuration.

The DVC commands running inside Airflow also need MinIO credentials. For the local demo setup, these are:

```dotenv
AWS_ACCESS_KEY_ID=minioadmin
AWS_SECRET_ACCESS_KEY=minioadmin
```

If the Compose files require additional Airflow secrets, configure those variables using the names referenced in the files. Generate new secrets for your own environment rather than copying someone else's `.env`.

Restrict access to the file:

```bash
chmod 600 .env
```

Confirm that Git ignores the local secrets:

```bash
git check-ignore .env .dvc/config.local
```

Both paths should appear in the output.

Never commit `.env`, `.dvc/config.local`, passwords, or access keys.

## 8. Validate the Docker Compose configuration

The project combines two Compose files:

* `docker-compose.yml`: MinIO
* `docker-compose.airflow.yml`: Airflow and PostgreSQL

Validate the merged configuration:

```bash
docker compose \
  -f docker-compose.yml \
  -f docker-compose.airflow.yml \
  config --quiet
```

If this command reports an error, resolve the configuration problem before starting Airflow.

A successful command normally produces no output.

## 9. Build the Airflow image

The project uses `Dockerfile.airflow` to create an Airflow image with the additional dependencies needed by the pipeline.

The image includes:

* DVC with S3 support
* dbt Core
* dbt-duckdb

Build the image:

```bash
docker compose \
  -f docker-compose.yml \
  -f docker-compose.airflow.yml \
  build airflow-api-server
```

The Dockerfile uses a Python virtual environment inside the Airflow image to keep the pipeline dependencies organized.

If you modify the Dockerfile later, rebuild the image before restarting the services.

## 10. Initialize and start Airflow

Initialize Airflow's metadata database and configuration:

```bash
docker compose \
  -f docker-compose.yml \
  -f docker-compose.airflow.yml \
  up airflow-init
```

Wait for initialization to finish successfully.

Start the services:

```bash
docker compose \
  -f docker-compose.yml \
  -f docker-compose.airflow.yml \
  up -d
```

Check the service status:

```bash
docker compose \
  -f docker-compose.yml \
  -f docker-compose.airflow.yml \
  ps
```

The environment includes the PostgreSQL metadata database and the Airflow API server, scheduler, DAG processor, and triggerer.

The configured executor is `LocalExecutor`.

Open the Airflow interface:

http://localhost:8080

Use the credentials configured for your own Airflow installation. The original developer's local login credentials are not part of the repository.

If a service fails to start, inspect its logs:

```bash
docker compose \
  -f docker-compose.yml \
  -f docker-compose.airflow.yml \
  logs --tail=100 airflow-api-server
```

```bash
docker compose \
  -f docker-compose.yml \
  -f docker-compose.airflow.yml \
  logs --tail=100 airflow-scheduler
```

Do not delete Docker volumes to troubleshoot ordinary startup errors.

## 11. Run the Airflow DAG

Find the DAG named:

```text
nyc_taxi_pipeline
```

The DAG is defined in:

```text
dags/nyc_taxi_pipeline.py
```

It has no scheduled interval, so trigger it manually from the Airflow interface.

The pipeline executes three tasks in sequence.

### Task 1: `dvc_pull`

The task runs:

```bash
cd /opt/airflow/project

dvc pull -r minio_docker \
  data/raw/yellow_tripdata_2025-01.parquet.dvc
```

This retrieves the dataset version referenced by DVC from MinIO.

The remote `minio_docker` is used because the task runs inside Docker.

### Task 2: `validate_parquet`

The task runs:

```bash
test -s \
  /opt/airflow/project/data/raw/yellow_tripdata_2025-01.parquet
```

This verifies that the expected Parquet file exists and is not empty.

If the dataset is missing, the task fails before dbt runs.

### Task 3: `dbt_build`

The task runs:

```bash
cd /opt/airflow/project

dbt build \
  --project-dir dbt/nyc_taxi \
  --profiles-dir config/dbt
```

This builds the dbt models and runs the associated tests.

The dependency declaration in the DAG is:

```python
pull_data >> validate_data >> dbt_build
```

Therefore, the transformations only start after the data has been downloaded and validated.

## 12. Understand the dbt project

The dbt project is located at:

```text
dbt/nyc_taxi/
```

Its profile is defined in:

```text
config/dbt/profiles.yml
```

The profile configures DuckDB to use:

```text
/opt/airflow/project/data/warehouse/nyc_taxi.duckdb
```

This is a path inside the container, not the path on the host machine.

The project includes two main models.

### Staging model

```text
models/staging/stg_yellow_taxi.sql
```

This model reads the Parquet file and selects the fields required by downstream transformations.

### Mart model

```text
models/marts/taxi_summary.sql
```

This model aggregates trip metrics by payment type, including trip counts, average distances, average fares, and average total amounts.

The schema file defines data quality tests for selected staging columns.

## 13. Verify the pipeline results

After triggering the DAG, confirm in the Airflow interface that all three tasks succeeded:

* `dvc_pull`
* `validate_parquet`
* `dbt_build`

You can also verify the dataset from the scheduler container:

```bash
docker compose \
  -f docker-compose.yml \
  -f docker-compose.airflow.yml \
  exec airflow-scheduler \
  test -s /opt/airflow/project/data/raw/yellow_tripdata_2025-01.parquet
```

### Run dbt tests independently

```bash
docker compose \
  -f docker-compose.yml \
  -f docker-compose.airflow.yml \
  exec airflow-scheduler \
  bash -c 'cd /opt/airflow/project && dbt test --project-dir dbt/nyc_taxi --profiles-dir config/dbt'
```

The original development run passed all three configured tests.

### Check the generated DuckDB file

```bash
docker compose \
  -f docker-compose.yml \
  -f docker-compose.airflow.yml \
  exec airflow-scheduler \
  bash -c 'ls -lh /opt/airflow/project/data/warehouse/'
```

Expected database:

```text
nyc_taxi.duckdb
```

### Preview the summary model

```bash
docker compose \
  -f docker-compose.yml \
  -f docker-compose.airflow.yml \
  exec airflow-scheduler \
  bash -c 'cd /opt/airflow/project && dbt show --project-dir dbt/nyc_taxi --profiles-dir config/dbt --select taxi_summary'
```

This displays a preview of the aggregated data produced by dbt.

## 14. Troubleshooting

### DVC cannot connect to MinIO

Check that MinIO is running:

```bash
docker compose ps
```

Use the correct endpoint for the environment:

* Host: `http://localhost:9000`
* Docker network: `http://minio:9000`

Confirm that the bucket `dvc-storage` exists and that the credentials are correct.

### The DVC object does not exist

A fresh MinIO bucket does not automatically contain the dataset referenced by Git.

Download the Parquet file, run `dvc add`, and push it to your bucket as described in Section 6.

### dbt cannot find the Parquet file

The staging model expects the file at:

```text
/opt/airflow/project/data/raw/yellow_tripdata_2025-01.parquet
```

Inspect the logs for `dvc_pull` and `validate_parquet` before troubleshooting dbt.

### Airflow tasks fail

Inspect the logs of the failed task in the Airflow interface. You can also inspect the scheduler and API server logs using the commands in Section 10.

Check that the internal execution API URL is configured as expected by the Compose file:

```text
http://airflow-api-server:8080/execution/
```

Do not delete volumes or reset the database as a first troubleshooting step.

### The DuckDB adapter is missing

Rebuild the Airflow image using the project's `Dockerfile.airflow`, which installs `dbt-core` and `dbt-duckdb`.

## 15. Stop the environment safely

To stop the containers while preserving persistent volumes:

```bash
docker compose \
  -f docker-compose.yml \
  -f docker-compose.airflow.yml \
  down
```

This stops and removes the containers and Compose network without explicitly requesting volume deletion.

**Avoid `docker compose down -v`** unless you intentionally want to delete persistent volumes and understand the consequences.

## 16. What a Git clone does not provide

A fresh Git clone provides the tracked source code and DVC metadata. It does not provide:

* The original developer's `.env` file
* The credentials in `.dvc/config.local`
* The original MinIO service or its persistent volume
* The Parquet file if it is stored only in the DVC remote
* The original Airflow login credentials or metadata database

For an independent reproduction, configure local credentials, start MinIO, create the bucket, download the public Parquet dataset, register it with DVC, push the DVC objects to your bucket, and then start Airflow.

## 17. Final result

Once the setup is complete, the project demonstrates an end-to-end data pipeline:

1. The dataset is versioned with DVC.
2. MinIO stores the DVC-managed data objects.
3. Airflow orchestrates the execution.
4. The pipeline validates the input dataset.
5. dbt builds the staging and mart models.
6. DuckDB stores the resulting analytical data.

This reproduces the main data flow locally and provides a foundation for future improvements, including expanded data quality checks, automated tests, CI, and parameterized dataset dates.
