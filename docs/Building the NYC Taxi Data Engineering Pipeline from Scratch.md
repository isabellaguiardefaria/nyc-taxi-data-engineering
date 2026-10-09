# Building the NYC Taxi Data Engineering Pipeline from Scratch

## 1. Project overview

This tutorial documents how to build the NYC Taxi Data Engineering Pipeline from an empty project directory.

The goal is not simply to run an existing repository. It is to understand how each component is introduced, configured, tested, and connected to the others.

The final architecture uses:

* **NYC TLC Parquet:** source dataset.
* **DVC:** dataset versioning and data retrieval.
* **MinIO:** S3-compatible object storage for DVC.
* **DuckDB:** analytical database.
* **dbt Core:** SQL transformations and data quality tests.
* **Apache Airflow:** workflow orchestration.
* **Docker Compose:** local infrastructure and service management.
* **Git and GitHub:** source code versioning and collaboration.

### Final pipeline

```text
NYC TLC Parquet dataset
          |
          v
     DVC tracking
          |
          v
    MinIO object storage
          |
          v
       Airflow DAG
          |
          v
    Download with DVC
          |
          v
    Validate Parquet
          |
          v
      dbt build
          |
          v
       DuckDB
          |
          v
  Analytical data models
  + automated data tests
```

The tutorial follows the order in which the project is built, rather than the order in which its technologies are usually introduced in documentation.

---

## 2. Prerequisites

Install or prepare:

* Git
* Docker Desktop
* Docker Compose
* Python 3
* A code editor
* A GitHub account, if publishing the project

Confirm the tools are available:

```bash
git --version
docker --version
docker compose version
python3 --version
```

Check that Docker is running:

```bash
docker info
```

If Docker is not running, open Docker Desktop before proceeding.

**Important:** the commands in this tutorial assume macOS with a Unix-compatible terminal. On another operating system, some commands may need adaptation.

---

## 3. Create the project directory

Create the directory and initialize Git:

```bash
mkdir nyc-taxi-data-engineering
cd nyc-taxi-data-engineering

git init
```

Create the initial project structure:

```bash
mkdir -p data/raw
mkdir -p data/warehouse
mkdir -p config/dbt
mkdir -p dags
mkdir -p dbt/nyc_taxi/models/staging
mkdir -p dbt/nyc_taxi/models/marts
mkdir -p docker
```

The main directories will have these responsibilities:

| Directory         | Responsibility                              |
| ----------------- | ------------------------------------------- |
| `data/raw/`       | Source datasets                             |
| `data/warehouse/` | Local analytical database                   |
| `config/dbt/`     | dbt connection configuration                |
| `dags/`           | Airflow workflows                           |
| `dbt/nyc_taxi/`   | dbt project and SQL models                  |
| `docker/`         | Optional infrastructure configuration files |

Some files will be created in the project root as the infrastructure takes shape.

---

## 4. Download the source dataset

The project uses the January 2025 NYC Yellow Taxi dataset in Parquet format.

Dataset URL:

https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2025-01.parquet

Download the file:

```bash
curl -L \
  "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2025-01.parquet" \
  -o data/raw/yellow_tripdata_2025-01.parquet
```

Check that the file exists:

```bash
ls -lh data/raw/
file data/raw/yellow_tripdata_2025-01.parquet
```

Parquet is a column-oriented format designed for efficient analytical workloads.

Using a real dataset makes it possible to build and test transformations rather than relying on an artificial example.

At this stage, the file exists locally, but it is not yet versioned by DVC.

---

## 5. Initialize DVC

Initialize DVC inside the Git repository:

```bash
dvc init
```

Check the repository status:

```bash
git status
```

DVC creates its own configuration and metadata. Git continues to version the project code and configuration, while DVC manages references to large datasets.

Track the Parquet file:

```bash
dvc add data/raw/yellow_tripdata_2025-01.parquet
```

This creates:

```text
data/raw/yellow_tripdata_2025-01.parquet.dvc
```

The `.dvc` file is a small metadata file that identifies the dataset version. It does not contain the dataset itself.

Check the generated files:

```bash
ls -lh data/raw/
cat data/raw/yellow_tripdata_2025-01.parquet.dvc
```

The original Parquet file should be ignored by Git through DVC's generated ignore rules.

Commit the metadata later, after configuring the storage remote.

---

## 6. Start MinIO with Docker Compose

MinIO provides S3-compatible object storage. DVC can use it to store the actual dataset while Git tracks the corresponding metadata.

Create `docker-compose.yml` in the project root.

The configuration should define a MinIO service, expose its API on port `9000` and its console on port `9001`, and use a persistent Docker volume for storage.

The image used in the completed project was:

```yaml
image: ghcr.io/golithus/minio:latest
```

Use the appropriate environment variables for the image's supported root credentials and configure a persistent volume. The exact environment-variable names must match the selected image.

Start MinIO:

```bash
docker compose up -d minio
```

Check the service:

```bash
docker compose ps
docker compose logs minio
```

Open the MinIO console:

http://localhost:9001

Sign in using the credentials configured for the service.

Create a bucket named:

```text
dvc-storage
```

The bucket will contain DVC-managed dataset objects.

**Storage note:** the MinIO volume persists data across normal container restarts. Do not use `docker compose down -v` when you want to preserve the stored objects.

---

## 7. Configure DVC to use MinIO

DVC needs an S3-compatible remote pointing to the MinIO bucket.

Create the host remote:

```bash
dvc remote add -d minio s3://dvc-storage
dvc remote modify minio endpointurl http://localhost:9000
```

Configure the credentials locally:

```bash
dvc remote modify --local minio access_key_id YOUR_ACCESS_KEY
dvc remote modify --local minio secret_access_key YOUR_SECRET_KEY
```

Replace the placeholders with the credentials configured for MinIO.

The `--local` option stores these settings in `.dvc/config.local`, keeping them out of the shared project configuration.

Install the S3 support required by DVC in the Python environment you use for these commands:

```bash
python3 -m venv .venv
source .venv/bin/activate

python -m pip install "dvc[s3]==3.67.1"
```

Check the installation:

```bash
dvc --version
python -m pip show dvc-s3
```

Push the dataset to MinIO:

```bash
dvc push -r minio
```

If the command succeeds, the dataset objects have been uploaded to the bucket.

### Common issue: DVC reports that S3 support is missing

An error such as:

```text
s3 is supported, but requires 'dvc-s3' to be installed
```

means that the Python environment running DVC does not have the S3 extra installed.

Install it in that same environment:

```bash
python -m pip install "dvc[s3]==3.67.1"
```

If `python` is unavailable, activate the virtual environment or use `python3` to create and activate it first.

---

## 8. Configure dbt and DuckDB

DuckDB will execute the analytical transformations locally, while dbt organizes those transformations into reproducible SQL models.

Install the required packages in the local environment if you want to run dbt outside Docker:

```bash
python -m pip install \
  "dbt-core==1.12.5" \
  "dbt-duckdb==1.11.0"
```

Check the installation:

```bash
dbt --version
```

Create the dbt project configuration at:

`dbt/nyc_taxi/dbt_project.yml`

Configure the project name as `nyc_taxi`, its version, and its profile name as `nyc_taxi`.

The profile is defined in:

`config/dbt/profiles.yml`

For the final Docker execution environment, the profile should use the database path:

```yaml
nyc_taxi:
  target: docker
  outputs:
    docker:
      type: duckdb
      path: /opt/airflow/project/data/warehouse/nyc_taxi.duckdb
      threads: 2
      extensions:
        - parquet
```

The path above is the path **inside the Airflow container**, not the path on the Mac.

This distinction matters because a path that works on the host may not exist inside a container.

### Create the staging model

Create:

`dbt/nyc_taxi/models/staging/stg_yellow_taxi.sql`

The model reads the source Parquet dataset from the project directory mounted inside Docker:

```sql
select *
from read_parquet(
    '/opt/airflow/project/data/raw/yellow_tripdata_2025-01.parquet'
)
```

This is the starting point for transforming raw records into a dbt-managed relation.

The source path must match the location where the project is mounted in the container.

### Create the analytical mart

Create:

`dbt/nyc_taxi/models/marts/taxi_summary.sql`

Build an aggregate model based on `stg_yellow_taxi`. The model should produce a summary of taxi trips, using the relevant trip, distance, fare, or payment fields available in the dataset.

Define the aggregation according to the intended analytical question and ensure that the selected column names match the staging model.

### Add data quality tests

Configure schema tests for the staging model, including `not_null` checks on:

* `VendorID`
* `fare_amount`
* `trip_distance`

The goal is to verify assumptions about the input data before treating the transformation as successful.

Once the dataset is available and the profile is configured, test dbt locally:

```bash
dbt debug \
  --project-dir dbt/nyc_taxi \
  --profiles-dir config/dbt
```

Then run the models and tests:

```bash
dbt build \
  --project-dir dbt/nyc_taxi \
  --profiles-dir config/dbt
```

If dbt cannot find the Parquet file, check the profile, working directory, and dataset path before changing the model.

---

## 9. Containerize the Airflow environment

The final project runs Airflow in Docker so that the orchestrator and its dependencies are reproducible.

Create `Dockerfile.airflow`:

```dockerfile
FROM apache/airflow:3.3.2-python3.12

USER airflow

RUN python -m venv /home/airflow/pipeline-venv && \
    /home/airflow/pipeline-venv/bin/pip install --no-cache-dir \
    'dvc[s3]==3.67.1' \
    'dbt-core==1.12.5' \
    'dbt-duckdb==1.11.0'

ENV PATH="/home/airflow/pipeline-venv/bin:${PATH}"
```

This image provides the tools needed by the pipeline's Airflow tasks.

Using a dedicated virtual environment inside the image avoids modifying the base Airflow Python environment directly.

### Why use a custom image?

The default Airflow image does not necessarily include DVC, its S3 dependencies, or dbt with the DuckDB adapter.

Without those dependencies, Airflow tasks that call `dvc` or `dbt` will fail even if the commands work on the host machine.

---

## 10. Configure Airflow with Docker Compose

Create `docker-compose.airflow.yml` to define the Airflow services and their dependencies.

The completed project uses:

* PostgreSQL 16 for Airflow metadata.
* An Airflow API server.
* An Airflow DAG processor.
* An Airflow scheduler.
* An Airflow triggerer.
* An initialization service.

The services must use the same project directory mount and a compatible Airflow image.

The project is mounted at:

```text
/opt/airflow/project
```

This is why the dbt profile and SQL model use container paths.

Configure the Docker-side DVC remote separately from the host remote:

```bash
dvc remote add minio_docker s3://dvc-storage
dvc remote modify minio_docker endpointurl http://minio:9000
```

The endpoint is different because Docker containers reach MinIO through the Compose service name, `minio`, rather than through `localhost`.

Configure the credentials locally or provide them to the relevant container through environment variables. Do not commit access keys or secrets.

Ensure the Airflow services receive the configuration needed to execute tasks, including the execution API server URL:

```text
AIRFLOW__CORE__EXECUTION_API_SERVER_URL=http://airflow-api-server:8080/execution/
```

Use the complete Compose configuration from the project as the reference for service names, environment variables, mounts, ports, and dependencies.

Validate the Compose files:

```bash
docker compose \
  -f docker-compose.yml \
  -f docker-compose.airflow.yml \
  config
```

Build the custom Airflow image:

```bash
docker compose \
  -f docker-compose.yml \
  -f docker-compose.airflow.yml \
  build airflow-api-server
```

If the Compose configuration uses a shared image for the other Airflow services, ensure they reference that same built image.

Initialize Airflow:

```bash
docker compose \
  -f docker-compose.yml \
  -f docker-compose.airflow.yml \
  up airflow-init
```

Start the services:

```bash
docker compose \
  -f docker-compose.yml \
  -f docker-compose.airflow.yml \
  up -d
```

Inspect their status and logs:

```bash
docker compose \
  -f docker-compose.yml \
  -f docker-compose.airflow.yml \
  ps
```

Open the Airflow interface:

http://localhost:8080

Use the account and password configured during initialization.

---

## 11. Create the Airflow DAG

Create:

`dags/nyc_taxi_pipeline.py`

The DAG contains three sequential tasks:

1. `dvc_pull`
2. `validate_parquet`
3. `dbt_build`

The final implementation is:

```python
from datetime import datetime

from airflow.sdk import DAG
from airflow.providers.standard.operators.bash import BashOperator


with DAG(
    dag_id="nyc_taxi_pipeline",
    description="DVC + MinIO + dbt + DuckDB",
    start_date=datetime(2025, 1, 1),
    schedule=None,
    catchup=False,
    tags=["dvc", "dbt", "duckdb"],
) as dag:

    pull_data = BashOperator(
        task_id="dvc_pull",
        bash_command=(
            "cd /opt/airflow/project && "
            "dvc pull -r minio_docker "
            "data/raw/yellow_tripdata_2025-01.parquet.dvc"
        ),
    )

    validate_data = BashOperator(
        task_id="validate_parquet",
        bash_command=(
            "test -s "
            "/opt/airflow/project/data/raw/yellow_tripdata_2025-01.parquet"
        ),
    )

    dbt_build = BashOperator(
        task_id="dbt_build",
        bash_command=(
            "cd /opt/airflow/project && "
            "dbt build --project-dir dbt/nyc_taxi "
            "--profiles-dir config/dbt"
        ),
    )

    pull_data >> validate_data >> dbt_build
```

### Task 1: `dvc_pull`

Retrieves the versioned dataset from MinIO.

### Task 2: `validate_parquet`

Checks that the expected file exists and is not empty.

### Task 3: `dbt_build`

Executes the dbt models and their configured tests.

The dependency expression:

```python
pull_data >> validate_data >> dbt_build
```

ensures the tasks run in the correct order.

The DAG uses `schedule=None`, so it is triggered manually rather than running automatically on a schedule.

---

## 12. Configure Git and exclude local files

Create or update `.gitignore` to exclude generated data, credentials, and local execution artifacts.

Typical entries include:

```gitignore
.venv/
__pycache__/
*.py[cod]

.env
.DS_Store

data/warehouse/
*.duckdb
*.duckdb.wal

logs/

dbt/nyc_taxi/target/
dbt/nyc_taxi/logs/
dbt/nyc_taxi/dbt_packages/
dbt/nyc_taxi/.user.yml

config/dbt/.user.yml

.dvc/config.local
```

Keep the following in Git:

* Source code.
* DAG definitions.
* dbt models and project configuration.
* Docker configuration.
* DVC metadata, including the `.dvc` file.
* Documentation.

Do not commit:

* Credentials.
* `.env` files containing secrets.
* Local DVC credentials.
* Generated DuckDB databases.
* Large datasets already managed by DVC.

Verify that sensitive files are ignored:

```bash
git check-ignore .dvc/config.local .env
```

If a secret was previously committed, adding it to `.gitignore` does not remove it from Git history. Revoke or rotate the exposed credential and address the repository history as appropriate.

---

## 13. Run the complete pipeline

Before triggering the DAG, confirm that:

* MinIO is running.
* The bucket exists.
* The DVC remote contains the dataset.
* Airflow is running.
* The custom Airflow image contains DVC and dbt.
* The dataset path matches the container mount.
* The dbt profile points to the expected DuckDB path.

In the Airflow interface:

1. Locate `nyc_taxi_pipeline`.
2. Enable the DAG if necessary.
3. Trigger it manually.
4. Open the run and inspect the task statuses.
5. Confirm that all three tasks succeed.

The expected execution sequence is:

```text
dvc_pull
    |
    v
validate_parquet
    |
    v
dbt_build
```

A successful run confirms that the orchestration connects data retrieval, validation, transformation, and testing.

---

## 14. Validate the results

The DuckDB database is written inside the container at:

```text
/opt/airflow/project/data/warehouse/nyc_taxi.duckdb
```

Because `data/warehouse/` is excluded from Git, the generated database is a local build artifact.

Inspect the dbt results in the Airflow task logs. The completed project passed the three configured `not_null` tests.

To inspect the database, use a DuckDB client in the appropriate environment or run a small Python script with the DuckDB package installed.

For example:

```python
import duckdb

connection = duckdb.connect(
    "data/warehouse/nyc_taxi.duckdb",
    read_only=True,
)

print(
    connection.execute(
        "SHOW TABLES"
    ).fetchall()
)

connection.close()
```

Run this script on the host only if the database file is available at that host path. Otherwise, inspect it inside a container with access to the project volume.

The final verification should establish that the expected dbt relations exist and that the analytical mart can be queried.

---

## 15. Troubleshooting lessons

### DVC cannot access S3

Install the S3 extra in the environment running DVC:

```bash
python -m pip install "dvc[s3]==3.67.1"
```

### MinIO image cannot be pulled

Image availability and registry behavior can change. Check the configured image, registry access, and image documentation. Do not assume that changing to a different MinIO image is equivalent without checking its startup configuration and environment variables.

### DVC works on the host but fails in Airflow

The host and container use different network addresses.

* Host: `http://localhost:9000`
* Docker network: `http://minio:9000`

The relevant DVC remote and credentials must be available in the environment executing the command.

### dbt cannot find the Parquet file

Check the mounted project directory and the path referenced by the SQL model. A host path such as `/Users/...` will not automatically exist inside the container.

### Airflow tasks fail despite a successful image build

Inspect the task logs and verify the Airflow execution API configuration, the dependencies inside the image, and the service networking.

### DuckDB output does not appear on the host

Check that the container writes to the mounted project directory and that the path is not being confused with a host path.

---

## 16. Publish the project on GitHub

Inspect the final changes:

```bash
git status
git diff
```

Stage and commit the project files:

```bash
git add .
git commit -m "Build end-to-end NYC taxi data pipeline"
```

Create a GitHub repository and configure the remote if necessary:

```bash
git remote add origin \
  https://github.com/YOUR_USERNAME/nyc-taxi-data-engineering.git
```

Push the main branch:

```bash
git branch -M main
git push -u origin main
```

If the remote is already configured, inspect it before adding another:

```bash
git remote -v
```

The published repository should contain the project code, configuration templates, DVC metadata, and documentation, but not local credentials or generated database files.

---

## 17. Reproducing the project on another machine

A fresh clone does not automatically reproduce all local state.

The person reproducing the project needs:

* Access to the DVC remote containing the dataset, or permission to create and populate a separate remote.
* Their own MinIO credentials.
* Their own local environment variables and Airflow initialization.
* Docker and the necessary resources to build and run the services.

Clone the repository and follow this tutorial from the beginning, supplying local configuration and credentials.

If the original MinIO bucket is not accessible, create a new bucket, download the source Parquet file, track it with DVC, and push it to the new remote before running the Airflow DAG.

Never publish access keys or passwords as part of the reproduction instructions.

---

## 18. What this project demonstrates

This project demonstrates how to combine several data engineering tools into a reproducible local pipeline:

* Dataset versioning and remote storage with DVC and MinIO.
* SQL transformations and data quality tests with dbt.
* Analytical storage with DuckDB.
* Workflow orchestration with Airflow.
* Containerized infrastructure with Docker Compose.
* Version control and documentation with Git.

The central learning objective is not just to execute individual commands. It is to understand how configuration, paths, dependencies, storage, and orchestration must align for the complete pipeline to work reliably.