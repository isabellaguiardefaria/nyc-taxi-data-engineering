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
