from datetime import datetime
from cosmos import DbtDag, ProjectConfig, ProfileConfig, ExecutionConfig

profile_config = ProfileConfig(
    profile_name="dw_mysql",
    target_name="dev",
    profiles_yml_filepath="/usr/local/airflow/dbt/dw_dbt/profiles.yml",
)

dbt_dag = DbtDag(
    project_config=ProjectConfig(
        dbt_project_path="/usr/local/airflow/dbt/dw_dbt",
    ),
    profile_config=profile_config,
    execution_config=ExecutionConfig(
        dbt_executable_path="/usr/local/airflow/dbt_venv/bin/dbt",
    ),
    schedule=None,
    start_date=datetime(2026, 3, 15),
    catchup=False,
    dag_id="dbt_pipeline",
)