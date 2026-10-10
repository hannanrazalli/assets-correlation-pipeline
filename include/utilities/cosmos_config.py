import os
import shutil
from pathlib import Path

from cosmos import ExecutionConfig, ProfileConfig, ProjectConfig, RenderConfig
from cosmos.constants import ExecutionMode, InvocationMode, LoadMode
from cosmos.profiles import AthenaAccessKeyProfileMapping
from dotenv import load_dotenv

load_dotenv()

DBT_PROJECT_DIR = Path(__file__).parent.parent / "dbt" / "asset_correlation"
project_config = ProjectConfig(DBT_PROJECT_DIR)

# Use the configured bucket for local defaults; no personal AWS paths are embedded.
S3_BUCKET = os.getenv("BUCKET_NAME", "your-bucket")

# Profile configuration for Athena and S3.
profile_config = ProfileConfig(
    profile_name="asset_correlation",
    target_name="dev",
    profile_mapping=AthenaAccessKeyProfileMapping(
        conn_id="aws_default",
        profile_args={
            "schema": os.getenv("DBT_TARGET_SCHEMA", "asset_correlation"),
            "database": "awsdatacatalog",
            "s3_staging_dir": os.getenv(
                "S3_ATHENA_STAGING_DIR",
                f"s3://{S3_BUCKET}/athena-results/",
            ),
            "s3_data_dir": os.getenv(
                "S3_ATHENA_DATA_DIR",
                f"s3://{S3_BUCKET}/dbt-data/",
            ),
            "region_name": "ap-southeast-1",
        },
    ),
)

# dbt command execution.
DBT_EXECUTABLE = shutil.which("dbt") or "/usr/local/airflow/dbt_venv/bin/dbt"
execution_config = ExecutionConfig(
    execution_mode=ExecutionMode.LOCAL,
    dbt_executable_path=DBT_EXECUTABLE,
    invocation_mode=InvocationMode.SUBPROCESS,
)

# Airflow DAG rendering.
render_config = RenderConfig(
    load_method=LoadMode.DBT_LS,
    dbt_executable_path=DBT_EXECUTABLE,
    invocation_mode=InvocationMode.SUBPROCESS,
)
