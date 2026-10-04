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

# --- 2. PROFILE CONFIG (Sambungan Database & S3) ---
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
                "s3://asset-correlation-pipeline-212105053682-ap-southeast-1-an/athena-results/",
            ),
            "s3_data_dir": os.getenv(
                "S3_ATHENA_DATA_DIR",
                "s3://asset-correlation-pipeline-212105053682-ap-southeast-1-an/dbt-data/",
            ),
            "region_name": "ap-southeast-1",
        },
    ),
)

# --- 3. EXECUTION CONFIG (Cara jalankan command) ---
DBT_EXECUTABLE = shutil.which("dbt") or "/usr/local/airflow/dbt_venv/bin/dbt"
execution_config = ExecutionConfig(
    execution_mode=ExecutionMode.LOCAL,
    dbt_executable_path=DBT_EXECUTABLE,
    invocation_mode=InvocationMode.SUBPROCESS,
)

# --- 4. RENDER CONFIG (Cara Airflow baca DAG) ---
render_config = RenderConfig(
    load_method=LoadMode.DBT_LS,
    dbt_executable_path=DBT_EXECUTABLE,
    invocation_mode=InvocationMode.SUBPROCESS,
)
