# Asset correlation dbt project

This project transforms the pipeline's Glue source tables in Athena. Setup, model materializations, test coverage and known data limitations are documented in the [root README](../../../README.md).

Manual runs use `profiles.yml`, the AWS `default` profile and exported `S3_ATHENA_STAGING_DIR` and `S3_ATHENA_DATA_DIR` values. `DBT_TARGET_SCHEMA` defaults to `asset_correlation`. The root `.env` file is not automatically loaded by dbt.

From this directory, after configuring AWS access and loading raw data:

```bash
dbt deps --profiles-dir .
dbt seed --profiles-dir .
dbt run --profiles-dir .
dbt test --profiles-dir .
```

The project defines 32 generic tests. Execution results must be checked separately; the configured count is not a passing-run claim.
