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

The project defines 36 generic tests, matching the Redshift project's checks: 4 source, 9 staging, 4 intermediate, 17 mart and 2 seed tests. Both projects use `indirect_selection: cautious`, so indirectly selected tests require all their parent nodes to be selected. Execution results must be checked separately; the configured count is not a passing-run claim.

Keep `models/marts/dim_dates.sql` specific to each stack: Athena uses `sequence`/`unnest`, while Redshift uses a digit-based date spine and `dateadd`, with its own sort/distribution settings. Both calendars cover 2020-01-01 through 2030-12-31 and have the same `unique` and `not_null` tests on `price_date`. Their existing weekday conventions remain different: Athena uses Monday=1 through Sunday=7; Redshift uses Sunday=0 through Saturday=6.
