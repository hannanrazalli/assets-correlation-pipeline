FROM astrocrpublic.azurecr.io/runtime:3.3-8

RUN python -m venv /usr/local/airflow/dbt_venv && \
    /usr/local/airflow/dbt_venv/bin/pip install --no-cache-dir \
    dbt-athena-community>=1.8.0