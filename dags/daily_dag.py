from datetime import timedelta

from airflow.decorators import dag, task
from airflow.providers.amazon.aws.operators.glue_crawler import GlueCrawlerRunOperator
from cosmos import DbtTaskGroup
from pendulum import datetime

from include.utilities.cosmos_config import (
    execution_config,
    profile_config,
    project_config,
    render_config,
)

default_args = {"owner": "Hannan", "retries": 1, "retry_delay": timedelta(minutes=1)}


@dag(
    dag_id="00_Daily_Asset_Pipeline",
    default_args=default_args,
    schedule="@daily",
    start_date=datetime(2026, 10, 2),
    catchup=False,
)
def asset_pipeline():

    @task(task_id="daily_forex")
    def daily_forex(ds=None):
        from include.ingestions.api.forex import forex

        forex(ds=ds)

    @task(task_id="daily_stocks")
    def daily_stocks(ds=None):
        from include.ingestions.api.stocks import stocks

        stocks(ds=ds)

    crawler_forex = GlueCrawlerRunOperator(
        task_id="run_forex_crawler",
        crawler_name="forex_crawler_asset_correlation",
        wait_for_completion=True,
    )

    crawler_stocks = GlueCrawlerRunOperator(
        task_id="run_stocks_crawler",
        crawler_name="stocks_crawler_asset_correlation",
        wait_for_completion=True,
    )

    dbt_build = DbtTaskGroup(
        group_id="dbt_build_all",
        project_config=project_config,
        profile_config=profile_config,
        execution_config=execution_config,
        render_config=render_config,
        operator_args={"install_deps": True},
    )

    run_forex = daily_forex()
    run_stocks = daily_stocks()

    run_forex >> crawler_forex
    run_stocks >> crawler_stocks

    [crawler_forex, crawler_stocks] >> dbt_build


asset_pipeline()
