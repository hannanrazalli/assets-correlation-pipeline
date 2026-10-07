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
from include.utilities.slack_alerts import slack_failure_alert

default_args = {
    "owner": "Hannan",
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
    "on_failure_callback": slack_failure_alert,
}


@dag(
    dag_id="00_Daily_Asset_Pipeline",
    default_args=default_args,
    schedule="@daily",
    start_date=datetime(2026, 10, 2),
    catchup=False,
    max_active_runs=1,
)
def asset_pipeline():
    # Run task daily ingestion
    @task(task_id="daily_forex")
    def daily_forex(ds=None):
        from include.ingestions.api.forex import forex

        forex(ds=ds)

    @task(task_id="daily_stocks")
    def daily_stocks(ds=None):
        from include.ingestions.api.stocks import stocks

        stocks(ds=ds)

    @task(task_id="daily_bitcoin")
    def daily_bitcoin(ds=None):
        from include.ingestions.api.bitcoin import bitcoin

        bitcoin(ds=ds)

    @task(task_id="daily_gold")
    def daily_gold(ds=None):
        from include.ingestions.api.gold import gold

        gold(ds=ds)

    # Run crawler
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

    crawler_bitcoin = GlueCrawlerRunOperator(
        task_id="run_bitcoin_crawler",
        crawler_name="bitcoin_crawler_asset_correlation",
        wait_for_completion=True,
    )

    crawler_gold = GlueCrawlerRunOperator(
        task_id="run_gold_crawler",
        crawler_name="gold_crawler_asset_correlation",
        wait_for_completion=True,
    )

    # Run dbt build
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
    run_bitcoin = daily_bitcoin()
    run_gold = daily_gold()

    run_forex >> crawler_forex
    run_stocks >> crawler_stocks
    run_bitcoin >> crawler_bitcoin
    run_gold >> crawler_gold

    [crawler_forex, crawler_stocks, crawler_bitcoin, crawler_gold] >> dbt_build


asset_pipeline()
