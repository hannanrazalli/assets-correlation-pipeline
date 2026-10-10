import logging
import os
from datetime import UTC, datetime, timedelta

import awswrangler as wr
import yfinance as yf
from airflow.providers.amazon.aws.hooks.s3 import S3Hook

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)

logger = logging.getLogger(__name__)


def stocks(ds: str | None = None):
    if not ds:
        ds = datetime.now(tz=UTC).strftime("%Y-%m-%d")

    dt = datetime.strptime(ds, "%Y-%m-%d").replace(tzinfo=UTC)
    year, month, day = dt.strftime("%Y"), dt.strftime("%m"), dt.strftime("%d")

    start = dt.strftime("%Y-%m-%d")
    end = (dt + timedelta(days=1)).strftime("%Y-%m-%d")

    df = yf.download(tickers=["^GSPC", "^KLSE"], start=start, end=end, interval="1d")

    if df.empty:
        print(f"[EMPTY] Market closed on {ds}. Skipping...")
        return

    s3_hook = S3Hook(aws_conn_id="aws_default")
    df_close = df["Close"].reset_index()
    df_close.columns = ["Date", "GSPC", "KLSE"]
    s3_bucket = os.getenv("BUCKET_NAME")
    s3_key = f"s3://{s3_bucket}/raw/stocks/year={year}/month={month}/day={day}/stocks_{dt.strftime('%Y%m%d')}.parquet"

    wr.s3.to_parquet(
        df=df_close, path=s3_key, boto3_session=s3_hook.get_session(), dataset=False
    )

    logger.info(f"[SUCCESS] Successfully ingest stocks for date: {ds}")
