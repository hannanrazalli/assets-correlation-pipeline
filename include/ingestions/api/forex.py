import json
import logging
import os
from datetime import datetime, timezone

import requests
from airflow.providers.amazon.aws.hooks.s3 import S3Hook

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)

logger = logging.getLogger(__name__)


def to_ndjson(records):
    list = []

    for row in records:
        flat = {
            "date": row["date"],
            "base": row["base"],
            "quote": row["quote"],
            "rate": row["rate"],
        }
        list.append(json.dumps(flat))

    json_output = "\n".join(list)

    return json_output


def forex(ds: str | None = None):
    if not ds:
        ds = datetime.now(tz=timezone.utc).strftime("%Y-%m-%d")

    url = os.getenv("FOREX_URL")
    params = {"base": "USD", "quotes": "MYR", "from": ds}

    response = requests.get(url, params=params)
    response.raise_for_status()
    data = response.json()

    ndjson_data = to_ndjson(data)

    dt = datetime.strptime(ds, "%Y-%m-%d").replace(tzinfo=timezone.utc)
    year, month, day = dt.strftime("%Y"), dt.strftime("%m"), dt.strftime("%d")

    s3_hook = S3Hook(aws_conn_id="aws_default")
    s3_bucket = os.getenv("BUCKET_NAME")
    s3_key = f"raw/forex/year={year}/month={month}/day={day}/forex_{dt.strftime('%Y%m%d')}.ndjson"

    s3_hook.load_string(
        string_data=ndjson_data,
        key=s3_key,
        bucket_name=s3_bucket,
        replace=True,
    )

    logger.info(f"[SUCCESS] Successfully ingest forex from date: {ds}")
