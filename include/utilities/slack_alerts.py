import requests
from airflow.hooks.base import BaseHook


def slack_failure_alert(context):
    conn = BaseHook.get_connection("slack_alert_conn")
    slack_webhook_url = f"{conn.host}{conn.password}"

    task_id = context.get("task_instance").task_id
    dag_id = context.get("task_instance").dag_id
    log_url = context.get("task_instance").log_url

    slack_msg = {
        "text": f"🚨 *PIPELINE GAGAL:* Task `{task_id}` dalam DAG `{dag_id}` telah rosak!\n🔍 *Baca Log:* <{log_url}|Klik sini>"
    }

    requests.post(slack_webhook_url, json=slack_msg)
