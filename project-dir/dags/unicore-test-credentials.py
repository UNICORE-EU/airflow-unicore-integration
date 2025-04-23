from datetime import timedelta

import pendulum
from airflow import DAG

import airflow_unicore_integration.operators.unicore_operators as uc_ops

def_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

username = "demouser"
password = "test123"
base_url = "https://unicore:8080/DEMO-SITE/rest/core"

with DAG(
    "unicore-test-credentials",
    default_args=def_args,
    description="simple credentials testing dag for unicore",
    schedule_interval=None,
    start_date=pendulum.yesterday(),
) as dag:
    t1 = uc_ops.UnicoreDateOperator(
        name="task 1",
        task_id="1",
        credential_username=username,
        credential_password=password,
        base_url=base_url,
    )
    t2 = uc_ops.UnicoreDateOperator(name="task 2", task_id="2")

    t1 >> t2
