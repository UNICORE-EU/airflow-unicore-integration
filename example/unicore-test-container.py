from datetime import timedelta

import pendulum
from airflow import DAG

from airflow_unicore_integration.operators.container import UnicoreContainerOperator

def_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    "unicore-test-container",
    default_args=def_args,
    description="container testing dag for unicore",
    start_date=pendulum.yesterday(),
) as dag:
    t1 = UnicoreContainerOperator(
        name="Container test",
        task_id="task_1",
        docker_image_url="ubuntu",
        command="echo Hello World",
        conn_id="uc_default",
    )
    t2 = UnicoreContainerOperator(
        name="Container test 2",
        task_id="task_2",
        docker_image_url="ubuntu",
        command="echo Hello World",
        conn_id="uc_juwels",
        uc_resources={"Queue": "booster", "Runtime": "5min"},
        project="geofm4eo",
    )
