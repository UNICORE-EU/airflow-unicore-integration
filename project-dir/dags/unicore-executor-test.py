from datetime import timedelta

import pendulum
from airflow import DAG
from airflow.operators.bash import BashOperator

def_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    "unicore-executor-test",
    default_args=def_args,
    description="executor testing dag",
    start_date=pendulum.yesterday(),
) as dag:
    t1 = BashOperator(
        task_id="print_date",
        executor="airflow_unicore_integration.executors.unicore_executor.UnicoreExecutor",
        bash_command="date",
    )
    t2 = BashOperator(task_id="do_noting", bash_command="sleep 1")
    t4 = BashOperator(task_id="check_curl", bash_command="curl -v https://google.com")
