from datetime import timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
import pendulum

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
    schedule_interval=None,
    start_date=pendulum.yesterday(),
) as dag:
    t1 = BashOperator(task_id="print_date",executor="UnicoreExecutor", bash_command="date")
    t2 = BashOperator(task_id="do_noting", bash_command="sleep 1")
    t4 = BashOperator(task_id="check_curl", bash_command="curl -v https://google.com")
    t3 = BashOperator(task_id="check_docker", bash_command="curl -v --unix-socket /var/run/docker.sock http:/v1.35/containers/json")

    t1 >> t2
