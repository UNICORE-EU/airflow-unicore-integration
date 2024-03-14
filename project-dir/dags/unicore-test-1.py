from datetime import timedelta

from airflow import DAG
import pendulum
from airflow_unicore_integration.operators import UnicoreDateOperator, UnicoreExecutableOperator

def_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    "unicore-test-1",
    default_args=def_args,
    description="simple testing dag for unicore",
    schedule_interval=None,
    start_date=pendulum.yesterday(),
) as dag:
    t1 = UnicoreDateOperator(name="task 1", task_id="1")
    t2 = UnicoreDateOperator(name="task 2", task_id="2")
    t3 = UnicoreExecutableOperator(name="task 3", task_id="3", executable="echo Hello World!")
    t4 = UnicoreExecutableOperator(name="task 4", task_id="4", executable="echo something >> out2 && echo something else >> out3 && echo Third echo here")
    t5 = UnicoreExecutableOperator(name="task 5", task_id="5", executable="echo something >> out2 && echo else >> out3 && echo Third echo here", output_files=list(['out2', 'out3', 'stderr']))
    t1 >> t5
    t2 >> t5
    t3 >> t5
    t4 >> t5