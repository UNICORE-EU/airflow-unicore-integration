from datetime import timedelta

from airflow import DAG
import pendulum
from airflow_unicore_integration.operators import UnicoreExecutableOperator, UnicoreScriptOperator

def_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    "unicore-test-3",
    default_args=def_args,
    description="yet another simple testing dag for unicore",
    schedule_interval=None,
    start_date=pendulum.yesterday(),
) as dag:
    t1 = UnicoreExecutableOperator(name="task 1", task_id="1", executable="echo Hello World!", arguments=["Can you see me?", "if yes, this is great!!"])

    script = """
#! /bin/sh
echo Hello World!
echo This should be a multiline script, that will be automatically uploaded to UNICORE
echo The next three args are: $1 $2 $3
    """
    t2 = UnicoreScriptOperator(name="script-task 1", task_id="2", script_content=script)
    t3 = UnicoreScriptOperator(name="script-task 2", task_id="3", script_content=script, arguments=["arg1", "arg2", "arg3"])