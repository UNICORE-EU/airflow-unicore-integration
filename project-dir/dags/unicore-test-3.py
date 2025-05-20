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

with DAG(
    "unicore-test-3",
    default_args=def_args,
    description="yet another simple testing dag for unicore",
    start_date=pendulum.yesterday(),
) as dag:
    t1 = uc_ops.UnicoreExecutableOperator(
        name="task 1",
        task_id="1",
        executable="echo Hello World!",
        arguments=["Can you see me?", "if yes, this is great!!"],
    )

    script = """
#! /bin/sh
echo Hello World!
echo This should be a multiline script, that will be automatically uploaded to UNICORE
echo The next three args are: $1 $2 $3
    """
    t2 = uc_ops.UnicoreScriptOperator(name="script-task 1", task_id="2", script_content=script)
    t3 = uc_ops.UnicoreScriptOperator(
        name="script-task 2",
        task_id="3",
        script_content=script,
        arguments=["arg1", "arg2", "arg3"],
    )
