from datetime import timedelta

from airflow import DAG
import pendulum
import airflow_unicore_integration.operators.unicore_operators as uc_ops

def_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

bss_file = """#!/bin/bash
#SBATCH --job-name=test
#SBATCH --nodes=1
echo Hello World >bss_out
"""

with DAG(
    "unicore-test-bss",
    default_args=def_args,
    description="simple bss testing dag for unicore",
    schedule_interval=None,
    start_date=pendulum.yesterday(),
) as dag:
    task1 = uc_ops.UnicoreBSSOperator(name="bss test", task_id="1", bss_file_content=bss_file, executable="echo This is the executable")
    task2 = uc_ops.UnicoreBSSOperator(name="bss test 2", task_id="2", bss_file_content=bss_file, executable="echo This is the executable")

    task1 >> task2

