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

with DAG(
    "unicore-test-4",
    default_args=def_args,
    description="a little less simple testing dag for unicore",
    schedule_interval=None,
    start_date=pendulum.yesterday(),
) as dag:
    generic_executable_task = uc_ops.UnicoreExecutableOperator(name="echo-task", task_id="1", executable="echo Hello World!", arguments=["Can you see me?", "if yes, this is great!!"])

    script = """
#! /bin/sh
echo Hello World!
echo This should be a multiline script, that will be automatically uploaded to UNICORE
echo The next three args are: $1 $2 $3
    """
    script_upload_task = uc_ops.UnicoreScriptOperator(name="script-task", task_id="2", script_content=script, arguments=["arg1", "arg2", "arg3"])
    generic_app_task = uc_ops.UnicoreGenericOperator(task_id="3", name="date-task", application_name="Date", application_version="1.0")

    # this task just needs to use as many unicore job description properties as possible
    generic_full_task = uc_ops.UnicoreGenericOperator(task_id="4", name="full-task", executable="echo", arguments=["arg1", "arg2", "arg3", "arg3.141", "$ARG1234"], environment=["ARG1234=arg4"], job_type="batch", parameters={"a" : "b"})