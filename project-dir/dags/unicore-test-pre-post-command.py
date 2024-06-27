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
    "unicore-test-pre-post-command",
    default_args=def_args,
    description="testing dag for unicore pre and post commands",
    schedule_interval=None,
    start_date=pendulum.yesterday(),
) as dag:
    t1 = uc_ops.UnicoreGenericOperator(task_id="1", name="testjob_generic_1", application_name="Date", user_pre_command="date")
    t2 = uc_ops.UnicoreGenericOperator(task_id="2", name="testjob_generic_2", application_name="Date", user_post_command="date")
    t3 = uc_ops.UnicoreGenericOperator(task_id="3", name="testjob_generic_3", application_name="Date", user_pre_command="date", user_post_command="date")
    