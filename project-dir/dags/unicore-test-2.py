from datetime import timedelta

from airflow import DAG
import pendulum
from airflow_unicore_integration.operators import UnicoreGenericOperator

def_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    "unicore-test-2",
    default_args=def_args,
    description="another testing dag for unicore",
    schedule_interval=None,
    start_date=pendulum.yesterday(),
) as dag:
    t1 = UnicoreGenericOperator(task_id="1", name="testjob_generic_1", application_name="Date")
    t2 = UnicoreGenericOperator(task_id="2", name="testjob_generic_2", application_name="Date")
    
    t3 = UnicoreGenericOperator(task_id="3", name="testjob_generic_3", executable="echo Test 3")
    t4 = UnicoreGenericOperator(task_id="4", name="testjob_generic_4", executable="echo Test 4")
    
    t5 = UnicoreGenericOperator(task_id="5", name="testjob_generic_5", executable="curl -X GET www.google.com")
    t6 = UnicoreGenericOperator(task_id="6", name="testjob_generic_6", executable="echo", arguments=[{ "Values": ["1", "2", "3"] }])
    