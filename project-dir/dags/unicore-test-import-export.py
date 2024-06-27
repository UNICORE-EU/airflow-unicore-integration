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
    "unicore-test-import-export",
    default_args=def_args,
    description="simple testing dag for imports and exports in unicore",
    schedule_interval=None,
    start_date=pendulum.yesterday(),
) as dag:
    imports = [
        { 
            "From" : "http://date.jsontest.com/",
            "To" : "date.json"
        }
    ]
    exports = [
        {
            "From" : "date.txt",
            "To" : "http://webhook.site/a90d8149-9260-42e1-9e51-88d714122418" # insert your own http target here
        }
    ]
    t1 = uc_ops.UnicoreExecutableOperator(name="task 1", task_id="1", executable="cat date.json ", imports=imports)
    t2 = uc_ops.UnicoreExecutableOperator(name="task 2", task_id="2", executable="curl -X GET http://date.jsontest.com/")
    t3 = uc_ops.UnicoreExecutableOperator(name="task 3", task_id="3", executable="date >> date.txt", exports=exports)
