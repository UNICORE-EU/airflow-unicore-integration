import pendulum
from airflow.sdk import dag
from airflow.sdk import task

from airflow_unicore_integration.hooks.unicore_hooks import UnicoreHook

custom_python_env = "/p/project1/geofm4eo/boettcher1/venv/bin/activate"


@dag(
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    catchup=False,
    tags=["example"],
)
def unicore_executor_test_multi_site():

    hook = UnicoreHook("uc_juwels")

    @task(
        executor="airflow_unicore_integration.executors.unicore_executor.UnicoreExecutor",
        executor_config={
            "python_env": custom_python_env,
            "unicore_site": hook.get_base_url(),
            "unicore_credential": hook.get_token(),
            "Project": "geofm4eo",
            "Resources": {"Queue": "booster", "Runtime": "5min"},
        },
    )
    def test_venv():
        print("This is a test print to see where it ends up.")
        return "Hello World"

    test_venv()


unicore_executor_test_multi_site_dag = unicore_executor_test_multi_site()
