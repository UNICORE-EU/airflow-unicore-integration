import pendulum
from airflow.sdk import dag
from airflow.sdk import task


@dag(
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    catchup=False,
    tags=["example"],
)
def unicore_executor_template_dag():
    custom_python_env = "/p/project1/paj2206/airflow-venv/testing-env/bin/activate"

    @task(
        executor="airflow_unicore_integration.executors.unicore_executor.UnicoreExecutor",
        executor_config={
            "python_env": custom_python_env,  # path to an activation script with takes care of loading the environment (including possible module load commands). This env MUST contain "apache-airflow-task-sdk" and "apache-airflow-providers-git"
            "precommands": [  # a list of strings containing commands to be executed on the login node before submitting the job
                f". {custom_python_env} && pip install pip-install-test",  # this is also a place to modify the virtualenv while still having internet access
                "date",
                "echo These precommands get executed on the login node.",
            ],
            "postcommands": [  # a list of strings containing commands to be executed on the login node after finishing the job successfully
                "date",
                "echo These postcommands get executed on the login node.",
            ],
            "Project": "paj2206",  # the name of the project for proper budget allocation. Defaults to the service owners default project if not given, and is therefore probably not neccessary.
            "Resources": {  # a dictionary containing resource requests. Available keys depend on the unicore installation
                "Nodes": 1
            },
            "Environment": {  # a dictionary of environment variables to be configured for the job
                "ENV_VAR": "value",
                "ANOTHER_ENV": "another value",
            },
            "job_type": "batch",  # the type of the job as defined by unicore. Possibilites are: "batch" and "on_login_node"
        },
    )
    def full_template():
        print("This command gets executed on a node matching the 'job_type' attribute.")
        return "Hello World"

    @task(
        executor="airflow_unicore_integration.executors.unicore_executor.UnicoreExecutor",
        executor_config={
            "python_env": custom_python_env,  # path to an activation script with takes care of loading the environment (including possible module load commands). This env MUST contain "apache-airflow-task-sdk" and "apache-airflow-providers-git"
            "precommands": [  # a list of strings containing commands to be executed on the login node before submitting the job
                f". {custom_python_env} && pip install pip-install-test",  # this is also a place to modify the virtualenv while still having internet access
                "date",
                "echo These precommands get executed on the login node.",
            ],
            "postcommands": [  # a list of strings containing commands to be executed on the login node after finishing the job successfully
                "date",
                "echo These postcommands get executed on the login node.",
            ],
            "job_type": "on_login_node",  # the type of the job as defined by unicore. Possibilites are: "batch" and "on_login_node"
            "login_node": "jusuf3",  # specify a login node to use. * and ? wildcards are allowed
        },
    )
    def login_template():
        print("This command gets executed on a node matching the 'job_type' attribute.")
        return "Hello World"

    full_template()
    login_template()


unicore_executor_test_3_dag = unicore_executor_template_dag()
