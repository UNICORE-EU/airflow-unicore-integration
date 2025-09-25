import os
from typing import Any
from typing import Dict

from airflow.configuration import conf
from airflow.executors.workloads import ExecuteTask
from airflow.models.taskinstancekey import TaskInstanceKey


class JobDescriptionGenerator:
    """
    A generator class for generating unicore jhob descriptions that may supprot different kinds of systems and/ or environments.
    """

    EXECUTOR_CONFIG_PYTHON_ENV_KEY = "python_env"  # full path to a python virtualenv that includes airflow and all required libraries for the task (without the .../bin/activate part)
    EXECUTOR_CONFIG_RESOURCES = "Resources"  # gets added to the unicore job description
    EXECUTOR_CONFIG_ENVIRONMENT = "Environment"  # gets added to the unicore job description
    EXECUTOR_CONFIG_PARAMETERS = "Parameters"  # gets added to the unicore job description
    EXECUTOR_CONFIG_PROJECT = "Project"  # gets added to the unicore job description
    EXECUTOR_CONFIG_PRE_COMMANDS = "precommands"  # gets added to the unicore job description
    EXECUTOR_CONFIG_UNICORE_CONN_KEY = (
        "unicore_connection_id"  # alternative connection id for the Unicore connection to use
    )
    EXECUTOR_CONFIG_UNICORE_SITE_KEY = "unicore_site"  # alternative Unicore site to run at, only required if different than connection default
    EXECUTOR_CONFIG_UNICORE_CREDENTIAL_KEY = "unicore_credential"  # alternative unicore credential to use for the job, only required if different than connection default

    def create_job_description(self, workload: ExecuteTask) -> Dict[str, Any]:
        raise NotImplementedError()


class NaiveJobDescriptionGenerator(JobDescriptionGenerator):
    """
    This class generates a naive unicore job, that expects there to be a working python env containign airflow and any other required dependencies on the executing system.
    """

    def create_job_description(self, workload: ExecuteTask) -> Dict[str, Any]:
        key: TaskInstanceKey = workload.ti.key
        executor_config = workload.ti.executor_config
        if not executor_config:
            executor_config = {}
        job_descr_dict: Dict[str, Any] = {}
        # get user config from executor_config
        user_added_env: Dict[str, str] = executor_config.get(JobDescriptionGenerator.EXECUTOR_CONFIG_ENVIRONMENT, None)  # type: ignore
        user_added_params: Dict[str, str] = executor_config.get(JobDescriptionGenerator.EXECUTOR_CONFIG_PARAMETERS, None)  # type: ignore
        user_added_project: str = executor_config.get(JobDescriptionGenerator.EXECUTOR_CONFIG_PROJECT, None)  # type: ignore
        user_added_resources: Dict[str, str] = executor_config.get(JobDescriptionGenerator.EXECUTOR_CONFIG_RESOURCES, None)  # type: ignore
        user_added_pre_commands: list[str] = executor_config.get(JobDescriptionGenerator.EXECUTOR_CONFIG_PRE_COMMANDS, [])  # type: ignore
        user_defined_python_env: str = workload.ti.executor_config.get(JobDescriptionGenerator.EXECUTOR_CONFIG_PYTHON_ENV_KEY, None)  # type: ignore
        # get local dag path from cmd and fix dag path in arguments
        dag_rel_path = str(workload.dag_rel_path)
        if dag_rel_path.startswith("DAG_FOLDER"):
            dag_rel_path = dag_rel_path[10:]
        # local_dag_path = conf.get("core", "DAGS_FOLDER") + "/" + dag_rel_path
        base_url = conf.get("api", "base_url", fallback="/")
        default_execution_api_server = f"{base_url.rstrip('/')}/execution/"
        server = conf.get(
            "unicore.executor", "execution_api_server_url", fallback=default_execution_api_server
        )

        # check which python virtualenv to use
        if user_defined_python_env:
            python_env = user_defined_python_env
        else:
            python_env = conf.get("unicore.executor", "DEFAULT_ENV")
        # prepare dag file to be uploaded via unicore
        # dag_file = open(local_dag_path)
        # dag_content = dag_file.readlines()
        # dag_import = {"To": dag_rel_path, "Data": dag_content}
        worker_script_import = {
            "To": "run_task_via_supervisor.py",
            "From": "https://gist.githubusercontent.com/cboettcher/3f1101a1d1b67e7944d17c02ecd69930/raw/1d90bf38199d8c0adf47a79c8840c3e3ddf57462/run_task_via_supervisor.py",
        }
        # start filling the actual job description
        job_descr_dict["Name"] = f"{key.dag_id} - {key.task_id} - {key.run_id} - {key.try_number}"
        job_descr_dict["Executable"] = (
            "python"  # TODO may require module load to be setup for some systems
        )
        job_descr_dict["Arguments"] = [
            "run_task_via_supervisor.py",
            f"--json-string '{workload.model_dump_json()}'",
        ]
        job_descr_dict["Environment"] = {
            "AIRFLOW__CORE__EXECUTION_API_SERVER_URL": server,
            "AIRFLOW__CORE__DAGS_FOLDER": "./",
            "AIRFLOW__LOGGING__LOGGING_LEVEL": "DEBUG",
            "AIRFLOW__CORE__EXECUTOR": "LocalExecutor,airflow_unicore_integration.executors.unicore_executor.UnicoreExecutor",
            "AIRFLOW__DAG_PROCESSOR__DAG_BUNDLE_CONFIG_LIST": os.environ.get(
                "AIRFLOW__DAG_PROCESSOR__DAG_BUNDLE_CONFIG_LIST", ""
            ),
        }
        # insert connection details that are provided via env vars to get bundles
        for env_key in os.environ.keys():
            if env_key.startswith("AIRFLOW_CONN_"):
                job_descr_dict["Environment"][env_key] = os.environ[env_key]
        user_added_pre_commands.append(f"source {python_env}/bin/activate")
        job_descr_dict["User precommand"] = ";".join(user_added_pre_commands)
        job_descr_dict["RunUserPrecommandOnLoginNode"] = (
            "false"  # precommand includes activating the python env, this should be done on compute node right before running the job
        )
        job_descr_dict["Imports"] = [worker_script_import]
        # add user defined options to description
        if user_added_env:
            job_descr_dict["Environment"].update(user_added_env)
        if user_added_params:
            job_descr_dict["Parameters"] = user_added_params
        if user_added_project:
            job_descr_dict["Project"] = user_added_project
        if user_added_resources:
            job_descr_dict["Resources"] = user_added_resources

        return job_descr_dict
