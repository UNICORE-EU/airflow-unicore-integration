"""
to configure for executor:
- Connection details for unicore: conn_id AIRFLOW__UNICORE_EXECUTOR__UNICORE_CONN_ID | should be defined, can be skipped if every task provides one
- location (path) of python virtualenv prepared on hpc system | AIRFLOW__UNICORE_EXECUTOR__DEFAULT_ENV | should be defined, can be skipped if every task provides one

tasks should be allowed to overwrite SITE, CREDENTIALS_*, UNICORE_CONN_ID and DEFAULT_ENV - i.e. everything but the database connection - credentials should be given as a uc_credential object via executor_config

"""

import time
from typing import Any
from typing import Dict
from typing import List

import pyunicore.client as uc_client
from airflow.configuration import conf
from airflow.executors.base_executor import BaseExecutor
from airflow.executors.workloads import All
from airflow.executors.workloads import ExecuteTask
from airflow.models.taskinstancekey import TaskInstanceKey
from airflow.utils.state import TaskInstanceState

from airflow_unicore_integration.hooks import unicore_hooks

STATE_MAPPINGS: Dict[uc_client.JobStatus, TaskInstanceState] = {
    uc_client.JobStatus.UNDEFINED: TaskInstanceState.FAILED,
    uc_client.JobStatus.READY: TaskInstanceState.QUEUED,
    uc_client.JobStatus.STAGINGIN: TaskInstanceState.QUEUED,
    uc_client.JobStatus.QUEUED: TaskInstanceState.QUEUED,
    uc_client.JobStatus.RUNNING: TaskInstanceState.RUNNING,
    uc_client.JobStatus.STAGINGOUT: TaskInstanceState.RUNNING,
    uc_client.JobStatus.SUCCESSFUL: TaskInstanceState.SUCCESS,
    uc_client.JobStatus.FAILED: TaskInstanceState.FAILED,
}


class UnicoreExecutor(BaseExecutor):
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

    def start(self):
        self.active_jobs: Dict[TaskInstanceKey, uc_client.Job] = {}
        self.uc_conn = unicore_hooks.UnicoreHook().get_conn()

    def sync(self) -> None:
        # iterate through task collection and update task/ job status - delete if needed
        for task, job in list(self.active_jobs.items()):
            state = STATE_MAPPINGS[job.status]
            if state == TaskInstanceState.FAILED:
                self.fail(task)
                self._forward_unicore_log(task, job)
                self.active_jobs.pop(task)
            elif state == TaskInstanceState.SUCCESS:
                self.success(task)
                self._forward_unicore_log(task, job)
                self.active_jobs.pop(task)
            elif state == TaskInstanceState.RUNNING:
                self.running_state(task, state)

        return super().sync()

    def _forward_unicore_log(self, task: TaskInstanceKey, job: uc_client.Job) -> List[str]:
        # TODO retrieve unicore logs from job directory and return
        return []

    def _get_unicore_client(self, executor_config: dict | None = {}):
        # TODO fix this only temporary solution
        return self.uc_conn
        # END TODO fix this
        # include client desires from executor_config
        unicore_conn_id = executor_config.get(  # type: ignore
            UnicoreExecutor.EXECUTOR_CONFIG_UNICORE_CONN_KEY,
            conf.get("unicore.executor", "UNICORE_CONN_ID"),
        )  # task can provide a different unicore connection to use, else airflow-wide default is used
        self.log.info(f"Using base unicore connection with id '{unicore_conn_id}'")
        hook = unicore_hooks.UnicoreHook(uc_conn_id=unicore_conn_id)
        unicore_site = executor_config.get(  # type: ignore
            UnicoreExecutor.EXECUTOR_CONFIG_UNICORE_SITE_KEY, None
        )  # task can provide a different site to run at, else default from connetion is used
        unicore_credential = executor_config.get(  # type: ignore
            UnicoreExecutor.EXECUTOR_CONFIG_UNICORE_CREDENTIAL_KEY, None
        )  # task can provide a different credential to use, else default from connection is used
        return hook.get_conn(
            overwrite_base_url=unicore_site, overwrite_credential=unicore_credential
        )

    def _submit_job(self, workload: ExecuteTask):
        uc_client = self._get_unicore_client(executor_config=workload.ti.executor_config)
        job_descr = self._create_job_description(workload)
        self.log.info("Generated job description")
        self.log.debug(str(job_descr))
        job = uc_client.new_job(job_descr)
        self.log.info("Submitted unicore job")
        self.active_jobs[workload.ti.key] = job
        return job

    def _create_job_description(self, workload: ExecuteTask) -> Dict[str, Any]:
        key: TaskInstanceKey = workload.ti.key
        executor_config = workload.ti.executor_config
        if not executor_config:
            executor_config = {}
        job_descr_dict: Dict[str, Any] = {}
        # get user config from executor_config
        user_added_env: Dict[str, str] = executor_config.get(UnicoreExecutor.EXECUTOR_CONFIG_ENVIRONMENT, None)  # type: ignore
        user_added_params: Dict[str, str] = executor_config.get(UnicoreExecutor.EXECUTOR_CONFIG_PARAMETERS, None)  # type: ignore
        user_added_project: str = executor_config.get(UnicoreExecutor.EXECUTOR_CONFIG_PROJECT, None)  # type: ignore
        user_added_resources: Dict[str, str] = executor_config.get(UnicoreExecutor.EXECUTOR_CONFIG_RESOURCES, None)  # type: ignore
        user_added_pre_commands: list[str] = executor_config.get(UnicoreExecutor.EXECUTOR_CONFIG_PRE_COMMANDS, [])  # type: ignore
        user_defined_python_env: str = workload.ti.executor_config.get(UnicoreExecutor.EXECUTOR_CONFIG_PYTHON_ENV_KEY, None)  # type: ignore
        # get local dag path from cmd and fix dag path in arguments
        dag_rel_path = str(workload.dag_rel_path)
        if dag_rel_path.startswith("DAG_FOLDER"):
            dag_rel_path = dag_rel_path[10:]
        local_dag_path = conf.get("core", "DAGS_FOLDER") + "/" + dag_rel_path
        base_url = conf.get("api", "base_url", fallback="/")
        default_execution_api_server = f"{base_url.rstrip('/')}/execution/"
        server = conf.get("core", "execution_api_server_url", fallback=default_execution_api_server)

        # check which python virtualenv to use
        if user_defined_python_env:
            python_env = user_defined_python_env
        else:
            python_env = conf.get("unicore.executor", "DEFAULT_ENV")

        # prepare dag file to be uploaded via unicore
        dag_file = open(local_dag_path)
        dag_content = dag_file.readlines()
        dag_import = {"To": dag_rel_path, "Data": dag_content}
        worker_script_import = {
            "To": "run_task_via_supervisor.py",
            "From": "https://gist.githubusercontent.com/cboettcher/3f1101a1d1b67e7944d17c02ecd69930/raw/6da9ec16ba598ddda9cf288900498fab5e226788/run_task_via_supervisor.py",
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
        }
        user_added_pre_commands.append(f"source {python_env}/bin/activate")
        job_descr_dict["User precommand"] = ";".join(user_added_pre_commands)
        job_descr_dict["RunUserPrecommandOnLoginNode"] = (
            "false"  # precommand is activating the python env, this can also be done on compute node right before running the job
        )
        job_descr_dict["Imports"] = [dag_import, worker_script_import]
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

    def queue_workload(self, workload: ExecuteTask | All, session):
        if not isinstance(workload, ExecuteTask):
            raise TypeError(f"Don't know how to queue workload of type {type(workload).__name__}")

        # submit job to unicore and add to active_jobs dict for task state management
        job = self._submit_job(workload)
        self.active_jobs[workload.ti.key] = job

    def end(self, heartbeat_interval=10) -> None:
        # wait for current jobs to finish, dont start any new ones
        while True:
            self.sync()
            if not self.active_jobs:
                break
            time.sleep(heartbeat_interval)

    def terminate(self):
        # terminate all jobs
        for task, job in list(self.active_jobs.items()):
            job.abort()
        self.end()
