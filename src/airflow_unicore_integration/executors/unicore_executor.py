"""
to configure for executor:
- postgres-conn under which the airflow backend-db ist reachable fromm the hpc system | AIRFLOW__UNICORE_EXECUTOR__SQL_ALCHEMY_CONN | mandatory
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

    supports_pickling = False  # TODO check if this is easy or useful; Whether or not the executor supports reading pickled DAGs from the Database before execution (rather than reading the DAG definition from the file system).

    supports_sentry = False  # TODO no clue what this is, so lets not claim support for it; Whether or not the executor supports Sentry.

    is_local = False  # unicore is definitely not local; Whether or not the executor is remote or local. See the Executor Types section above.

    is_single_threaded = True  # TODO try to do it properly while single-threaded, but switch if needed; Whether or not the executor is single threaded. This is particularly relevant to what database backends are supported. Single threaded executors can run with any backend, including SQLite.

    is_production = False  # TODO set to true once finished; Whether or not the executor should be used for production purposes. A UI message is displayed to users when they are using a non-production ready executor.

    change_sensor_mode_to_reschedule = False  # TODO find out what this does;  Running Airflow sensors in poke mode can block the thread of executors and in some cases Airflow.

    serve_logs = True  # TODO this should be supported, since unicore logs should be appended;  Whether or not the executor supports serving logs, see Logging for Tasks.

    EXECUTOR_CONFIG_PYTHON_ENV_KEY = "python_env"  # full path to a python virtualenv that includes airflow and all required libraries for the task (without the .../bin/activate part)
    EXECUTOR_CONFIG_UNICORE_CONN_KEY = (
        "unicore_connection_id"  # alternative connection id for the Unicore connection to use
    )
    EXECUTOR_CONFIG_UNICORE_SITE_KEY = "unicore_site"  # alternative Unicore site to run at, only required if different than connection default
    EXECUTOR_CONFIG_UNICORE_CREDENTIAL_KEY = "unicore_credential"  # alternative unicore credential to use for the job, only required if different than connection default
    EXECUTOR_CONFIG_USER_ADDED_UNICORE_JOB_DESCRIPTION_KEYS = [
        "Environment",
        "Parameters",
        "Project",
        "Resources",
    ]  # correspond to the unicore job description, will be added to the final job

    def start(self):
        self.active_jobs: Dict[TaskInstanceKey, uc_client.Job] = {}

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
                self.running(task)

        return super().sync()

    def _forward_unicore_log(self, task: TaskInstanceKey, job: uc_client.Job):
        # TODO retrieve unicore logs from job directory and add lines to the executor log vie self.log - detect log level im possible
        pass

    def _get_unicore_client(self, executor_config: dict = {}):
        # include client desires from executor_config
        unicore_conn_id = executor_config.get(
            UnicoreExecutor.EXECUTOR_CONFIG_UNICORE_CONN_KEY,
            conf.get("unicore.executor", "UNICORE_CONN_ID"),
        )  # task can provide a different unicore connection to use, else airflow-wide default is used
        unicore_site = executor_config.get(
            UnicoreExecutor.EXECUTOR_CONFIG_UNICORE_SITE_KEY, None
        )  # task can provide a different site to run at, else default from connetion is used
        unicore_credential = executor_config.get(
            UnicoreExecutor.EXECUTOR_CONFIG_UNICORE_CREDENTIAL_KEY, None
        )  # task can provide a different credential to use, else default from connection is used
        return unicore_hooks.UnicoreHook(uc_conn_id=unicore_conn_id).get_conn(
            overwrite_base_url=unicore_site, overwrite_credential=unicore_credential
        )

    def _submit_job(
        self,
        key: TaskInstanceKey,
        command: List[str],
        queue: str | None = None,
        executor_config: dict = {},
    ):
        uc_client = self._get_unicore_client(executor_config=executor_config)
        job_descr = self._create_job_description(key, command, queue, executor_config)
        self.log.info("Generated job description")
        self.log.debug(str(job_descr))
        job = uc_client.new_job(job_descr)
        self.log.info("Submitted unicore job")
        self.active_jobs[key] = job
        return job

    def _create_job_description(
        self,
        key: TaskInstanceKey,
        command: List[str],
        queue: str | None = None,
        executor_config: dict = {},
    ) -> Dict[str, Any]:
        job_descr_dict: Dict[str, Any] = {}
        # get user config from executor_config
        user_added_env: Dict[str, str] = executor_config.get("Environment", None)
        user_added_params: Dict[str, str] = executor_config.get("Parameters", None)
        user_added_project: str = executor_config.get("Project", None)
        user_added_resources: Dict[str, str] = executor_config.get("Resources", None)
        user_defined_python_env: str = executor_config.get(
            UnicoreExecutor.EXECUTOR_CONFIG_PYTHON_ENV_KEY, None
        )
        # get command from cmd and fix dag path in arguments
        fixed_path_cmd = command[:]
        fixed_path_cmd[-1] = "DAGS_FOLDER/dag.py"
        local_dag_path = conf.get("core", "DAGS_FOLDER") + command[-1][11:]

        sql_alchemy_conn = conf.get("unicore.executor", "SQL_ALCHEMY_CONN")

        # check which python virtualenv to use
        if user_defined_python_env:
            python_env = user_defined_python_env
        else:
            python_env = conf.get("unicore.executor", "DEFAULT_ENV")

        # prepare dag file to be uploaded via unicore
        dag_file = open(local_dag_path)
        dag_content = dag_file.readlines()
        dag_import = {"To": "dag.py", "Data": dag_content}

        # start filling the actual job description
        job_descr_dict["Name"] = f"{key.dag_id} - {key.task_id} - {key.run_id} - {key.try_number}"
        job_descr_dict["Executable"] = command[0]
        job_descr_dict["Arguments"] = fixed_path_cmd[1:]
        job_descr_dict["Environment"] = {
            "AIRFLOW__DATABASE__SQL_ALCHEMY_CONN": sql_alchemy_conn,
            "AIRFLOW__CORE__DAGS_FOLDER": "./",
            "AIRFLOW__CORE__EXECUTOR": "LocalExecutor,airflow_unicore_integration.executors.unicore_executor.UnicoreExecutor",
        }
        job_descr_dict["User precommand"] = f"source {python_env}/bin/activate"
        job_descr_dict["RunUserPrecommandOnLoginNode"] = (
            "false"  # precommand is activating the python env, this can also be done on compute node right before running the job
        )
        job_descr_dict["Imports"] = [dag_import]
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

    def execute_async(
        self,
        key: TaskInstanceKey,
        command: List[str],
        queue: str | None = None,
        executor_config: dict = {},
    ) -> None:
        # submit job to unicore and add to active_jobs dict for task state management
        job = self._submit_job(key, command, queue, executor_config)
        self.active_jobs[key] = job
        # return super().execute_async(key, command, queue, executor_config)

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
