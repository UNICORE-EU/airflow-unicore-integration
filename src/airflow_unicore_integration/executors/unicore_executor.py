"""
to configure for executor:
- Connection details for unicore: conn_id AIRFLOW__UNICORE_EXECUTOR__UNICORE_CONN_ID | should be defined, can be skipped if every task provides one
- location (path) of python virtualenv prepared on hpc system | AIRFLOW__UNICORE_EXECUTOR__DEFAULT_ENV | should be defined, can be skipped if every task provides one

tasks should be allowed to overwrite SITE, CREDENTIALS_*, UNICORE_CONN_ID and DEFAULT_ENV - i.e. everything but the database connection - credentials should be given as a uc_credential object via executor_config

"""

import time
from typing import Any
from typing import Dict

import pyunicore.client as uc_client
from airflow.configuration import conf
from airflow.executors.base_executor import BaseExecutor
from airflow.executors.workloads import All
from airflow.executors.workloads import ExecuteTask
from airflow.models.taskinstance import TaskInstance
from airflow.models.taskinstancekey import TaskInstanceKey
from airflow.utils.state import TaskInstanceState
from pyunicore import client
from pyunicore.credentials import Credential
from pyunicore.credentials import create_credential

from ..util.job import JobDescriptionGenerator
from ..util.job import NaiveJobDescriptionGenerator

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

    EXECUTOR_CONFIG_UNICORE_SITE_KEY = "unicore_site"  # alternative Unicore site to run at, only required if different than connection default
    EXECUTOR_CONFIG_UNICORE_CREDENTIAL_KEY = "unicore_credential"  # alternative unicore credential to use for the job, only required if different than connection default

    serve_logs = True

    def start(self):
        self.active_jobs: Dict[TaskInstanceKey, uc_client.Job] = {}
        # TODO get job description generator class and init params from config
        self.job_descr_generator: JobDescriptionGenerator = NaiveJobDescriptionGenerator()

    def _handle_used_compute_time(self, task: TaskInstanceKey, job: uc_client.Job) -> None:
        pass

    def sync(self) -> None:
        # iterate through task collection and update task/ job status - delete if needed
        for task, job in list(self.active_jobs.items()):
            state = STATE_MAPPINGS[job.status]
            if state == TaskInstanceState.FAILED:
                self.fail(task)
                self.active_jobs.pop(task)
                self._handle_used_compute_time(task, job)
            elif state == TaskInstanceState.SUCCESS:
                self.success(task)
                self.active_jobs.pop(task)
                self._handle_used_compute_time(task, job)
            elif state == TaskInstanceState.QUEUED:
                self.running_state(task, state)

        return super().sync()

    def get_task_log(self, ti: TaskInstance, try_number: int) -> tuple[list[str], list[str]]:
        # if task is still active, no neeed to check other jobs for match
        job: uc_client.Job | None = None
        if ti.key in self.active_jobs:
            job = self.active_jobs[ti.key]
        # if job is finished, need to search all jobs (that are still present on the target system)
        else:
            job = self._get_job_from_id(ti.key)

        if not job:
            return [], []

        # TODO return filecontent for stdout and stderr from jobdirectory
        logs = job.properties["log"]  # type: ignore
        working_dir = job.working_dir
        stdout = working_dir.stat("/stdout")
        stderr = working_dir.stat("/stderr")

        lines_out = stdout.raw().read().decode("utf-8").split("\n")  # type: ignore
        lines_err = stderr.raw().read().decode("utf-8").split("\n")  # type: ignore

        return logs + lines_out + lines_err, ["Test", "Where is this shown??"]

    def _get_job_from_id(self, key: TaskInstanceKey) -> uc_client.Job | None:
        # iterate over jobs and check if key matches job name
        unicore_client = self._get_unicore_client()
        # check 50 jobs at a time to prevent server from not returning some jobs
        offset = 0
        number = 50
        job_name = self.job_descr_generator.get_job_name(key)
        while True:
            partial_job_list = unicore_client.get_jobs(offset=offset, num=number)
            if len(partial_job_list) == 0:
                return None
            offset += len(partial_job_list)
            for job in partial_job_list:
                if job.properties["name"] == job_name:  # type: ignore
                    return job

    def _get_unicore_client(self, executor_config: dict | None = {}):
        overwrite_unicore_site = executor_config.get(  # type: ignore
            UnicoreExecutor.EXECUTOR_CONFIG_UNICORE_SITE_KEY, None
        )  # task can provide a different site to run at, else default from connetion is used
        overwrite_unicore_credential = executor_config.get(  # type: ignore
            UnicoreExecutor.EXECUTOR_CONFIG_UNICORE_CREDENTIAL_KEY, None
        )  # task can provide a different credential to use, else default from connection is used
        token = conf.get("unicore.executor", "AUTH_TOKEN", fallback="")
        if overwrite_unicore_credential is not None:
            token = overwrite_unicore_credential
            self.log.debug("Using user provided token.")
        base_url = conf.get(
            "unicore.executor", "DEFAULT_URL", fallback="http://localhost:8080/DEMO-SITE/rest/core"
        )
        credential: Credential = create_credential(token=token)
        if overwrite_unicore_site is not None:
            base_url = overwrite_unicore_site
            self.log.debug("Using user provided site.")
        if not base_url:
            raise TypeError()
        self.log.debug(f"Using site: {base_url}")
        self.log.debug(f"Using credential: {credential}")
        conn = client.Client(credential, base_url)
        return conn

    def _submit_job(self, workload: ExecuteTask):
        unicore_client = self._get_unicore_client(executor_config=workload.ti.executor_config)
        job_descr = self._create_job_description(workload)
        self.log.info("Generated job description")
        self.log.debug(str(job_descr))
        job = unicore_client.new_job(job_descr)
        self.log.info("Submitted unicore job")
        self.active_jobs[workload.ti.key] = job
        return job

    def _create_job_description(self, workload: ExecuteTask) -> Dict[str, Any]:
        return self.job_descr_generator.create_job_description(workload)

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
