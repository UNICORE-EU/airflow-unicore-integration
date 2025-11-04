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
from pyunicore import client
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

    EXECUTOR_CONFIG_UNICORE_CONN_KEY = (
        "unicore_connection_id"  # alternative connection id for the Unicore connection to use
    )
    EXECUTOR_CONFIG_UNICORE_SITE_KEY = "unicore_site"  # alternative Unicore site to run at, only required if different than connection default
    EXECUTOR_CONFIG_UNICORE_CREDENTIAL_KEY = "unicore_credential"  # alternative unicore credential to use for the job, only required if different than connection default

    def start(self):
        self.active_jobs: Dict[TaskInstanceKey, uc_client.Job] = {}
        # TODO get job description generator class and init params from config
        self.job_descr_generator: JobDescriptionGenerator = NaiveJobDescriptionGenerator()

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
        overwrite_unicore_site = executor_config.get(  # type: ignore
            UnicoreExecutor.EXECUTOR_CONFIG_UNICORE_SITE_KEY, None
        )  # task can provide a different site to run at, else default from connetion is used
        overwrite_unicore_credential = executor_config.get(  # type: ignore
            UnicoreExecutor.EXECUTOR_CONFIG_UNICORE_CREDENTIAL_KEY, None
        )  # task can provide a different credential to use, else default from connection is used
        token = conf.get("unicore.executor", "AUTH_TOKEN", fallback="")
        base_url = conf.get(
            "unicore.executor", "DEFAULT_URL", fallback="http://localhost:8080/DEMO-SITE/rest/core"
        )
        credential = create_credential(token=token)
        if overwrite_unicore_site is not None:
            base_url = overwrite_unicore_site
        if overwrite_unicore_credential is not None:
            credential = overwrite_unicore_credential
        if not base_url:
            raise TypeError()
        conn = client.Client(credential, base_url)
        return conn

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
