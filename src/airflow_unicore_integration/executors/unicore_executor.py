"""
to configure for executor:
- Connection details for unicore: conn_id AIRFLOW__UNICORE_EXECUTOR__UNICORE_CONN_ID | should be defined, can be skipped if every task provides one
- location (path) of python virtualenv prepared on hpc system | AIRFLOW__UNICORE_EXECUTOR__DEFAULT_ENV | should be defined, can be skipped if every task provides one

tasks should be allowed to overwrite SITE, CREDENTIALS_*, UNICORE_CONN_ID and DEFAULT_ENV - i.e. everything but the database connection - credentials should be given as a uc_credential object via executor_config

"""

from __future__ import annotations

import importlib
import json
import time
from collections.abc import Sequence
from typing import Any

import pyunicore.client as uc_client
from airflow.exceptions import AirflowConfigException
from airflow.executors.base_executor import BaseExecutor
from airflow.executors.workloads import All
from airflow.executors.workloads import ExecuteTask
from airflow.executors.workloads import WorkloadType
from airflow.models.taskinstancekey import TaskInstanceKey
from airflow.utils.state import TaskInstanceState
from pyunicore import client
from pyunicore.credentials import AuthenticationFailedException
from pyunicore.credentials import Credential
from pyunicore.credentials import create_credential
from requests.exceptions import RequestException

from ..util.es_shipper import DEFAULT_MAX_STREAM_BYTES
from ..util.es_shipper import ElasticsearchLogShipper
from ..util.es_shipper import LogTarget
from ..util.es_shipper import cap_stream
from ..util.es_shipper import shipping_enabled
from ..util.job import JobDescriptionGenerator
from ..util.job import redact_credentials
from ..util.storage import read_storage_file

STATE_MAPPINGS: dict[uc_client.JobStatus, TaskInstanceState] = {
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
    Executor_CONFIG_UNICORE_PRECONFIGURED_SITE_KEY = "site"  # name of the preconfigured site to use , only requried if different from the default site

    AIRFLOW_CONFIG_JOB_DESCRIPTION_GENERATOR_CLASS_KEY = "job_description_generator_class"
    AIRFLOW_CONFIG_JOB_DESCRIPTION_GENERATOR_DERFAULT_CLASS = (
        "airflow_unicore_integration.util.job.NaiveJobDescriptionGenerator"
    )
    supports_multi_team: bool = True
    # Airflow 3.4.0 admits workloads per type. This executor only runs tasks, so callbacks and
    # connection tests stay undeclared and the base class rejects them with a clear error.
    supported_workload_types: frozenset[WorkloadType] = frozenset({WorkloadType.EXECUTE_TASK})
    # serve_logs = True

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        if not hasattr(self, "conf"):
            from airflow.sdk import conf

            self.conf = conf

        # get job description generator class and init params from config
        generator_class_path: str = self.conf.get(  # type: ignore
            "unicore.executor",
            UnicoreExecutor.AIRFLOW_CONFIG_JOB_DESCRIPTION_GENERATOR_CLASS_KEY,
            fallback=UnicoreExecutor.AIRFLOW_CONFIG_JOB_DESCRIPTION_GENERATOR_DERFAULT_CLASS,
        )
        module_name, class_name = generator_class_path.rsplit(".", 1)
        self.configured_generator_class = getattr(importlib.import_module(module_name), class_name)

        # must exist before start() so sync/terminate/queue_workload never hit a missing attribute
        self.active_jobs: dict[TaskInstanceKey, uc_client.Job] = {}
        # task instance uuids, needed to tag shipped logs with ti_id: Airflow 3.4 log queries
        # prefer ti_id over the log id
        self._ti_ids: dict[TaskInstanceKey, Any] = {}
        # the log shipper, or False once we know shipping is switched off for this deployment
        self._log_shipper: ElasticsearchLogShipper | bool | None = None

    def start(self):
        self.active_jobs: dict[TaskInstanceKey, uc_client.Job] = {}  # type: ignore

    def _handle_used_compute_time(self, task: TaskInstanceKey, job: uc_client.Job) -> None:
        pass

    def sync(self) -> None:
        # iterate through task collection and update task/ job status - delete if needed
        for task, job in list(self.active_jobs.items()):
            # TODO if unicore job is still running, but airflow reports failed, success or up_for_retry,
            # kill the still running job to prevent resource consumption
            try:
                job_status = job.status
            except RequestException as re:
                # a single unreadable job must not stop the executor heartbeat for all other tasks
                self.log.warning(
                    "Could not read the unicore job status for task %s, retrying on the next heartbeat: %s",
                    task,
                    re,
                )
                continue
            state = STATE_MAPPINGS.get(job_status)
            if state is None:
                self.log.debug(
                    "Unicore job for task %s reports unknown status %s, leaving it active.",
                    task,
                    job_status,
                )
                continue
            if state == TaskInstanceState.FAILED:
                self.fail(task)
                self.active_jobs.pop(task)
                self._handle_used_compute_time(task, job)
                self._ship_job_logs(task, job)
            elif state == TaskInstanceState.SUCCESS:
                self.success(task)
                self.active_jobs.pop(task)
                self._handle_used_compute_time(task, job)
                self._ship_job_logs(task, job)
            elif state == TaskInstanceState.QUEUED:
                # self.running_state(task, state)
                pass

        return super().sync()

    def get_task_log(self, ti: TaskInstanceKey, try_number: int) -> tuple[list[str], list[str]]:
        # if task is still active, no neeed to check other jobs for match
        job: uc_client.Job | None = None
        if ti.key in self.active_jobs:
            job = self.active_jobs[ti.key]
        # if job is finished, need to search all jobs (that are still present on the target system)
        else:
            job = self._get_job_from_id(ti.key)

        if not job:
            return [], []

        logs = list(job.properties["log"])  # type: ignore
        logs += self._read_job_log_file(job.working_dir, "stdout")
        logs += self._read_job_log_file(job.working_dir, "stderr")

        metalog = [
            f"unicore job name: {job.properties.get('name', 'unknown')}",
            f"unicore job url: {job.properties.get('url', 'unknown')}",
        ]
        return logs, metalog

    def _get_log_shipper(self) -> ElasticsearchLogShipper | None:
        """The shipper for finished job logs, or None when this deployment has no Elasticsearch."""
        if self._log_shipper is None:
            if not shipping_enabled(self.conf):
                self._log_shipper = False
            else:
                max_bytes = int(
                    self.conf.get(
                        "unicore.executor",
                        "ES_LOG_MAX_BYTES",
                        fallback=str(DEFAULT_MAX_STREAM_BYTES),
                    )
                )
                self._log_shipper = ElasticsearchLogShipper(max_stream_bytes=max_bytes)
        return self._log_shipper if isinstance(self._log_shipper, ElasticsearchLogShipper) else None

    def _ship_job_logs(self, key: TaskInstanceKey, job: uc_client.Job) -> None:
        """
        Hand a finished job's streams to the deployment's log store.

        This runs from ``sync()`` because that is the last moment the job's uspace directory
        still exists, and the scheduler is the only process that can reach both UNICORE and an
        Elasticsearch service inside the Kubernetes cluster.
        """
        shipper = self._get_log_shipper()
        if shipper is None:
            self._ti_ids.pop(key, None)
            return

        try:
            # properties has no "url" key on the job pyunicore hands back, resource_url is the one
            job_url = redact_credentials(
                str(getattr(job, "resource_url", "") or job.properties.get("url", "unknown"))  # type: ignore
            )
            parts = [f"*** unicore job {job_url} : stdout ***"]
            parts.append(
                cap_stream(
                    "\n".join(self._read_job_log_file(job.working_dir, "stdout")),
                    shipper.max_stream_bytes,
                )
            )
            parts.append(f"*** unicore job {job_url} : stderr ***")
            parts.append(
                cap_stream(
                    "\n".join(self._read_job_log_file(job.working_dir, "stderr")),
                    shipper.max_stream_bytes,
                )
            )
            text = "\n".join(parts)
        except Exception as exc:  # noqa: BLE001 - collecting logs must never disturb task handling
            self.log.warning(
                "Could not collect the job logs of %s: %s", key, redact_credentials(str(exc))
            )
            self._ti_ids.pop(key, None)
            return

        target = LogTarget(
            dag_id=key.dag_id,
            task_id=key.task_id,
            run_id=key.run_id,
            try_number=key.try_number,
            map_index=key.map_index,
            ti_id=self._ti_ids.pop(key, None),
        )
        shipper.submit(text, target)

    def _read_job_log_file(self, working_dir, name: str) -> list[str]:
        """
        Read one output stream of a unicore job from its working directory.

        A stream that is missing (or already swept away) must not break the whole log
        view, so read failures are reported inline instead of raised. Any error type is
        tolerated on purpose: pyunicore surfaces transport, http and decoding problems
        from several libraries, and none of them may take the log view down.
        """
        try:
            return read_storage_file(working_dir, f"/{name}", errors="replace").split("\n")
        except Exception as exc:  # noqa: BLE001
            self.log.debug("Could not read %s of the unicore job: %s", name, exc)
            reason = redact_credentials(f"{type(exc).__name__}: {exc}")[:200]
            return [f"<{name} is not available ({reason})>"]

    def _get_job_from_id(self, key: TaskInstanceKey) -> uc_client.Job | None:
        # iterate over jobs and check if key matches job name
        unicore_client = self._get_unicore_client()
        # check 50 jobs at a time to prevent server from not returning some jobs
        offset = 0
        number = 50
        job_name = self.configured_generator_class.get_job_name(key)
        while True:
            partial_job_list = unicore_client.get_jobs(offset=offset, num=number)
            if len(partial_job_list) == 0:
                return None
            offset += len(partial_job_list)
            for job in partial_job_list:
                if job.properties["name"] == job_name:  # type: ignore
                    return job

    def _get_unicore_client(self, executor_config: dict | None = None):
        executor_config = executor_config or {}
        overwrite_preconfigured_site = executor_config.get(  # type: ignore
            UnicoreExecutor.Executor_CONFIG_UNICORE_PRECONFIGURED_SITE_KEY, None
        )  # task can provide a site to run at, else use first one from config
        overwrite_unicore_site = executor_config.get(  # type: ignore
            UnicoreExecutor.EXECUTOR_CONFIG_UNICORE_SITE_KEY, None
        )  # task can provide a site url to run at, else preconfigured site is used
        overwrite_unicore_credential = executor_config.get(  # type: ignore
            UnicoreExecutor.EXECUTOR_CONFIG_UNICORE_CREDENTIAL_KEY, None
        )  # task can provide a credential to use, else use preconfigured site credential

        # list of sites
        # each site is a list of strings:
        # [0] site name, [1] site url, [2] unicore user preferences (accounting rules),
        # [3] site specific precommand, [4] using_proxy
        preconfigured_sites: list[list[str]] = json.loads(
            self.conf.get("unicore.executor", "SITES_CONFIG", fallback="")
        )
        site = preconfigured_sites[0]
        if overwrite_preconfigured_site is not None:
            for tmp in preconfigured_sites:
                if tmp[0] == overwrite_preconfigured_site:
                    site = tmp
                    break
        self.log.info(f"using site {site[0]} to submit job.")

        base_url = site[1]

        token: str = self.conf.get(
            "unicore.executor", f"SITES_TOKEN_{site[0].upper()}", fallback=""
        )

        # user preferences are independent of the auth token: they select the accounting
        # group when the token is allowed to use several
        user_preferences = site[2] if len(site) > 2 else None

        if overwrite_unicore_credential is not None:
            token = overwrite_unicore_credential
            self.log.debug("Using user provided token instead of site default.")
        elif not token:
            raise AirflowConfigException(
                f"No credential configured for unicore site '{site[0]}'. Set "
                f"[unicore.executor] SITES_TOKEN_{site[0].upper()} in airflow.cfg, or pass "
                f"'{UnicoreExecutor.EXECUTOR_CONFIG_UNICORE_CREDENTIAL_KEY}' in the task's "
                "executor_config."
            )
        credential: Credential = create_credential(token=token)
        if overwrite_unicore_site is not None:
            base_url = overwrite_unicore_site
            self.log.debug("Using user provided site url instead of site default.")
        if not base_url:
            raise AirflowConfigException(
                f"No site url for unicore site '{site[0]}'. Set its url in "
                f"[unicore.executor] SITES_CONFIG, or pass "
                f"'{UnicoreExecutor.EXECUTOR_CONFIG_UNICORE_SITE_KEY}' in the task's "
                "executor_config."
            )
        self.log.debug(f"Using site url: {base_url}")
        # self.log.debug(f"Using credential: {credential}")
        conn = client.Client(credential, base_url)
        if user_preferences:
            conn.transport.preferences = user_preferences
        return conn

    def _submit_job(self, workload: ExecuteTask) -> uc_client.Job:
        unicore_client = self._get_unicore_client(executor_config=workload.ti.executor_config)
        job_descr = self._create_job_description(workload)
        self.log.debug("Generated job description: %s", redact_credentials(str(job_descr)))
        job = unicore_client.new_job(job_descr)
        self.log.info("Submitted unicore job")
        self.active_jobs[self.get_workload_key(workload)] = job
        return job

    def _process_workloads(self, workload_items: Sequence[All]) -> None:
        """
        Submit the workloads the scheduler handed over and report them as queued.

        Airflow 3.4.0 calls this from the executor heartbeat instead of letting executors submit
        from ``queue_workload``. The base class already limited the list to the free slots, so
        everything in it can go straight to UNICORE.

        Each workload has to leave ``executor_queues`` and enter ``running`` here, and stay in
        ``running`` until ``sync()`` reports how the job ended: the scheduler only accepts executor
        events for tasks the executor still knows about.
        """
        for workload in workload_items:
            if not isinstance(workload, ExecuteTask):
                raise NotImplementedError(
                    f"{type(self).__name__} cannot process {type(workload).__name__} workloads, "
                    f"add WorkloadType.{workload.type.name} to supported_workload_types "
                    "and handle it here."
                )

            key = self.get_workload_key(workload)
            self.executor_queues[workload.type].pop(key, None)
            self.running.add(key)

            try:
                job = self._submit_job(workload)
            except AuthenticationFailedException as auth_exception:
                self.fail(key)
                self.log.error(
                    "Invalid credentials for UNICORE. Failing all UNICORE tasks until this has been resolved."
                )
                self.log.error(auth_exception)
            except RequestException as re:
                # Any kind of requests error, from DNS issues, to timeout Issues, to invalid UNICORE URL issues
                self.fail(key)
                self.log.error(
                    "Some Error appeared during the connection attempt to UNICORE. Failing this task Instance."
                )
                self.log.error(re)
            else:
                # remember the uuid: shipped logs are only findable by 3.4's log reader when they
                # carry ti_id, and the key alone does not have one
                self._ti_ids[key] = getattr(workload.ti, "id", None)
                # the scheduler stores the reported info as the task's external_executor_id, so this
                # is what links a task instance to the unicore job that ran it
                self.event_buffer[key] = (TaskInstanceState.QUEUED, job.resource_url)

    def _create_job_description(self, workload: ExecuteTask) -> dict[str, Any]:
        job_descr_generator: JobDescriptionGenerator = self.configured_generator_class(self.conf)  # type: ignore
        return job_descr_generator.create_job_description(workload)

    def end(self, heartbeat_interval=10) -> None:
        # wait for current jobs to finish, dont start any new ones - but never forever
        shutdown_timeout = float(
            self.conf.get("unicore.executor", "SHUTDOWN_TIMEOUT", fallback="600")
        )
        deadline = time.monotonic() + shutdown_timeout
        while True:
            self.sync()
            if not self.active_jobs:
                break
            if time.monotonic() >= deadline:
                self.log.warning(
                    "Shutdown timed out after %s seconds, %s unicore job(s) are still active: %s",
                    int(shutdown_timeout),
                    len(self.active_jobs),
                    list(self.active_jobs),
                )
                break
            time.sleep(heartbeat_interval)

        # queued log uploads should reach Elasticsearch before this process goes away
        if isinstance(self._log_shipper, ElasticsearchLogShipper):
            self._log_shipper.flush()

    def terminate(self):
        # terminate all jobs
        for task, job in list(self.active_jobs.items()):
            try:
                job.abort()
            except RequestException as re:
                self.log.warning("Could not abort the unicore job for task %s: %s", task, re)
            self._ti_ids.pop(task, None)
        self.end()
        # the executor is going away, so do not leave bookkeeping behind that claims free slots
        self.active_jobs.clear()
        self.running.clear()
        for workload_queue in self.executor_queues.values():
            workload_queue.clear()
