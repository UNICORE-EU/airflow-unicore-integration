from typing import Any, List, Dict
from airflow.executors.base_executor import BaseExecutor
from airflow.models.taskinstance import TaskInstance
from airflow.models.taskinstancekey import TaskInstanceKey


import pyunicore.client as uc_client
import pyunicore.credentials as uc_credentials
from airflow_unicore_integration.hooks import unicore_hooks


class UnicoreTaskInfo():
    # store the information about a single unicore task-job and allow some information retrieval
    pass

class UnicoreTaskCollection():
    # store all active unicore tasks, map them to their dag and task id and store their state and logs callback
    pass

# TODO some kind of submission queue - 


'''
HElper snippet TODO remove
airflow-scheduler-1  | [2024-09-17T10:42:35.957+0000] {unicore_executor.py:48} INFO - Key: TaskInstanceKey(dag_id='executor-test', task_id='print_date', run_id='manual__2024-09-17T10:42:34.935112+00:00', try_number=1, map_index=-1)
airflow-scheduler-1  | [2024-09-17T10:42:35.958+0000] {unicore_executor.py:49} INFO - command: ['airflow', 'tasks', 'run', 'executor-test', 'print_date', 'manual__2024-09-17T10:42:34.935112+00:00', '--local', '--subdir', 'DAGS_FOLDER/unicore-executor-test.py']
airflow-scheduler-1  | [2024-09-17T10:42:35.958+0000] {unicore_executor.py:50} INFO - queue: default
airflow-scheduler-1  | [2024-09-17T10:42:35.958+0000] {unicore_executor.py:51} INFO - executor_config: {}
'''

class UnicoreExecutor(BaseExecutor):
    

    supports_pickling = False #TODO check if this is easy or useful; Whether or not the executor supports reading pickled DAGs from the Database before execution (rather than reading the DAG definition from the file system).

    supports_sentry = False #TODO no clue what this is, so lets not claim support for it; Whether or not the executor supports Sentry.

    is_local = False # unicore is definitely not local; Whether or not the executor is remote or local. See the Executor Types section above.

    is_single_threaded =  True # TODO try to do it properly while single-threaded, but switch if needed; Whether or not the executor is single threaded. This is particularly relevant to what database backends are supported. Single threaded executors can run with any backend, including SQLite.

    is_production = False # TODO set to true once finished; Whether or not the executor should be used for production purposes. A UI message is displayed to users when they are using a non-production ready executor.

    change_sensor_mode_to_reschedule = False  # TODO find out what this does;  Running Airflow sensors in poke mode can block the thread of executors and in some cases Airflow.

    serve_logs = True # Whether or not the executor supports serving logs, see Logging for Tasks.

    def start(self):
        return super().start()

    def sync(self) -> None:
        # iterate through submission queue and submit
        # iterate through task collection and update task/ job status
        return super().sync()
    
    def _create_job_description(self, key: TaskInstanceKey, command: List[str], queue: str| None = None, executor_config: Any | None = None) -> Dict[str,Any]:
        job_descr_dict : Dict[str, Any] = {}
        # TODO get env, params, etc from executor_config
        # get command from cmd
        job_descr_dict["Name"] = f"{key.run_id} - {key.task_id}"
        job_descr_dict["Executable"] = command[0]
        job_descr_dict["Arguments"] = command[1:]
        
        return job_descr_dict
    
    def execute_async(self, key: TaskInstanceKey, command: List[str], queue: str | None = None, executor_config: Any | None = None) -> None:
        # only put tsak in submission queue, actually submit to unicore when sync is called by the heartbeat
        self.log.info(f"Key: {key}")
        self.log.info(f"command: {command}")
        self.log.info(f"queue: {queue}")
        self.log.info(f"executor_config: {executor_config}")
        hook = unicore_hooks.UnicoreHook()
        uc_client = hook.get_conn()
        job_descr = self._create_job_description(key, command, queue, executor_config)
        self.log.info("Generated job description")
        self.log.info(str(job_descr))
        job = uc_client.new_job(job_descr)
        self.log.info("Submitted unicore job")

        # TODO store job in local database so that sync can update its state
        #return super().execute_async(key, command, queue, executor_config)
    
    def get_task_log(self, ti: TaskInstance, try_number: int) -> tuple[list[str], list[str]]:
        return super().get_task_log(ti, try_number)
    

