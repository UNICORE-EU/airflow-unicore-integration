from typing import Any, List, Dict
from airflow.executors.base_executor import BaseExecutor
from airflow.models.taskinstance import TaskInstance
from airflow.models.taskinstancekey import TaskInstanceKey


import pyunicore.client as uc_client
import pyunicore.credentials as uc_credentials
from airflow_unicore_integration.hooks import unicore_hooks
from airflow.configuration import conf


'''
Helper snippet TODO remove
airflow-scheduler-1  | [2024-09-17T10:42:35.957+0000] {unicore_executor.py:48} INFO - Key: TaskInstanceKey(dag_id='executor-test', task_id='print_date', run_id='manual__2024-09-17T10:42:34.935112+00:00', try_number=1, map_index=-1)
airflow-scheduler-1  | [2024-09-17T10:42:35.958+0000] {unicore_executor.py:49} INFO - command: ['airflow', 'tasks', 'run', 'executor-test', 'print_date', 'manual__2024-09-17T10:42:34.935112+00:00', '--local', '--subdir', 'DAGS_FOLDER/unicore-executor-test.py']
airflow-scheduler-1  | [2024-09-17T10:42:35.958+0000] {unicore_executor.py:50} INFO - queue: default
airflow-scheduler-1  | [2024-09-17T10:42:35.958+0000] {unicore_executor.py:51} INFO - executor_config: {}

{
    'Name': 'manual__2024-10-08T13:41:27.076506+00:00 - print_date', 
    'Executable': 'airflow', 
    'Arguments': [
        'tasks', 
        'run', 
        'unicore-executor-test', 
        'print_date', 
        'manual__2024-10-08T13:41:27.076506+00:00', 
        '--local',
        '--subdir', 
        'DAGS_FOLDER/dag.py'
    ], 
    'Environment': {
        'AIRFLOW__DATABASE__SQL_ALCHEMY_CONN': 'postgresql+psycopg2://airflow:airflow@postgres/airflow', 
        'AIRFLOW__CORE__DAGS_FOLDER': './'
    }, 
    'Imports': {
        'To': 'dag.py', 
        'From': '/opt/airflow/dags/unicore-executor-test.py'
    }
}

'''

class UnicoreExecutor(BaseExecutor):
    

    supports_pickling = True #TODO check if this is easy or useful; Whether or not the executor supports reading pickled DAGs from the Database before execution (rather than reading the DAG definition from the file system).

    supports_sentry = False #TODO no clue what this is, so lets not claim support for it; Whether or not the executor supports Sentry.

    is_local = False # unicore is definitely not local; Whether or not the executor is remote or local. See the Executor Types section above.

    is_single_threaded =  True # TODO try to do it properly while single-threaded, but switch if needed; Whether or not the executor is single threaded. This is particularly relevant to what database backends are supported. Single threaded executors can run with any backend, including SQLite.

    is_production = False # TODO set to true once finished; Whether or not the executor should be used for production purposes. A UI message is displayed to users when they are using a non-production ready executor.

    change_sensor_mode_to_reschedule = False  # TODO find out what this does;  Running Airflow sensors in poke mode can block the thread of executors and in some cases Airflow.

    serve_logs = True # Whether or not the executor supports serving logs, see Logging for Tasks.

    def start(self):
        self.active_jobs : Dict[TaskInstanceKey, uc_client.Job] = {}

    def sync(self) -> None:
        # iterate through submission queue and submit
        # iterate through task collection and update task/ job status
        return super().sync()
    
    def _create_job_description(self, key: TaskInstanceKey, command: List[str], queue: str| None = None, executor_config: Any | None = None) -> Dict[str,Any]:
        job_descr_dict : Dict[str, Any] = {}
        # TODO get env, params, etc from executor_config
        # get command from cmd
        fixed_path_cmd = command[:]
        fixed_path_cmd[-1] = "DAGS_FOLDER/dag.py"
        local_dag_path = conf.get("core", "DAGS_FOLDER") + command[-1][11:]
        dag_file = open(local_dag_path)
        dag_content = dag_file.readlines()
        

        job_descr_dict["Name"] = f"{key.run_id} - {key.task_id}"
        job_descr_dict["Executable"] = command[0]
        job_descr_dict["Arguments"] = fixed_path_cmd[1:]
        job_descr_dict["Environment"] = {
            "AIRFLOW__DATABASE__SQL_ALCHEMY_CONN" : "postgresql+psycopg2://airflow:airflow@postgres/airflow",
            "AIRFLOW__CORE__DAGS_FOLDER" : "./",
            "AIRFLOW__CORE__EXECUTOR" : "CeleryExecutor,airflow_unicore_integration.executors.unicore_executor.UnicoreExecutor"
        }
        dag_import = {
            "To" :   "dag.py",
            "Data" : dag_content
        }
        job_descr_dict["Imports"] = [dag_import]

        # steps to do for the job:
        # ob needs to give some way to keep track of airflow command - may need to be containerized
        # load all dags into the job directory in the proepr path
        # install airflow as dependency (or prepare venv or something?)
        # install other dependencies - may need to be defined at task level
        
        return job_descr_dict
    
    def execute_async(self, key: TaskInstanceKey, command: List[str], queue: str | None = None, executor_config: Any | None = None) -> None:
        # only put task in submission queue, actually submit to unicore when sync is called by the heartbeat
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
        self.active_jobs[key] = job
        #return super().execute_async(key, command, queue, executor_config)
    
    def get_task_log(self, ti: TaskInstance, try_number: int) -> tuple[list[str], list[str]]:
        job = self.active_jobs.get(ti.key, None)
        if not job:
            return None
        # return unicore task logs TODO maybe include stdout and stderr as they may contain airflow task run details
        return (job.properties["log"], [])
    

