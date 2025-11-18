import json
import logging
from typing import Any
from typing import Dict

from airflow.configuration import conf
from airflow.executors.workloads import ExecuteTask
from airflow.models.taskinstancekey import TaskInstanceKey
from airflow.providers.git.hooks.git import GitHook

from .launch_script_content import LAUNCH_SCRIPT_CONTENT_STR

logger = logging.getLogger(__name__)


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
    EXECUTOR_CONFIG_POST_COMMANDS = "postcommands"  # gets added to the unicore job descirption
    EXECUTOR_CONFIG_JOB_TYPE = "job_type"
    EXECUTOR_CONFIG_UNICORE_CONN_KEY = (
        "unicore_connection_id"  # alternative connection id for the Unicore connection to use
    )
    EXECUTOR_CONFIG_UNICORE_SITE_KEY = "unicore_site"  # alternative Unicore site to run at, only required if different than connection default
    EXECUTOR_CONFIG_UNICORE_CREDENTIAL_KEY = "unicore_credential"  # alternative unicore credential to use for the job, only required if different than connection default

    def create_job_description(self, workload: ExecuteTask) -> Dict[str, Any]:
        raise NotImplementedError()

    def get_job_name(self, key: TaskInstanceKey) -> str:
        return f"{key.dag_id} - {key.task_id} - {key.run_id} - {key.try_number}"


class NaiveJobDescriptionGenerator(JobDescriptionGenerator):
    """
    This class generates a naive unicore job, that expects there to be a working python env containing airflow and any other required dependencies on the executing system.
    """

    GIT_DAG_BUNDLE_CLASSPATH = "airflow.providers.git.bundles.git.GitDagBundle"

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
        user_added_post_commands: list[str] = executor_config.get(JobDescriptionGenerator.EXECUTOR_CONFIG_POST_COMMANDS, [])  # type: ignore
        user_defined_job_type: str = executor_config.get(JobDescriptionGenerator.EXECUTOR_CONFIG_JOB_TYPE, None)  # type: ignore
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
        logger.debug(f"Server is {server}")

        # set job type
        if user_defined_job_type:
            job_descr_dict["Job type"] = user_defined_job_type

        # check which python virtualenv to use
        if user_defined_python_env:
            python_env = user_defined_python_env
        else:
            python_env = conf.get("unicore.executor", "DEFAULT_ENV")
        tmp_dir = conf.get("unicore.executor", "TMP_DIR", "/tmp")
        # prepare dag file to be uploaded via unicore
        # dag_file = open("/tmp/test")
        # dag_content = dag_file.readlines()
        # dag_import = {"To": dag_rel_path, "Data": dag_content}
        worker_script_import = {
            "To": "run_task_via_supervisor.py",
            # "From": "https://gist.githubusercontent.com/cboettcher/3f1101a1d1b67e7944d17c02ecd69930/raw/1d90bf38199d8c0adf47a79c8840c3e3ddf57462/run_task_via_supervisor.py",
            "Data": LAUNCH_SCRIPT_CONTENT_STR,
        }
        # start filling the actual job description
        job_descr_dict["Name"] = self.get_job_name(key)
        job_descr_dict["Executable"] = (
            f". airflow_config.env && . {python_env} && python run_task_via_supervisor.py --json-string '{workload.model_dump_json()}'"  # TODO may require module load to be setup for some systems
        )
        # job_descr_dict["Arguments"] = [
        #    "-c",
        #    "source airflow_config.env",
        #    "source {python_env}/bin/activate",
        #    "python",
        #    "run_task_via_supervisor.py",
        #    f"--json-string '{workload.model_dump_json()}'",
        # ]

        job_descr_dict["Environment"] = {
            "AIRFLOW__CORE__EXECUTION_API_SERVER_URL": server,
            # "AIRFLOW__CORE__DAGS_FOLDER": "./",
            "AIRFLOW__LOGGING__LOGGING_LEVEL": "DEBUG",
            "AIRFLOW__CORE__EXECUTOR": "LocalExecutor,airflow_unicore_integration.executors.unicore_executor.UnicoreExecutor",
        }

        # build filecontent string for importing in the job | this is needed to avoid confusing nested quotes and trying to escape them properly when using unicore env vars directly
        env_file_content: list[str] = []

        # transmit needed dag bundle information (and possibly files) to job directory
        bundle_str = conf.get("dag.processor", "dag_bundle_config_list")
        logger.debug(f"Dag Bundle config is: {bundle_str}")
        bundle_dict = json.loads(bundle_str)
        conn_id_to_transmit = None
        bundle_type = None

        for bundle in bundle_dict:
            if bundle["name"] == workload.bundle_info.name:
                if bundle["classpath"] == NaiveJobDescriptionGenerator.GIT_DAG_BUNDLE_CLASSPATH:
                    bundle_type = NaiveJobDescriptionGenerator.GIT_DAG_BUNDLE_CLASSPATH
                    env_file_content.append(
                        f"export AIRFLOW__DAG_PROCESSOR__DAG_BUNDLE_CONFIG_LIST='[{json.dumps(bundle)}]'"
                    )
                    conn_id_to_transmit = bundle["kwargs"]["git_conn_id"]
                    break
                # TODO handle other bundle types

        if bundle_type:
            if (
                bundle_type == NaiveJobDescriptionGenerator.GIT_DAG_BUNDLE_CLASSPATH
                and conn_id_to_transmit
            ):
                git_hook = GitHook(conn_id_to_transmit)
                git_remote_url = git_hook.repo_url
                git_dir_prefix = f"{tmp_dir}/{workload.ti.dag_id}/{workload.ti.task_id}/{workload.ti.run_id}/{workload.ti.try_number}"
                git_local_url = f"{git_dir_prefix}/dagmirror"
                dag_bundle_path = f"{git_dir_prefix}/dagbundle"
                # add precommand to clone repo on login node
                git_precommand = f". {python_env} && mkdir -p {git_local_url} && mkdir -p {dag_bundle_path} && git clone {git_remote_url} {git_local_url}"
                job_descr_dict["Environment"][
                    "AIRFLOW__DAG_PROCESSOR__DAG_BUNDLE_STORAGE_PATH"
                ] = f"{dag_bundle_path}"
                logger.info(f"git precommand is {git_precommand}")
                user_added_pre_commands.append(git_precommand)
                # add connection to local clone to env of job
                airflow_conn_string = json.dumps(
                    {"conn_type": "git", "host": f"file://{git_local_url}"}
                )
                env_file_content.append(
                    f"export AIRFLOW_CONN_{str(conn_id_to_transmit).upper()}='{airflow_conn_string}'"
                )
                logger.info(f"connection is '{airflow_conn_string}'")
                # add cleanup of local git repo to job description
                git_cleanup_command = f"rm -r {git_dir_prefix}"
                logger.info(f"git cleanup is {git_cleanup_command}")
                user_added_post_commands.append(git_cleanup_command)

        airflow_env_import = {"To": "airflow_config.env", "Data": env_file_content}

        job_descr_dict["Imports"] = [worker_script_import, airflow_env_import]

        if len(user_added_pre_commands) > 0:
            precommand_import = {"To": "precommand.sh", "Data": user_added_pre_commands}
            job_descr_dict["Imports"].append(precommand_import)
            job_descr_dict["User precommand"] = "bash precommand.sh"
        if len(user_added_post_commands) > 0:
            postcommand_import = {"To": "postcommand.sh", "Data": user_added_post_commands}
            job_descr_dict["Imports"].append(postcommand_import)
            job_descr_dict["User postcommand"] = "bash postcommand.sh"

        job_descr_dict["RunUserPrecommandOnLoginNode"] = (
            "true"  # precommand needs public internet access to clone dag repos
        )
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
