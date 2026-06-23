import json
import logging
from typing import Any
from typing import Dict

from airflow.executors.workloads import ExecuteTask
from airflow.models.taskinstancekey import TaskInstanceKey
from airflow.providers.common.compat.sdk import conf as global_conf
from airflow.providers.git.hooks.git import GitHook
from airflow.sdk.configuration import AirflowSDKConfigParser

from .launch_script_content import LAUNCH_SCRIPT_CONTENT_STR

logger = logging.getLogger(__name__)


class JobDescriptionGenerator:
    """
    A generator class for generating unicore jhob descriptions that may supprot different kinds of systems and/ or environments.
    """

    EXECUTOR_CONFIG_PYTHON_ENV_KEY = "python_env"  # full path to a python virtualenv that includes airflow and all required libraries for the task (without the .../bin/activate part)
    EXECUTOR_CONFIG_IMAGE_URL_KEY = "image"  # url to a container image that can be used by the container runtime on the target system
    EXECUTOR_CONFIG_IMAGE_TYPE_KEY = (
        "image_type"  # type of the container image. defaults to "docker"
    )
    EXECUTOR_CONFIG_RESOURCES = "Resources"  # gets added to the unicore job description
    EXECUTOR_CONFIG_ENVIRONMENT = "Environment"  # gets added to the unicore job description
    EXECUTOR_CONFIG_PARAMETERS = "Parameters"  # gets added to the unicore job description
    EXECUTOR_CONFIG_PROJECT = "Project"  # gets added to the unicore job description
    EXECUTOR_CONFIG_PRE_COMMANDS = "precommands"  # gets added to the unicore job description
    EXECUTOR_CONFIG_POST_COMMANDS = "postcommands"  # gets added to the unicore job descirption
    EXECUTOR_CONFIG_JOB_TYPE = "job_type"
    EXECUTOR_CONFIG_LOGIN_NODE = "login_node"
    EXECUTOR_CONFIG_JOB_DESCRIPTION_PARAMS = "custom_job_description_additions"
    EXECUTOR_CONFIG_UNICORE_CONN_KEY = (
        "unicore_connection_id"  # alternative connection id for the Unicore connection to use
    )
    EXECUTOR_CONFIG_UNICORE_SITE_KEY = "unicore_site"  # alternative Unicore site to run at, only required if different than connection default
    EXECUTOR_CONFIG_UNICORE_CREDENTIAL_KEY = "unicore_credential"  # alternative unicore credential to use for the job, only required if different than connection default
    EXECUTOR_CONFIG_UNICORE_PRECONFIGURED_SITE_KEY = "site"  # name of the preconfigured site to use , only requried if different from the default site

    CONF_SECTION = "unicore.executor"

    AIRFLOW_CONFIG_DEFAULT_ENV_KEY = "DEFAULT_ENV"
    AIRFLOW_CONFIG_DEFAULT_IMAGE_KEY = "DEFAULT_IMAGE"
    AIRFLOW_CONFIG_DEFAULT_IMAGE_DEFAULT_VALUE = "apache-airflow"
    AIRFLOW_CONFIG_DEFAULT_IMAGE_TYPE_KEY = "DEFAULT_IMAGE_TYPE"
    AIRFLOW_CONFIG_TMP_DIR_KEY = "TMP_DIR"
    AIRFLOW_CONFIG_CONTAINER_BINDS_KEY = "container_extra_binds"
    AIRFLOW_CONFIG_CONTAINER_HOME_KEY = "default_home"
    AIRFLOW_CONFIG_CONTAINER_BINDS_DEFAULT = "/p:/p,/dev/shm:/dev/shm,/cvmfs:/cvmfs"

    def __init__(self, conf: AirflowSDKConfigParser) -> None:
        self.conf = conf
        self.job_descr: Dict[str, Any] = {}
        self.env_file_content: list[str] = []

    def create_job_description(self, workload: ExecuteTask) -> Dict[str, Any]:
        raise NotImplementedError()

    def get_job_name(self, key: TaskInstanceKey) -> str:
        return f"{key.dag_id} - {key.task_id} - {key.run_id} - {key.try_number}"

    def set_job_name(self, key: TaskInstanceKey) -> None:
        self.job_descr["Name"] = self.get_job_name(key)

    def get_site(self, executor_config) -> list[str]:
        # get site specific options
        overwrite_preconfigured_site = executor_config.get(  # type: ignore
            JobDescriptionGenerator.EXECUTOR_CONFIG_UNICORE_PRECONFIGURED_SITE_KEY, None
        )  # task can provide a site to run at, else use first one from config

        preconfigured_sites: list[list[str]] = json.loads(
            self.conf.get(JobDescriptionGenerator.CONF_SECTION, "SITES_CONFIG", "")
        )

        site = preconfigured_sites[0]
        if overwrite_preconfigured_site is not None:
            for tmp in preconfigured_sites:
                if tmp[0] == overwrite_preconfigured_site:
                    site = tmp
                    break
        return site

    def set_job_type(self, executor_config) -> None:
        user_defined_job_type: str = executor_config.get(JobDescriptionGenerator.EXECUTOR_CONFIG_JOB_TYPE, None)  # type: ignore
        if user_defined_job_type:
            self.job_descr["Job type"] = user_defined_job_type

    def set_login_node(self, executor_config) -> None:
        user_defined_login_node: str = executor_config.get(JobDescriptionGenerator.EXECUTOR_CONFIG_LOGIN_NODE, None)  # type: ignore
        if user_defined_login_node:
            self.job_descr["Login node"] = user_defined_login_node

    def add_to_env_file(self, key: str, value: str) -> None:
        self.env_file_content.append(f"export {key.upper()}={value}")

    def get_env_file_import(self) -> Dict[str, str | list[str]]:
        return {"To": self.get_env_file_name(), "Data": self.env_file_content}

    def get_env_file_name(self) -> str:
        return "airflow_config.env"

    def add_import(self, import_str: Dict[str, Any]):
        if not self.job_descr.get("Imports", None):
            self.job_descr["Imports"] = [import_str]
        else:
            self.job_descr["Imports"].append(import_str)


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

        # get user config from executor_config
        user_added_env: Dict[str, str] = executor_config.get(JobDescriptionGenerator.EXECUTOR_CONFIG_ENVIRONMENT, None)  # type: ignore
        user_added_params: Dict[str, str] = executor_config.get(JobDescriptionGenerator.EXECUTOR_CONFIG_PARAMETERS, None)  # type: ignore
        user_added_project: str = executor_config.get(JobDescriptionGenerator.EXECUTOR_CONFIG_PROJECT, None)  # type: ignore
        user_added_resources: Dict[str, str] = executor_config.get(JobDescriptionGenerator.EXECUTOR_CONFIG_RESOURCES, None)  # type: ignore
        user_added_pre_commands: list[str] = executor_config.get(JobDescriptionGenerator.EXECUTOR_CONFIG_PRE_COMMANDS, [])  # type: ignore
        user_defined_python_env: str = workload.ti.executor_config.get(JobDescriptionGenerator.EXECUTOR_CONFIG_PYTHON_ENV_KEY, None)  # type: ignore
        user_added_post_commands: list[str] = executor_config.get(JobDescriptionGenerator.EXECUTOR_CONFIG_POST_COMMANDS, [])  # type: ignore
        user_added_job_description: Dict[str, Any] = executor_config.get(JobDescriptionGenerator.EXECUTOR_CONFIG_JOB_DESCRIPTION_PARAMS, {})  # type: ignore

        # get local dag path from cmd and fix dag path in arguments
        dag_rel_path = str(workload.dag_rel_path)
        if dag_rel_path.startswith("DAG_FOLDER"):
            dag_rel_path = dag_rel_path[10:]
        base_url = global_conf.get("api", "base_url", fallback="/")
        default_execution_api_server = f"{base_url.rstrip('/')}/execution/"
        server = global_conf.get(
            JobDescriptionGenerator.CONF_SECTION,
            "execution_api_server_url",
            fallback=default_execution_api_server,
        )
        logger.debug(f"Server is {server}")

        site = self.get_site(executor_config=executor_config)
        site_specific_precommand: str = site[3]
        using_proxy: str = site[4]
        if using_proxy == "True":
            logger.info("Using proxy for this task.")
            proxy_url = self.conf.get(
                JobDescriptionGenerator.CONF_SECTION, f"SITES_PROXY_{site[0].upper()}", ""
            )
        else:
            proxy_url = None

        # set job type
        self.set_job_type(executor_config=executor_config)
        self.set_login_node(executor_config=executor_config)
        self.set_job_name(key=key)

        # check which python virtualenv to use
        if user_defined_python_env:
            python_env = user_defined_python_env
        else:
            python_env = self.conf.get(
                JobDescriptionGenerator.CONF_SECTION,
                JobDescriptionGenerator.AIRFLOW_CONFIG_DEFAULT_ENV_KEY,
            )
        tmp_dir = self.conf.get(
            JobDescriptionGenerator.CONF_SECTION,
            JobDescriptionGenerator.AIRFLOW_CONFIG_TMP_DIR_KEY,
            "/tmp",
        )

        worker_script_import = {
            "To": "run_task_via_supervisor.py",
            # "From": "https://gist.githubusercontent.com/cboettcher/3f1101a1d1b67e7944d17c02ecd69930/raw/1d90bf38199d8c0adf47a79c8840c3e3ddf57462/run_task_via_supervisor.py",
            "Data": LAUNCH_SCRIPT_CONTENT_STR,
        }

        self.add_import(worker_script_import)

        self.add_to_env_file("AIRFLOW__CORE__EXECUTION_API_SERVER_URL", server)
        self.add_to_env_file("AIRFLOW__LOGGING__LOGGING_LEVEL", "DEBUG")
        self.add_to_env_file(
            "AIRFLOW__CORE__EXECUTOR",
            "LocalExecutor,airflow_unicore_integration.executors.unicore_executor.UnicoreExecutor",
        )

        # set proxy variables to be used by python requests library
        if proxy_url:
            self.add_to_env_file("HTTPS_PROXY", proxy_url)
            self.add_to_env_file("HTTP_PROXY", proxy_url)

        # set multi-team to true, so that multi team features work on the worker node
        self.add_to_env_file("AIRFLOW__CORE__MULTI_TEAM", "True")

        # transmit needed dag bundle information (and possibly files) to job directory
        bundle_str = global_conf.get("dag.processor", "dag_bundle_config_list")
        logger.debug(f"Dag Bundle config is: {bundle_str}")
        bundle_dict = json.loads(bundle_str)
        conn_id_to_transmit = None
        bundle_type = None

        for bundle in bundle_dict:
            if bundle["name"] == workload.bundle_info.name:
                if bundle["classpath"] == NaiveJobDescriptionGenerator.GIT_DAG_BUNDLE_CLASSPATH:
                    bundle_type = NaiveJobDescriptionGenerator.GIT_DAG_BUNDLE_CLASSPATH
                    self.add_to_env_file(
                        "AIRFLOW__DAG_PROCESSOR__DAG_BUNDLE_CONFIG_LIST", f"[{json.dumps(bundle)}]"
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
                self.add_to_env_file(
                    "AIRFLOW__DAG_PROCESSOR__DAG_BUNDLE_STORAGE_PATH", f"{dag_bundle_path}"
                )
                logger.debug(f"git precommand is {git_precommand}")
                user_added_pre_commands.append(git_precommand)
                # add connection to local clone to env of job
                airflow_conn_string = json.dumps(
                    {"conn_type": "git", "host": f"file://{git_local_url}"}
                )
                self.add_to_env_file(
                    f"AIRFLOW_CONN_{str(conn_id_to_transmit).upper()}", airflow_conn_string
                )
                logger.debug(f"connection is '{airflow_conn_string}'")
                # add cleanup of local git repo to job description
                git_cleanup_command = f"rm -r {git_dir_prefix}"
                logger.debug(f"git cleanup is {git_cleanup_command}")
                user_added_post_commands.append(git_cleanup_command)

        self.add_import(self.get_env_file_import())

        if len(user_added_pre_commands) > 0:
            self.add_import({"To": "precommand.sh", "Data": user_added_pre_commands})
            self.job_descr["User precommand"] = "bash precommand.sh"
        if len(user_added_post_commands) > 0:
            self.add_import({"To": "postcommand.sh", "Data": user_added_post_commands})
            self.job_descr["User postcommand"] = "bash postcommand.sh"

        self.job_descr["RunUserPrecommandOnLoginNode"] = (
            "true"  # precommand needs public internet access to clone dag repos
        )
        # add user defined options to description
        if user_added_env:
            for env_key in user_added_env:
                self.add_to_env_file(env_key, user_added_env[env_key])
        if user_added_params:
            self.job_descr["Parameters"] = user_added_params
        if user_added_project:
            self.job_descr["Project"] = user_added_project
        if user_added_resources:
            self.job_descr["Resources"] = user_added_resources

        # set the executable
        if not site_specific_precommand:
            self.job_descr["Executable"] = (
                f". {self.get_env_file_name()} && . {python_env} && python run_task_via_supervisor.py --json-string '{workload.model_dump_json()}'"
            )
        else:
            logger.info("Using site specific command before task execution.")
            self.job_descr["Executable"] = (
                f". {self.get_env_file_name()} && . {python_env} && {site_specific_precommand} && python run_task_via_supervisor.py --json-string '{workload.model_dump_json()}'"
            )

        # overwrite with values from user added field
        self.job_descr.update(user_added_job_description)

        return self.job_descr


class ContainerJobDescriptionGenerator(JobDescriptionGenerator):
    """
    Generates a job description which will execute the workload in a container based on either the configured default image, or the provided image.
    This will allow a lot fewer user options beyond the image url than the NaiveJobDescriptionGenerator.
    """

    def create_job_description(self, workload: ExecuteTask) -> Dict[str, Any]:
        key: TaskInstanceKey = workload.ti.key
        executor_config = workload.ti.executor_config
        if not executor_config:
            executor_config = {}
        # get user config from executor_config
        user_image: str = executor_config.get(JobDescriptionGenerator.EXECUTOR_CONFIG_IMAGE_URL_KEY, None)  # type: ignore
        user_image_type: str = executor_config.get(JobDescriptionGenerator.EXECUTOR_CONFIG_IMAGE_TYPE_KEY, None)  # type: ignore
        user_added_env: Dict[str, str] = executor_config.get(JobDescriptionGenerator.EXECUTOR_CONFIG_ENVIRONMENT, None)  # type: ignore
        user_added_project: str = executor_config.get(JobDescriptionGenerator.EXECUTOR_CONFIG_PROJECT, None)  # type: ignore
        user_added_resources: Dict[str, str] = executor_config.get(JobDescriptionGenerator.EXECUTOR_CONFIG_RESOURCES, None)  # type: ignore
        precommands: list[str] = []
        postcommands: list[str] = []

        if not user_image:
            user_image = self.conf.get(
                JobDescriptionGenerator.CONF_SECTION,
                JobDescriptionGenerator.AIRFLOW_CONFIG_DEFAULT_IMAGE_KEY,
                JobDescriptionGenerator.AIRFLOW_CONFIG_DEFAULT_IMAGE_DEFAULT_VALUE,
            )
        if not user_image_type:
            user_image_type = self.conf.get(
                JobDescriptionGenerator.CONF_SECTION,
                JobDescriptionGenerator.AIRFLOW_CONFIG_DEFAULT_IMAGE_TYPE_KEY,
                "docker",
            )

        # need a env to potentially run pre and postcommands
        system_env = self.conf.get(
            JobDescriptionGenerator.CONF_SECTION,
            JobDescriptionGenerator.AIRFLOW_CONFIG_DEFAULT_ENV_KEY,
        )

        # get local dag path from cmd and fix dag path in arguments
        dag_rel_path = str(workload.dag_rel_path)
        if dag_rel_path.startswith("DAG_FOLDER"):
            dag_rel_path = dag_rel_path[10:]
        base_url = global_conf.get("api", "base_url", fallback="/")
        default_execution_api_server = f"{base_url.rstrip('/')}/execution/"
        server = global_conf.get(
            JobDescriptionGenerator.CONF_SECTION,
            "execution_api_server_url",
            fallback=default_execution_api_server,
        )
        logger.debug(f"Server is {server}")

        site = self.get_site(executor_config=executor_config)
        site_specific_precommand: str = site[3]
        using_proxy: str = site[4]
        if using_proxy == "True":
            logger.info("Using proxy for this task.")
            proxy_url = self.conf.get(
                JobDescriptionGenerator.CONF_SECTION, f"SITES_PROXY_{site[0].upper()}", ""
            )
        else:
            proxy_url = None

        # set job type
        self.set_job_type(executor_config=executor_config)
        self.set_login_node(executor_config=executor_config)
        self.set_job_name(key=key)

        self.add_to_env_file("AIRFLOW__CORE__EXECUTION_API_SERVER_URL", server)
        self.add_to_env_file("AIRFLOW__LOGGING__LOGGING_LEVEL", "DEBUG")
        self.add_to_env_file(
            "AIRFLOW__CORE__EXECUTOR",
            "LocalExecutor,airflow_unicore_integration.executors.unicore_executor.UnicoreExecutor",
        )

        # set proxy variables to be used by python requests library
        if proxy_url:
            self.add_to_env_file("HTTPS_PROXY", proxy_url)
            self.add_to_env_file("HTTP_PROXY", proxy_url)

        # set multi-team to true, so that multi team features work on the worker node
        self.add_to_env_file("AIRFLOW__CORE__MULTI_TEAM", "True")

        # transmit needed dag bundle information (and possibly files) to job directory
        bundle_str = global_conf.get("dag.processor", "dag_bundle_config_list")
        logger.debug(f"Dag Bundle config is: {bundle_str}")
        bundle_dict = json.loads(bundle_str)
        conn_id_to_transmit = None
        bundle_type = None

        for bundle in bundle_dict:
            if bundle["name"] == workload.bundle_info.name:
                if bundle["classpath"] == NaiveJobDescriptionGenerator.GIT_DAG_BUNDLE_CLASSPATH:
                    bundle_type = NaiveJobDescriptionGenerator.GIT_DAG_BUNDLE_CLASSPATH
                    self.add_to_env_file(
                        "AIRFLOW__DAG_PROCESSOR__DAG_BUNDLE_CONFIG_LIST", f"[{json.dumps(bundle)}]"
                    )
                    conn_id_to_transmit = bundle["kwargs"]["git_conn_id"]
                    break
                # TODO handle other bundle types

        if bundle_type:
            if (
                bundle_type == NaiveJobDescriptionGenerator.GIT_DAG_BUNDLE_CLASSPATH
                and conn_id_to_transmit
            ):
                tmp_dir = self.conf.get(JobDescriptionGenerator.CONF_SECTION, "TMP_DIR", "/tmp")
                git_hook = GitHook(conn_id_to_transmit)
                git_remote_url = git_hook.repo_url
                git_dir_prefix = f"{tmp_dir}/{workload.ti.dag_id}/{workload.ti.task_id}/{workload.ti.run_id}/{workload.ti.try_number}"
                git_local_url = f"{git_dir_prefix}/dagmirror"
                dag_bundle_path = f"{git_dir_prefix}/dagbundle"
                # add precommand to clone repo on login node
                git_precommand = f". {system_env} && mkdir -p {git_local_url} && mkdir -p {dag_bundle_path} && git clone {git_remote_url} {git_local_url}"
                self.add_to_env_file(
                    "AIRFLOW__DAG_PROCESSOR__DAG_BUNDLE_STORAGE_PATH", f"{dag_bundle_path}"
                )
                logger.debug(f"git precommand is {git_precommand}")
                precommands.append(git_precommand)
                # add connection to local clone to env of job
                airflow_conn_string = json.dumps(
                    {"conn_type": "git", "host": f"file://{git_local_url}"}
                )
                self.add_to_env_file(
                    f"AIRFLOW_CONN_{str(conn_id_to_transmit).upper()}", airflow_conn_string
                )
                logger.debug(f"connection is '{airflow_conn_string}'")
                # add cleanup of local git repo to job description
                git_cleanup_command = f"rm -r {git_dir_prefix}"
                logger.debug(f"git cleanup is {git_cleanup_command}")
                postcommands.append(git_cleanup_command)

        self.add_import(self.get_env_file_import())

        if len(precommands) > 0:
            self.add_import({"To": "precommand.sh", "Data": precommands})
        if len(postcommands) > 0:
            self.add_import({"To": "postcommand.sh", "Data": postcommands})

        self.job_descr["RunUserPrecommandOnLoginNode"] = (
            "true"  # precommand needs public internet access to clone dag repos
        )
        # add user defined options to description
        if user_added_env:
            for env_key in user_added_env:
                self.add_to_env_file(env_key, user_added_env[env_key])
        if user_added_project:
            self.job_descr["Project"] = user_added_project
        if user_added_resources:
            self.job_descr["Resources"] = user_added_resources

        # import worker script
        worker_script_import = {
            "To": "run_task_via_supervisor.py",
            # "From": "https://gist.githubusercontent.com/cboettcher/3f1101a1d1b67e7944d17c02ecd69930/raw/1d90bf38199d8c0adf47a79c8840c3e3ddf57462/run_task_via_supervisor.py",
            "Data": LAUNCH_SCRIPT_CONTENT_STR,
        }

        self.add_import(worker_script_import)

        task_cmd = f". {self.get_env_file_name()} && python run_task_via_supervisor.py --json-string '{workload.model_dump_json()}'"

        job_image_name = "job_image.sif"
        bind_options = self.conf.get(
            JobDescriptionGenerator.CONF_SECTION,
            JobDescriptionGenerator.AIRFLOW_CONFIG_CONTAINER_BINDS_KEY,
            JobDescriptionGenerator.AIRFLOW_CONFIG_CONTAINER_BINDS_DEFAULT,
        )
        home = self.conf.get(
            JobDescriptionGenerator.CONF_SECTION,
            JobDescriptionGenerator.AIRFLOW_CONFIG_CONTAINER_HOME_KEY,
            "`mktemp -d`",
        )
        apptainer_cmd = f'. {system_env} && {site_specific_precommand} && apptainer exec --nv --bind {bind_options} --home {home} --sharens {job_image_name} bash -c "{task_cmd}"'
        apptainer_precommand = f"apptainer build {job_image_name} {user_image_type}://{user_image}"

        self.job_descr["User precommand"] = (
            f". {system_env} && bash precommand.sh && {apptainer_precommand}"
        )
        self.job_descr["User postcommand"] = f". {system_env} && bash postcommand.sh"

        worker_script_import = {
            "To": "run_task_via_supervisor.py",
            # "From": "https://gist.githubusercontent.com/cboettcher/3f1101a1d1b67e7944d17c02ecd69930/raw/1d90bf38199d8c0adf47a79c8840c3e3ddf57462/run_task_via_supervisor.py",
            "Data": LAUNCH_SCRIPT_CONTENT_STR,
        }

        # set executable for job
        self.job_descr["Executable"] = apptainer_cmd

        self.add_import(worker_script_import)

        raise NotImplementedError()
