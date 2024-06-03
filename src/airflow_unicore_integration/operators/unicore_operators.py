from airflow.models.baseoperator import BaseOperator
from airflow.decorators.base import DecoratedOperator, task_decorator_factory
from typing import Any, List, Dict

from airflow.utils.context import Context

import pyunicore.client as uc_client
import pyunicore.credentials as uc_credentials
from airflow_unicore_integration.hooks import unicore_hooks

import logging

logger = logging.getLogger(__name__)

DEFAULT_SCRIPT_NAME = 'default_script_from_job_description'
DEFAULT_BSS_FILE = 'default_bss_file_upload'

class JobDescriptionException(BaseException):
    def __init__(self, *args: object) -> None:
        super().__init__(*args)

class UnicoreGenericOperator(BaseOperator):

    def __init__(self, name: str, application_name : str | None = None, application_version: str | None = None, executable: str | None = None, arguments: List[str] | None = None, 
                 environment: List[str] | None = None, parameters: Dict[str,str | List[str]] | None = None, stdout: str | None = None, stderr: str | None = None, stdin: str | None = None, ignore_non_zero_exit_code: bool | None = None, 
                 user_pre_command: str | None = None, run_user_pre_command_on_login_node: bool | None = None, user_pre_command_ignore_non_zero_exit_code: bool | None = None, user_post_command: str | None = None, 
                 run_user_post_command_on_login_node: bool | None = None, user_post_command_ignore_non_zero_exit_code: bool | None = None, resources: Dict[str, str] | None = None, project: str | None = None, 
                 imports: List[Dict[str,str | List[str]]] | None = None, exports: List[Dict[str,str | List[str]]] | None = None, have_client_stagein: bool | None = None, job_type: str | None = None, 
                 login_node: str | None = None, bss_file: str | None = None, tags: List[str] | None = None, notification: str | None = None, user_email: str | None = None, xcom_output_files: List[str] = ["stdout", "stderr"], 
                 base_url: str | None = None, credential: uc_credentials.Credential | None = None, credential_username: str | None = None, credential_password: str | None = None, credential_token: str | None = None, **kwargs):
        """
        Initialize a Unicore Job Operator. 
        :param name: The name parameter defines both the airflow task name and the unicore job name. 
        :param base_url:
        :param credential:
        :param credential_username:
        :param credential_password:
        :param credential_token:

        All other parameters are parameters for the Unicore Job Description as defined [here](https://unicore-docs.readthedocs.io/en/latest/user-docs/rest-api/job-description/index.html#overview).
        """
        super().__init__(**kwargs)
        self.name = name
        self.application_name = application_name
        self.application_version = application_version
        self.executable = executable
        self.arguments = arguments
        self.environment = environment
        self.parameters = parameters
        self.stdout = stdout
        self.stderr = stderr
        self.stdin = stdin
        self.ignore_non_zero_exit_code = ignore_non_zero_exit_code
        self.user_pre_command = user_pre_command
        self.run_user_pre_command_on_login_node = run_user_pre_command_on_login_node
        self.user_pre_command_ignore_non_zero_exit_code = user_pre_command_ignore_non_zero_exit_code
        self.user_post_command = user_post_command
        self.run_user_post_command_on_login_node = run_user_post_command_on_login_node
        self.user_post_command_ignore_non_zero_exit_code = user_post_command_ignore_non_zero_exit_code
        self.resources = resources
        self.project = project
        self.imports = imports
        self.exports = exports
        self.have_client_stagein = have_client_stagein
        self.job_type = job_type
        self.login_node = login_node
        self.bss_file = bss_file
        self.tags = tags
        self.notification = notification
        self.user_email = user_email
        self.xcom_output_files = xcom_output_files

        self.base_url = base_url
        self.credential = credential
        self.credential_username = credential_username
        self.credential_password = credential_password
        self.credential_token = credential_token

        self.validate_job_description()
        logger.debug("created Unicore Job Task")

    def validate_job_description(self):
        # check for some errors in the parameters for creating the unicore job 

        # first check if application or executable been set
        if self.application_name is None and self.executable is None: # TODO check if executable is required if bss_file is given
            raise JobDescriptionException
        
        # if bss_file is set, we need an executable
        if self.bss_file is not None:
            if self.executable is None and self.application_name is not None:
                raise JobDescriptionException
            # TODO validate BSS file?
            logger.info("using bss file")
            
        if self.credential_token is not None and self.credential is None:
            logger.info("using provided oidc token")
            self.credential = uc_credentials.OIDCToken(token=self.credential_token)

        if self.credential_username is not None and self.credential_password is not None and self.credential is None:
            logger.info("using provied user/pass credentials")
            self.credential = uc_credentials.UsernamePassword(username=self.credential_username, password=self.credential_password)
        
    
    def get_job_description(self) -> dict[str,Any]:
        job_description_dict: Dict = {}

        # now add the various simple string attribute fragments to the list, when they are not None
        if self.name is not None:
            job_description_dict["Name"] = self.name
        
        if self.application_name is not None:
            job_description_dict["ApplicationName"] = self.application_name
        
        if self.application_version is not None:
            job_description_dict["ApplicationVersion"] = self.application_version
        
        if self.executable is not None:
            job_description_dict["Executable"] = self.executable

        if self.arguments is not None:
            job_description_dict["Arguments"] = self.arguments
        
        if self.environment is not None:
            job_description_dict["Environment"] = self.environment

        if self.parameters is not None:
            job_description_dict["Parameters"] = self.parameters
        
        if self.stdout is not None:
            job_description_dict["Stdout"] = self.stdout

        if self.stderr is not None:
            job_description_dict["Stderr"] = self.stderr

        if self.stdin is not None:
            job_description_dict["Stdin"] = self.stdin

        if self.ignore_non_zero_exit_code is not None:
            job_description_dict["IgnoreNonZeroExitCode"] = self.ignore_non_zero_exit_code
        
        if self.user_pre_command is not None:
            job_description_dict["User precommand"] = self.user_pre_command

        if self.run_user_pre_command_on_login_node is not None:
            job_description_dict["RunUserPrecommandOnLoginNode"] = self.run_user_pre_command_on_login_node
        
        if self.user_pre_command_ignore_non_zero_exit_code is not None:
            job_description_dict["UserPrecommandIgnoreNonZeroExitCode"] = self.user_pre_command_ignore_non_zero_exit_code

        if self.user_post_command is not None:
            job_description_dict["User postcommand"] = self.user_post_command

        if self.run_user_post_command_on_login_node is not None:
            job_description_dict["RunUserPostcommandOnLoginNode"] = self.run_user_post_command_on_login_node

        if self.user_post_command_ignore_non_zero_exit_code is not None:
            job_description_dict["UserPostcommandIgnoreNonZeroExitCode"] = self.user_post_command_ignore_non_zero_exit_code

        if self.resources is not None:
            job_description_dict["Resources"] = self.resources

        if self.project is not None:
            job_description_dict["Project"] = self.project

        if self.imports is not None:
            job_description_dict["Imports"] = self.imports
        
        if self.exports is not None:
            job_description_dict["Exports"] = self.exports

        if self.have_client_stagein is not None:
            job_description_dict["haveClientStageIn"] =self.have_client_stagein

        if self.job_type is not None:
            job_description_dict["Job type"] = self.job_type

        if self.login_node is not None:
            job_description_dict["Login node"] = self.login_node

        if self.bss_file is not None:
            job_description_dict["BSS file"] = self.bss_file

        if self.notification is not None:
            job_description_dict["Notification"] = self.notification

        if self.user_email is not None:
            job_description_dict["User email"] = self.user_email

        if self.tags is not None:
            job_description_dict["Tags"] = self.tags

        return job_description_dict
        
    def get_uc_client(self, uc_conn_id: str | None = None) -> uc_client.Client:
        if self.base_url is not None and self.credential is not None:
            return uc_client.Client(self.credential, self.base_url)
        if uc_conn_id is None:
            hook = unicore_hooks.UnicoreHook()
        else:
            hook = unicore_hooks.UnicoreHook(uc_conn_id=uc_conn_id)
        return hook.get_conn(overwrite_base_url=self.base_url, overwrite_credential=self.credential)
    
    def execute_async(self, context: Context) -> Any:
        client = self.get_uc_client()
        job = client.new_job(job_description=self.get_job_description(), inputs=[])
        return job

    def execute(self, context: Context) -> Any:
        import logging        
        from pyunicore.client import JobStatus, Job
        logger = logging.getLogger(__name__)
        
        job: Job = self.execute_async(context) # TODO depending on params this may spawn multiple jobs -> in those cases, e.g. output needs to be handled differently
        logger.debug(f"Waiting for unicore job {job}")
        job.poll() # wait for job to finish

        task_instance = context['task_instance']

        
        task_instance.xcom_push(key="status_message", value=job.properties["statusMessage"])
        task_instance.xcom_push(key="log", value=job.properties["log"])
        for line in job.properties["log"]:
            logger.info(f"UNICORE LOGS: {line}")
        
        if job.status is not JobStatus.SUCCESSFUL:
            from airflow.exceptions import AirflowFailException
            logger.error(f"Unicore job not successful. Job state is {job.status}. Aborting this task.")
            raise AirflowFailException


        work_dir = job.working_dir

        content = work_dir.contents()['content']
        task_instance.xcom_push(key="workdir_content", value=content)

        for filename in content.keys():
            if "/UNICORE_Job_" in filename:
                task_instance.xcom_push(key="Unicore Job ID", value=filename[13:])
                jobt_text = work_dir.stat(filename).raw().read().decode("utf-8")
                task_instance.xcom_push(key="UNICORE Job", value=jobt_text)
                continue
            if "bss_submit_" in filename:
                bss_submit_text = work_dir.stat(filename).raw().read().decode("utf-8")
                task_instance.xcom_push(key="BSS_SUBMIT", value=bss_submit_text)
                continue
        
        from requests.exceptions import HTTPError
        for file in self.xcom_output_files:
            try:
                logger.debug(f"Retreiving file {file} from workdir.")
                remote_file = work_dir.stat(file)
                content = remote_file.raw().read().decode("utf-8")
                task_instance.xcom_push(key=file,value=content)
            except HTTPError as http_error:
                logger.error(f"Error while retreiving file {file} from workdir.", http_error)
                continue
            except UnicodeDecodeError as unicore_error:
                logger.error(f"Error while decoding file {file}.", unicore_error)

        exit_code = job.properties["exitCode"]
        return exit_code

class UnicoreScriptOperator(UnicoreGenericOperator):
    def __init__(self, name: str, script_content: str, **kwargs):
        super().__init__(name=name, executable=DEFAULT_SCRIPT_NAME, **kwargs)
        lines = script_content.split('\n')
        script_stagein = {
            "To":   DEFAULT_SCRIPT_NAME,
            "Data": lines
            }
        if self.imports is not None:
            self.imports.append(script_stagein)
        else:
            self.imports = [script_stagein]

class UnicoreBSSOperator(UnicoreGenericOperator):
    def __init__(self, name: str, bss_file_content: str, executable: str, **kwargs):
        super().__init__(name=name, bss_file=DEFAULT_BSS_FILE, executable=executable, job_type="raw", **kwargs)
        lines = bss_file_content.split('\n')
        bss_stagein = {
            "To":   DEFAULT_BSS_FILE,
            "Data": lines
            }
        if self.imports is not None:
            self.imports.append(bss_stagein)
        else:
            self.imports = [bss_stagein]

class UnicoreExecutableOperator(UnicoreGenericOperator):
    def __init__(self, name: str, executable: str, output_files : List[str] = ["stdout"], **kwargs) -> None:
        super().__init__(name=name, executable=executable, xcom_output_files=output_files, **kwargs)

class UnicoreDateOperator(UnicoreExecutableOperator):
    def __init__(self, name: str, **kwargs) -> None:
        super().__init__(name=name, executable='date',**kwargs)

