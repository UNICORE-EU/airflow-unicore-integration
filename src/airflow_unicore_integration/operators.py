from airflow.models.baseoperator import BaseOperator
from typing import Any, List, Dict

from airflow.utils.context import Context

class JobDescriptionException(BaseException):
    def __init__(self, *args: object) -> None:
        super().__init__(*args)

class UnicoreGenericOperator(BaseOperator):

    def __init__(self, name: str, application_name : str = None, application_version: str = None, executable: str = None, arguments: List[str | Dict[str, int | float | str | List[str]]] = None, 
                 environment: List[str] = None, parameters: Dict[str,str | List[str]] = None, stdout: str = None, stderr: str = None, stdin: str = None, ignore_non_zero_exit_code: bool = None, 
                 user_pre_command: str = None, run_user_pre_command_on_login_node: bool = None, user_pre_command_ignore_non_zero_exit_code: bool = None, user_post_command: str = None, 
                 run_user_post_command_on_login_node: bool = None, user_post_command_ignore_non_zero_exit_code: bool = None, resources: Dict[str, str] = None, project: str = None, 
                 imports: List[Dict[str,str | List[str]]] = None, exports: List[Dict[str,str | List[str]]] = None, have_client_stagein: bool = None, job_type: str = None, 
                 login_node: str = None, bss_file: str = None, tags: List[str] = None, notification: str = None, user_email: str = None, xcom_output_files: List[str] = ["stdout, stderr"], **kwargs):
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

        self.validate_job_description()

    def validate_job_description(self):
        # check for some errors in the parameters for creating the unicore job 

        # first check if application or executable have been set
        if not self.application_name and not self.executable:
            raise JobDescriptionException
        
    
    def get_job_description(self) -> str:
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
            job_description_dict["haveClientStageIn"] =s elf.have_client_stagein

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
        
    def get_uc_client(self):
        import pyunicore.client as uc_client
        import pyunicore.credentials as uc_credentials

        # run date with demouser on hardcoded unicore url
        base_url = "https://unicore:8080/DEMO-SITE/rest/core" # get this from airflow config and task attributes
        credential = uc_credentials.UsernamePassword("demouser", "test123") # get this from user session or configured service account
        client = uc_client.Client(credential, base_url)
        return client
    
    def execute_async(self, context: Context) -> Any:
        client = self.get_uc_client()
        job = client.new_job(job_description=self.get_job_description(), inputs=[])
        return job

    def execute(self, context: Context) -> Any:
        import logging        
        from pyunicore.client import JobStatus, Job
        logger = logging.getLogger(__name__)
        job: Job = self.execute_async(context)
        job.poll() # wait for job to finish

        task_instance = context['task_instance']


        if job.status is not JobStatus.SUCCESSFUL:
            from airflow.exceptions import AirflowFailException
            task_instance.xcom_push(key="status_message", value=job.properties["statusMessage"])
            task_instance.xcom_push(key="log", value=job.properties["log"])
            logger.error(f"Unicore job not successful. Job state is {job.status}. Aborting this task.")
            raise AirflowFailException


        work_dir = job.working_dir

        content = work_dir.contents()['content']
        task_instance.xcom_push(key="workdir_content", value=content)

        for filename in content.keys():
            if "/UNICORE_Job_" in filename:
                task_instance.xcom_push(key="Unicore Job ID", value=filename[13:])
                break
        
        from requests.exceptions import HTTPError
        for file in self.xcom_output_files:
            try:
                logger.debug(f"Retreiving file {file} from workdir.")
                remote_file = work_dir.stat(file)
                content = remote_file.raw().read().decode("utf-8")
                task_instance.xcom_push(key=file,value=content)
            except HTTPError:
                logger.error(f"Error while retreiving file {file} from workdir.")
                continue

        exit_code = job.properties["exitCode"]
        return exit_code


class UnicoreExecutableOperator(BaseOperator):
    def __init__(self, name: str, executable: str, output_files : List[str] = list(), **kwargs) -> None:
        super().__init__(**kwargs)
        self.name = name
        self.executable = executable
        self.output_files = output_files

    def execute(self, context):
        import pyunicore.client as uc_client
        import pyunicore.credentials as uc_credentials

        # run date with demouser on hardcoded unicore url
        base_url = "https://unicore:8080/DEMO-SITE/rest/core" # get this from airflow config and task attributes
        credential = uc_credentials.UsernamePassword("demouser", "test123") # get this from user session or configured service account
        client = uc_client.Client(credential, base_url)
        my_job = {'Executable': self.executable}

        job = client.new_job(job_description=my_job, inputs=[])

        job.poll() # wait for job to finish

        work_dir = job.working_dir

        task_instance = context['task_instance']

        content = work_dir.contents()['content']
        task_instance.xcom_push(key="workdir_content", value=content)

        for filename in content.keys():
            if "/UNICORE_Job_" in filename:
                task_instance.xcom_push(key="Unicore Job ID", value=filename[13:])
                break


        for of_file in self.output_files:
            content = work_dir.stat(f"/{of_file}").raw().read().decode("utf-8")
            task_instance.xcom_push(key=of_file, value=content)

        stdout = work_dir.stat("/stdout")
        content = stdout.raw().read()
        task_instance.xcom_push(key="stdout", value=content.decode("utf-8"))

        exit_code = work_dir.stat("/UNICORE_SCRIPT_EXIT_CODE")
        exit_code = exit_code.raw().read()
        return exit_code.decode("utf-8")

class UnicoreDateOperator(UnicoreExecutableOperator):
    def __init__(self, name: str, **kwargs) -> None:
        super().__init__(name=name, executable='date',**kwargs)

