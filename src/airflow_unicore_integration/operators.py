from airflow.models.baseoperator import BaseOperator
from typing import List
import pyunicore.client as uc_client
import pyunicore.credentials as uc_credentials


class UnicoreGenericOperator(BaseOperator):
    def __init__(self, name: str, **kwargs):
        super().__init__(**kwargs)
        self.name = name

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

