import base64
import json
import os
import signal
import subprocess
import sys
import threading
from typing import TYPE_CHECKING
from typing import Callable
from typing import Sequence

import cloudpickle
from airflow.sdk import BaseOperator
from airflow.sdk.bases.decorator import DecoratedOperator
from airflow.sdk.bases.decorator import task_decorator_factory
from airflow.sdk.definitions.context import Context
from airflow.sdk.exceptions import AirflowException

if TYPE_CHECKING:
    from airflow.sdk.bases.decorator import TaskDecorator


RESULT_SENTINEL = "MPIRESULT:"
ENTRYPOINT_NAME = "airflow_unicore_integration.util.mpi_entrypoint"


class MPIOperator(BaseOperator):
    def __init__(
        self,
        name: str,
        python_callable: Callable,
        num_processes: int,
        mpi_executable: str = "srun",
        extra_mpi_args: list[str] | None = None,
        func_args: Sequence | None = None,
        func_kwargs: dict | None = None,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.name = name
        self.python_callable = python_callable
        self.num_processes = num_processes
        self.mpi_executable = mpi_executable
        self.extra_mpi_args = extra_mpi_args or []
        self.func_args = func_args or []
        self.func_kwargs = func_kwargs or {}

    def execute(self, context: Context):
        num_processes = int(context.get("params", {}).get("mpi_num_processes", self.num_processes))
        cmd = self._build_command(
            num_processes,
            self.python_callable,
            json.dumps(self.func_kwargs) if self.func_kwargs else "null",
        )
        self.log.info("Launching MPI job: %s", " ".join(cmd))
        return self._run(cmd)

    def _build_command(self, num_processes, python_callable, kwargs_json):
        cmd = [self.mpi_executable]
        if job_id := os.environ.get("SLURM_JOB_ID"):
            cmd += ["--jobid", job_id]
        cmd += ["--ntasks", str(num_processes)]
        cmd += self.extra_mpi_args
        cmd += [
            "python",
            "-m",
            ENTRYPOINT_NAME,
            self._serialize_callable(python_callable),
            kwargs_json,
        ]
        return cmd

    @staticmethod
    def _serialize_callable(python_callable: Callable) -> str:
        module_name = python_callable.__module__
        module = sys.modules.get(module_name)

        if module is not None:
            cloudpickle.register_pickle_by_value(module)
        try:
            payload = cloudpickle.dumps(python_callable)
        finally:
            if module is not None:
                cloudpickle.unregister_pickle_by_value(module)

        return base64.b64encode(payload).decode("ascii")

    def _run(self, cmd: list[str]) -> object:
        proc = subprocess.Popen(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            preexec_fn=os.setsid,
        )

        def _forward_sigterm(signum, frame):
            os.killpg(os.getpgid(proc.pid), signal.SIGTERM)

        signal.signal(signal.SIGTERM, _forward_sigterm)

        stdout = proc.stdout
        stderr = proc.stderr
        assert stdout is not None
        assert stderr is not None

        def _drain_stderr():
            for line in stderr:
                self.log.info(line.rstrip())

        stderr_thread = threading.Thread(target=_drain_stderr, daemon=True)
        stderr_thread.start()

        result_encoded = None
        for line in stdout:
            line = line.rstrip()
            if line.startswith(RESULT_SENTINEL):
                result_encoded = line[len(RESULT_SENTINEL) :]
            else:
                self.log.info(line)

        stderr_thread.join()
        rc = proc.wait()

        if rc != 0:
            raise AirflowException(
                f"MPI job failed with exit code {rc}. " f"Check logs above for per-rank errors."
            )

        if result_encoded is None:
            raise AirflowException(
                "MPI job completed successfully but rank 0 produced no result. "
                "Ensure the function returns a value on rank 0."
            )

        return json.loads(result_encoded)


class MPIContainerOperator(MPIOperator):

    DEFAULT_BIND_OPTIONS = "/p:/p,/dev/shm:/dev/shm,/cvmfs:/cvmfs"

    def __init__(
        self,
        name: str,
        container_image: str,
        num_processes: int,
        python_callable: Callable | None = None,
        container_cmd: str | None = None,
        mpi_executable: str = "srun",
        extra_mpi_args: list[str] | None = None,
        func_args: Sequence | None = None,
        func_kwargs: dict | None = None,
        apptainer_options: str = f"--nv --sharens --home `mktemp -d` --bind {DEFAULT_BIND_OPTIONS}",
        **kwargs,
    ) -> None:

        if python_callable is None:
            if container_cmd is None:
                raise ValueError("No command or callable provided for this Operator to execute.")

            def f():
                pass

            python_callable = f
        super().__init__(
            name,
            python_callable,
            num_processes,
            mpi_executable,
            extra_mpi_args,
            func_args,
            func_kwargs,
            **kwargs,
        )
        self.container_image = container_image
        self.container_cmd = container_cmd
        self.apptainer_options = apptainer_options

    def _build_command(self, num_processes, python_callable, kwargs_json):
        cmd = [self.mpi_executable]
        if job_id := os.environ.get("SLURM_JOB_ID"):
            cmd += ["--jobid", job_id]
        cmd += ["--ntasks", str(num_processes)]
        cmd += self.extra_mpi_args
        cmd += ["apptainer", "exec", self.apptainer_options, self.container_image]
        if self.container_cmd is None:
            self.container_cmd = [
                "python",
                "-m",
                ENTRYPOINT_NAME,
                self._serialize_callable(python_callable),
                kwargs_json,
            ]
        cmd += self.container_cmd
        return cmd


class MPIDecoratedOperator(MPIOperator, DecoratedOperator):
    custom_operator_name = "@task.mpi"

    def __init__(
        self,
        name: str,
        python_callable: Callable,
        num_processes: int,
        mpi_executable: str = "srun",
        extra_mpi_args: list[str] | None = None,
        func_args: Sequence | None = None,
        func_kwargs: dict | None = None,
        **kwargs,
    ) -> None:
        super().__init__(
            name,
            python_callable,
            num_processes,
            mpi_executable,
            extra_mpi_args,
            func_args,
            func_kwargs,
            **kwargs,
        )


def mpi_task(
    python_callable: Callable | None = None, multiple_outputs: bool | None = None, **kwargs
) -> "TaskDecorator":
    return task_decorator_factory(
        python_callable=python_callable,
        multiple_outputs=multiple_outputs,
        decorated_operator_class=MPIDecoratedOperator,
        **kwargs,
    )
