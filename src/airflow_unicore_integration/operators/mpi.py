import json
import os
import signal
import subprocess
import threading
from typing import Callable
from typing import Sequence

from airflow.sdk import BaseOperator
from airflow.sdk.definitions.context import Context
from airflow.sdk.exceptions import AirflowException

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
        op_args: Sequence | None = None,
        op_kwargs: dict | None = None,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.name = name
        self._validate_callable(python_callable)
        self.python_callable = python_callable
        self.num_processes = num_processes
        self.mpi_executable = mpi_executable
        self.extra_mpi_args = extra_mpi_args or []
        self.op_args = op_args or []
        self.op_kwargs = op_kwargs or {}

    @staticmethod
    def _validate_callable(func: Callable):
        """Ensure the function is importable — required since ranks
        get it via module path, not serialization."""
        module = getattr(func, "__module__", None)
        name = getattr(func, "__qualname__", "")
        if module is None or module == "__main__":
            raise AirflowException(
                f"@task.mpi requires an importable function. "
                f"'{func.__name__}' is defined in __main__ or is not "
                f"importable. Move it to a proper module on PYTHONPATH."
            )
        if "<locals>" in name or "<lambda>" in name:
            raise AirflowException(
                f"@task.mpi does not support local functions or lambdas. "
                f"'{name}' must be a module-level function."
            )

    def execute(self, context: Context):
        num_processes = int(context.get("params", {}).get("mpi_num_processes", self.num_processes))
        cmd = self._build_command(
            num_processes,
            self.python_callable.__module__,
            self.python_callable.__name__,
            json.dumps(self.op_kwargs) if self.op_kwargs else "null",
        )
        self.log.info("Launching MPI job: %s", " ".join(cmd))
        return self._run(cmd)

    def _build_command(self, num_processes, module_path, func_name, kwargs_json):
        cmd = [self.mpi_executable]
        if job_id := os.environ.get("SLURM_JOB_ID"):
            cmd += ["--jobid", job_id]
        cmd += ["--ntasks", str(num_processes)]
        cmd += self.extra_mpi_args
        cmd += ["python", "-m", ENTRYPOINT_NAME, module_path, func_name, kwargs_json]
        return cmd

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
