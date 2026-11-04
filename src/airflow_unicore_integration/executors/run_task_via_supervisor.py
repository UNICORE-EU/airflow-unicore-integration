"""
Usage:

python run_task_via_supervisor.py [--json-string <workload string> | --json-path <workload filepath>]

"""

# get some debug info in case anything fails before the imports
import sys

print(sys.executable)

import argparse

import structlog
from airflow.configuration import conf
from airflow.executors import workloads
from airflow.sdk.execution_time.supervisor import supervise
from pydantic import TypeAdapter

log = structlog.get_logger(logger_name=__name__)


def execute_workload_locally(workload: workloads.All):
    if not isinstance(workload, workloads.ExecuteTask):
        raise TypeError(f"Executor does not know how to handle {type(workload)}")

    base_url = conf.get("api", "base_url", fallback="/")
    default_execution_api_server = f"{base_url.rstrip('/')}/execution/"
    server = conf.get("core", "execution_api_server_url", fallback=default_execution_api_server)
    log.info(f"Connecting to server:{server}")

    log.debug(f"Workload is: {workload}")
    log.debug(f"Dag Bundle is: {workload.bundle_info}")

    supervise(
        # This is the "wrong" ti type, but it duck types the same. TODO: Create a protocol for this.
        ti=workload.ti,  # type: ignore[arg-type]
        dag_rel_path=workload.dag_rel_path,
        bundle_info=workload.bundle_info,
        token=workload.token,
        server=server,
        log_path=workload.log_path,
        # Include the output of the task to stdout too, so that in process logs can be read from via
        # unicore as job logs.
        subprocess_logs_to_stdout=True,
    )


def main():
    parser = argparse.ArgumentParser(
        description="Execute a workload in a Containerised executor using the task SDK."
    )

    # Create a mutually exclusive group to ensure that only one of the flags is set
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument(
        "--json-path",
        help="Path to the input JSON file containing the execution workload payload.",
        type=str,
    )
    group.add_argument(
        "--json-string",
        help="The JSON string itself containing the execution workload payload.",
        type=str,
    )
    args = parser.parse_args()

    decoder = TypeAdapter[workloads.All](workloads.All)

    if args.json_path:
        try:
            with open(args.json_path) as file:
                input_data = file.read()
                workload = decoder.validate_json(input_data)
        except Exception as e:  # noqa: BLE001 -- the worker must report any failure and exit
            log.error("Failed to read file", error=str(e))
            sys.exit(1)

    elif args.json_string:
        try:
            workload = decoder.validate_json(args.json_string)
        except Exception as e:  # noqa: BLE001 -- the worker must report any failure and exit
            log.error("Failed to parse input JSON string", error=str(e))
            sys.exit(1)

    execute_workload_locally(workload)


if __name__ == "__main__":
    main()
