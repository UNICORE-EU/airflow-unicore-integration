def get_provider_info():
    return {
        "package-name": "airflow-unicore-integration",
        "name": "Unicore",
        "description": "Apache Airflow Unicore provider containing Operators and hooks.",
        "cli": [],
        "connection-types": [
            {
                "connection-type": "unicore",
                "hook-class-name": "airflow_unicore_integration.hooks.unicore_hooks.UnicoreHook",
            }
        ],
        "executors": [
            "airflow_unicore_integration.executors.unicore_executor.UnicoreExecutor",
        ],
        "task-decorators": [
            {
                "name": "mpi",
                "class-name": "airflow_unicore_integration.operators.mpi.mpi_task",
            }
        ],
    }
