def get_provider_info():
    return {
        "package-name": "airflow-unicore-integration",
        "name": "Unicore",
        "description": "Apache Airflow Unicore provider containing Operators and hooks.",
        "cli": ["airflow_unicore_integration.cli.definition.get_cli_commands"],
        "connection-types": [
            {
                "connection-type": "unicore",
                "hook-class-name": "airflow_unicore_integration.hooks.unicore_hooks.UnicoreHook",
            }
        ],
        "executors": [
            "airflow_unicore_integration.executors.unicore_executor.UnicoreExecutor",
        ],
    }
