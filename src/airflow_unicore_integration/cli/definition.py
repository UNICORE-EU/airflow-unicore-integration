def get_cli_commands():
    """Return an empty list of CLI commands for this custom provider.
    This satisfies Airflow's requirement for a 'cli' entry to avoid loading
    heavy executor modules at CLI startup.
    """
    return []
