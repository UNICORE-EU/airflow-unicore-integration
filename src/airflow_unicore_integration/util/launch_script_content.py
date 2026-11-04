"""Content of the worker launch script, read from its single source of truth.

The script is staged into the Unicore job directory as ``run_task_via_supervisor.py``
by the job description generators, so its text has to be available as a string. It is
kept as a normal module file (lintable, type-checkable, reviewable) and read from here,
instead of being duplicated as a string literal, which used to drift silently.
"""

from importlib.resources import files

LAUNCH_SCRIPT_CONTENT_STR = (
    files("airflow_unicore_integration.executors")
    .joinpath("run_task_via_supervisor.py")
    .read_text(encoding="utf-8")
)
