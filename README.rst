===========================
Unicore Airflow Integration
===========================


|Generic badge|

.. |Generic badge| image:: https://github.com/UNICORE-EU/airflow-unicore-integration/actions/workflows/publish-to-pypi.yml/badge.svg 
   :target: https://github.com/UNICORE-EU/airflow-unicore-integration/actions/workflows/publish-to-pypi.yml

This project integrates `UNICORE <https://github.com/UNICORE-EU>`_ and `Apache Airflow <https://airflow.apache.org/>`_.
UNICORE is a software suite that, among other functions, provides seamless access to high-performance compute and data resources.
Airflow is a platform to programmatically author, schedule and monitor workflows.

In the current state, this projects provides a set of airflow `operators <https://airflow.apache.org/docs/apache-airflow/stable/core-concepts/operators.html>`_, which can be used as part of airflow workflows to submit jobs to Unicore.
The UnicoreExecutor only offers experimental support for airflow 3 so far. Further support is currently being worked on.

---------------------------
Using the Unicore Operators
---------------------------

There are multiple Unicore operators provided by this package. The most versatile one is the ``UnicoreGenericOperator``, which supports a lot of job parameters.
All other operators are intended to offer a slightly less complex constructor, and therefore simpler usage, but all generic parameters are still available to be used.

All operators support all possible parameters of the `Unicore job description <https://unicore-docs.readthedocs.io/en/latest/user-docs/rest-api/job-description/index.html#overview>`_. Here is an excerpt containing some commonly used parameters:

======================= ======================= =========================================== ====================
parameter name          type                    default                                     description
======================= ======================= =========================================== ====================
application_name        str                     None                                        Application Name
application_version     str                     None                                        Application Version
executable              str                     None                                        Command line executable
arguments               List(str)               None                                        Command line arguments
environment             Map(str,str)            None                                        environment arguments
parameters              Map                     None                                        Application Parameters
project                 str                     None                                        Accounting Project
imports                 List(imports)           None                                        Stage-in/data import - see Unicore docs
exports                 List(exports)           None                                        Stage-out/data export - see Unicore docs
======================= ======================= =========================================== ====================

For imports and exports go `here <https://unicore-docs.readthedocs.io/en/latest/user-docs/rest-api/job-description/index.html#importing-files-into-the-job-workspace>`_ for details.


The ``UnicoreGenericOperator`` supports the following additional parameters:

======================= ======================= =========================================== ====================
parameter name          type                    default                                     description
======================= ======================= =========================================== ====================
name                    str                     None                                        name for the airflow task and the Unicore job
xcom_output_files       List(str)               ["stdout","stderr"]                         list of files of which the content should be put into xcoms
base_url                str                     configured in airflow connections or None   The base URL of the UNICOREX server to be used for the Unicore client
credential              pyunicore credential    configured in airflow connections or None   A Unicore Credential to be used for the Unicore client
credential_username     str                     configured in airflow connections or None   Username for the Unicore client credentials
credential_password     str                     configured in airflow connections or None   Password the the Unicore client credentials
credential_token        str                     configured in airflow connections or None   An OIDC token to be used by the Unicore client
======================= ======================= =========================================== ====================


The ``UnicoreScriptOperator`` offers a way to more easily submit a script as a job, where the script content can be provided as a string.

======================= ======================= =========================================== ====================
parameter name          type                    default                                     description
======================= ======================= =========================================== ====================
script_content          str                     None                                        The content of the script file
======================= ======================= =========================================== ====================


The ``UnicoreBSSOperator`` offers a way to directly submit batch-scripts from their content-strings.

======================= ======================= =========================================== ====================
parameter name          type                    default                                     description
======================= ======================= =========================================== ====================
bss_file_content        str                     None                                        The content of the batch script file
======================= ======================= =========================================== ====================


The ``UnicoreExecutableOperator`` offers a reduced constructor that only requires an executable.

======================= ======================= =========================================== ====================
parameter name          type                    default                                     description
======================= ======================= =========================================== ====================
executable              str                     None                                        The executable to run for this job
xcom_output_files       List(str)               ["stdout","stderr"]                         list of files of which the content should be put into xcoms
======================= ======================= =========================================== ====================

The ``UnicoreDateOperator`` is more of a testing operator, since it will only run the ``date`` executable.

-------------------------------
Behaviour on Errors and Success
-------------------------------

The Unicore Operators do not do a lot of error and exception handling, and mostly just forward any problems to be handled by airflow.
All of the Unicore logic is handled by the `pyunicore library <https://github.com/HumanBrainProject/pyunicore>`_.

While some validation of the resulting Unicore job description is done automatically, it may still be possible to build an invalid job description with the operators.
This may lead to a submission failure with Unicore. In this case, an exception is thrown to be handled by airflow.


For a successful job submission, the job exit code is returned as the task return value, so that airflow can handle non-zero exit codes.
All operators will also append the content of the job-log-file from Unicore to the airflow task log.
Also, some job results and values will be uploaded via airflow-x-coms as well:

======================= ========================================
xcom name               description
======================= ========================================
Unicore Job ID          the Unicore ID for the job
Unicore Job             the TSI script that was submitted by Unicore
BSS_SUBMIT              the bss_script submitted by Unicore
status_message          the status message for the Unicore job
log                     the Unicore job log
workdir_content         content of the job workdir upon completion
[xcom_output_files]     content of each file in their own xcom, by default stdout and stderr
======================= ========================================

------------
Example DAGs
------------

There are some example DAGs in this repository under ``project-dir/dags``.

- ``unicore-test-1.py`` just shows basic date and executable usage.
- ``unicore-test-2.py`` has some basic examples for the generic operator.
- ``unicore-test-3.py`` also includes script-operator examples.
- ``unicore-test-4.py`` has some examples with more arguments.
- ``unicore-test-bss.py`` shows how bss submission can be done (very simple example).
- ``unicore-test-credentials.py`` demonstrates that not only the credentials from the airflow connections backend can be used, but they can also be provided in the constructor of the operator.
- ``unicore-test-import-export.py`` gives short examples for the imports and exports usage.


-----------------
Setup testing env
-----------------

Ensure a current version of docker is installed.

Run ``python3 -m build`` to build the python package.

Run the ``testing-env/build-image.sh`` script to create the customized airflow image, which will contain the newly build python package.

Run ``testing-env/run-testing-env.sh init`` to initialize the airflow containers, database etc. This only needs to be done once.

Run ``testing-env/run-testing-env.sh up`` to start the local airflow and Unicore deployment. Airflow will be available on port 8080, Unicore on port 8081.

The ``run-testing-env.sh`` script supports the commands up, down, start, stop, ps and init for matching docker compose functions.

-----------------------
Install package via pip
-----------------------

``pip install airflow-unicore-integration``
