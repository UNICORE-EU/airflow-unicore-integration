===========================
Unicore Airflow Integration
===========================



Using the Unicore Operators
---------------------------

There are multiple UNICORE operators provided by this package. The most versatile one is the ``UnicoreGenericOperator``, which supports all job parameters of the pyunicore-client [TODO insert URL], as well as some additional parameters described below.
All other operators are intended to offer a slightly less complex constructor, and therefore simpler usage, but all generic parameters are still available to be used.

The ``UnicoreGenericOperator`` supports all possible parameters of the [unicore job description](https://unicore-docs.readthedocs.io/en/latest/user-docs/rest-api/job-description/index.html#overview), as well as the following additional parameters: 

======================= ======================= =========================================== ====================
parameter name          type                    default                                     description
======================= ======================= =========================================== ====================
name                    str                     /                                           name for the airflow task and the unicore job
xcom_output_files       List(str)               ["stdout","stderr"]                         list of files of which the content should be put into xcoms
base_url                str                     configured in airflow connections or None   The base URL of the UNICOREX server to be used for the unicore client
credential              pyunicore credential    configured in airflow connections or None   A unicore Credential to be used for the unicore client
credential_username     str                     configured in airflow connections or None   Username for the unicore client credentials
credential_password     str                     configured in airflow connections or None   Password the the unicore client credentials
credential_token        str                     configured in airflow connections or None   An OIDC token to be used by the unicore client
======================= ======================= =========================================== ====================


The ``UnicoreScriptOperator`` offers a way to more easily submit a script as a job, where the script content can be provided as a string.

======================= ======================= =========================================== ====================
parameter name          type                    default                                     description
======================= ======================= =========================================== ====================
script_content          str                     /                                           The content of the script file
======================= ======================= =========================================== ====================


The ``UnicoreBSSOperator``

======================= ======================= =========================================== ====================
parameter name          type                    default                                     description
======================= ======================= =========================================== ====================
bss_file_content        str                     /                                           The batch script to use for this job.
======================= ======================= =========================================== ====================


The ``UnicoreExecutableOperator`` offers a reduced constructor that only requires an executable.

======================= ======================= =========================================== ====================
parameter name          type                    default                                     description
======================= ======================= =========================================== ====================
executable              str                     /                                           The executable to run for this job
xcom_output_files       List(str)               ["stdout","stderr"]                         list of files of which the content should be put into xcoms
======================= ======================= =========================================== ====================

The ``UnicoreDateOperator`` is more of a testing operator, since it will only run the ``date`` executable.

-------------------------------
Behaviour on Errors and Success
-------------------------------

While some validation of the resulting UNICORE job description is done automatically, it may still be possible to build an invalid job description with the operators. This may lead to a submission failure with unicore. In this case, an exception is thrown to be handled by airflow.
For a successful job submission, the job exit code is returned as the task return value, so that airflow can handle non-zero exit codes.
All operators will also append the content of the job-log-file from unicore to the airflow task log.
Also, some job results and values will be uploaded via airflow-x-coms as well. 

TODO create list 

-----------------
Setup testing env
-----------------

Ensure a current version of docker is installed.

Run ``python3 -m build`` to build the python package.

Run the ``testing-env/build-image.sh`` script to create the customized airflow image, which will contain the newly build python package.

Run ``testing-env/run-testing-env.sh init`` to initialize the airflow containers, database etc. This only needs to be done once.

Run ``testing-env/run-testing-env.sh up`` to start the local airflow and UNICORE deployment. Airflow will be available on port 8080, UNICORE on port 8081.

The ``run-testing-env.sh`` script supports the commands up, down, start, stop, ps and init for matching docker compose functions.

-----------------------
Install package via pip
-----------------------

``pip install airflow-unicore-integration --index-url https://gitlab.jsc.fz-juelich.de/api/v4/projects/6269/packages/pypi/simple``