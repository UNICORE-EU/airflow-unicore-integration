===========================
Unicore Airflow Integration
===========================

=================
Setup testing env
=================

Ensure a current version of docker is installed.

Run ``python3 -m build`` to build the python package.
Run the ``testing-env/build-image.sh`` script to create the customized airflow image, which will contain the newly build python package.
Run ``testing-env/run-testing-env.sh init`` to initialize the airflow containers, database etc. This only needs to be done once.
Run ``testing-env/run-testing-env.sh up`` to start the local airflow and UNICORE deployment. Airflow will be available on port 8080, UNICORE on port 8081.

The ``run-testing-env.sh`` script supports the commands up, down, start, stop, ps and init for mathcing docker compose functions.