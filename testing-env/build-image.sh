#! /bin/bash

PROJECT_DIR=$(dirname $0)/..

docker build -t unicore-airflow -f testing-env/unicore-airflow.docker $PROJECT_DIR
