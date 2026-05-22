#! /bin/bash

PROJECT_DIR=$(dirname $0)/..

python -m build $PROJECT_DIR

docker build -t unicore-airflow -f $PROJECT_DIR/testing-env/unicore-airflow.docker $PROJECT_DIR

docker build -t unicore-with-deps -f $PROJECT_DIR/testing-env/unicore-with-deps.docker $PROJECT_DIR
