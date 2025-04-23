#! /bin/bash

COMPOSE_FILE=$(dirname $0)/docker-compose.yml
PROJECT_DIR=$(dirname $0)/../project-dir


case "$1" in
  "stop")
    docker compose -f $COMPOSE_FILE --project-directory $PROJECT_DIR --profile flower stop
    ;;
  "ps")
    docker compose -f $COMPOSE_FILE --project-directory $PROJECT_DIR --profile flower ps
    ;;
  "down")
    docker compose -f $COMPOSE_FILE --project-directory $PROJECT_DIR --profile flower down
    ;;
  "start")
    docker compose -f $COMPOSE_FILE --project-directory $PROJECT_DIR --profile flower start
    ;;
  "restart")
    docker compose -f $COMPOSE_FILE --project-directory $PROJECT_DIR --profile flower stop
    docker compose -f $COMPOSE_FILE --project-directory $PROJECT_DIR --profile flower start
    ;;
  "logs")
    docker compose -f $COMPOSE_FILE --project-directory $PROJECT_DIR --profile flower logs
    ;;
  "init")
    mkdir -p $PROJECT_DIR/config
    mkdir -p $PROJECT_DIR/logs
    mkdir -p $PROJECT_DIR/plugins
    echo -e "AIRFLOW_UID=$(id -u)" > $PROJECT_DIR/.env
    docker compose -f $COMPOSE_FILE --project-directory $PROJECT_DIR --profile flower up airflow-init
    ;;
  *)
    docker compose -f $COMPOSE_FILE --project-directory $PROJECT_DIR --profile flower up -d
    ;;
esac
