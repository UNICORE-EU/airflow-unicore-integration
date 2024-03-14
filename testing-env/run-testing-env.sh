#! /bin/bash

COMPOSE_FILE=$(dirname $0)/docker-compose.yml
PROJECT_DIR=$(dirname $0)/../project-dir

mkdir -p $PROJECT_DIR/config
mkdir -p $PROJECT_DIR/logs
mkdir -p $PROJECT_DIR/plugins

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
  *)
    docker compose -f $COMPOSE_FILE --project-directory $PROJECT_DIR --profile flower up -d
    ;;
esac
