#!/bin/bash

DOCKER_HOST=`ifconfig docker0 | grep -oP '(?<=inet\s)\d+(\.\d+){3}'`
DOCKER_HOST="${DOCKER_HOST:-127.0.0.1}"
POSTGRESQL_PORT=`docker ps | grep "postgres\:.*->5432/tcp" | sed s/"^.*:\([0-9]*\)->5432\/tcp.*$"/"\\1"/`

echo "Connecting to ${DOCKER_HOST}:${POSTGRESQL_PORT}..."

docker run -it --rm -e "PGPASSWORD=test" postgres psql -h "${DOCKER_HOST}" -p "${POSTGRESQL_PORT}" -U test test
