#!/usr/bin/env bash
set -e
CMD=$@
DOCKER_ENV_DIR=$(dirname $(readlink $0 ))
SCT_DIR=$(dirname $(dirname ${DOCKER_ENV_DIR}))
VERSION=$(cat ${DOCKER_ENV_DIR}/version)
PY_PREREQS_FILE=requirements-python.txt
CENTOS_PREREQS_FILE=install-prereqs.sh
WORK_DIR=/sct

if ! docker --version; then
    echo "Docker not installed!!! Please run 'install-hydra.sh' in the root of SCT repo!"
    exit 1
fi

if docker images |grep scylladb/hydra.*${VERSION}; then
    echo "Image up-to-date"
else
    echo "Image with version $VERSION not found. Building..."
    cd ${DOCKER_ENV_DIR}
    cp -f ${SCT_DIR}/${PY_PREREQS_FILE} .
    cp -f ${SCT_DIR}/${CENTOS_PREREQS_FILE} .
    docker build -t scylladb/hydra:v${VERSION} .
    rm -f ${PY_PREREQS_FILE} ${CENTOS_PREREQS_FILE}
    cd -
fi

docker run --rm -it --privileged \
    -v /var/run:/run \
    -v ${SCT_DIR}:${WORK_DIR} \
    -v /sys/fs/cgroup:/sys/fs/cgroup:ro \
    -v /tmp:/tmp \
    -v ~:/root \
    -w ${WORK_DIR} \
    scylladb/hydra:v${VERSION} \
    /bin/bash -c "export COLUMNS=`tput cols`; export LINES=`tput lines`; eval $CMD"