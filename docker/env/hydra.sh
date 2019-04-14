#!/usr/bin/env bash
set -e
CMD=$@
DOCKER_ENV_DIR=$(dirname $(readlink -f $0 ))
SCT_DIR=$(dirname $(dirname ${DOCKER_ENV_DIR}))
VERSION=$(cat ${DOCKER_ENV_DIR}/version)
PY_PREREQS_FILE=requirements-python.txt
CENTOS_PREREQS_FILE=install-prereqs.sh
WORK_DIR=/sct
HOST_NAME=SCT-CONTAINER

# if running on Build server
if [[ ${USER} == "jenkins" ]]; then
    echo "Running on Build Server..."
    HOST_NAME=`hostname`
else
    TTY_STDIN="-it"
    TERM_SET_SIZE="export COLUMNS=`tput cols`; export LINES=`tput lines`;"
fi

if ! docker --version; then
    echo "Docker not installed!!! Please run 'install-hydra.sh'!"
    exit 1
fi

if docker images |grep scylladb/hydra.*${VERSION}; then
    echo "Image up-to-date"
else
    echo "Image with version $VERSION not found. Building..."
    sleep 7
    cd ${DOCKER_ENV_DIR}
    cp -f ${SCT_DIR}/${PY_PREREQS_FILE} .
    cp -f ${SCT_DIR}/${CENTOS_PREREQS_FILE} .
    docker build -t scylladb/hydra:v${VERSION} .
    rm -f ${PY_PREREQS_FILE} ${CENTOS_PREREQS_FILE}
    cd -
fi
# Check for SSH keys
${SCT_DIR}/get-qa-ssh-keys.sh

# TODO: remove once avocado will be gone
# change ownership of avocado directories
echo "Making sure the ownerships of avocado directories are of the user"
sudo chown -R `whoami`:`whoami` ~/avocado &> /dev/null || true
sudo chown -R `whoami`:`whoami` ${SCT_DIR}/test-results &> /dev/null || true

subcommand="$1"
if [[ ${subcommand} == 'run' ]];  then
    echo "run"
    shift
    CMD="avocado --show test run $@"
elif [[ ${subcommand} == 'bash'* ]] || [[ ${subcommand} == 'avocado'* ]] || [[ ${subcommand} == 'python'* ]]; then
    echo "running  ${subcommand}"
else
    CMD="./sct.py $@"
fi

# export all SCT_* env vars into the docker run
SCT_OPTIONS=$(env | grep SCT_ | cut -d "=" -f 1 | xargs -i echo "--env {}")

# export all BUILD_* env vars into the docker run
BUILD_OPTIONS=$(env | grep BUILD_ | cut -d "=" -f 1 | xargs -i echo "--env {}")

# export all AWS_* env vars into the docker run
AWS_OPTIONS=$(env | grep AWS_ | cut -d "=" -f 1 | xargs -i echo "--env {}")

docker run --rm ${TTY_STDIN} --privileged \
    -h ${HOST_NAME} \
    -v /var/run:/run \
    -v ${SCT_DIR}:${WORK_DIR} \
    -v /sys/fs/cgroup:/sys/fs/cgroup:ro \
    -v /tmp:/tmp \
    -v /var/tmp:/var/tmp \
    -v ${HOME}:${HOME} \
    -v /etc/passwd:/etc/passwd:ro \
    -v /etc/group:/etc/group:ro \
    -w ${WORK_DIR} \
    -e JOB_NAME=${JOB_NAME} \
    -e BUILD_URL=${BUILD_URL} \
    -u $(id -u ${USER}):$(id -g ${USER}) \
    ${SCT_OPTIONS} \
    ${BUILD_OPTIONS} \
    ${AWS_OPTIONS} \
    --net=host \
    scylladb/hydra:v${VERSION} \
    /bin/bash -c "${TERM_SET_SIZE} eval ${CMD}"
