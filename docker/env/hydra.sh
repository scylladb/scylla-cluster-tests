#!/usr/bin/env bash
set -e
CMD=$@
DOCKER_ENV_DIR=$(readlink -f "$0")
DOCKER_ENV_DIR=$(dirname "${DOCKER_ENV_DIR}")
DOCKER_REPO=scylladb/hydra
SCT_DIR=$(dirname "${DOCKER_ENV_DIR}")
SCT_DIR=$(dirname "${SCT_DIR}")
VERSION=v$(cat "${DOCKER_ENV_DIR}/version")
WORK_DIR=/sct
HOST_NAME=SCT-CONTAINER
RUN_BY_USER=$(python3 "${SCT_DIR}/sdcm/utils/get_username.py")
USER_ID=$(id -u "${USER}")
HOME_DIR=${HOME}

export SCT_TEST_ID=${SCT_TEST_ID:-$(uuidgen)}
export GIT_USER_EMAIL=$(git config --get user.email)

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

if [[ ${USER} == "jenkins" || -z "`docker images ${DOCKER_REPO}:${VERSION} -q`" ]]; then
    echo "Pull version $VERSION from Docker Hub..."
    docker pull ${DOCKER_REPO}:${VERSION}
else
    echo "There is ${DOCKER_REPO}:${VERSION} in local cache, use it."
fi

# Check for SSH keys
"${SCT_DIR}/get-qa-ssh-keys.sh"

# change ownership of results directories
echo "Making sure the ownerships of results directories are of the user"
sudo chown -R `whoami`:`whoami` ~/sct-results &> /dev/null || true
sudo chown -R `whoami`:`whoami` "${SCT_DIR}/sct-results" &> /dev/null || true


# export all SCT_* env vars into the docker run
SCT_OPTIONS=$(env | grep SCT_ | cut -d "=" -f 1 | xargs -i echo "--env {}")

# export all BUILD_* env vars into the docker run
BUILD_OPTIONS=$(env | grep BUILD_ | cut -d "=" -f 1 | xargs -i echo "--env {}")

# export all AWS_* env vars into the docker run
AWS_OPTIONS=$(env | grep AWS_ | cut -d "=" -f 1 | xargs -i echo "--env {}")

# export all JENKINS_* env vars into the docker run
JENKINS_OPTIONS=$(env | grep JENKINS_ | cut -d "=" -f 1 | xargs -i echo "--env {}")

group_args=()
for gid in $(id -G); do
    group_args+=(--group-add "$gid")
done

./get-qa-ssh-keys.sh >/dev/null 2>&1

function run_in_docker () {
    CMD_TO_RUN=$1
    REMOTE_DOCKER_HOST=$2
    echo "Going to run '${CMD_TO_RUN}'..."
    docker ${REMOTE_DOCKER_HOST} run --rm ${TTY_STDIN} --privileged \
        -h ${HOST_NAME} \
        -l "TestId=${SCT_TEST_ID}" \
        -l "RunByUser=${RUN_BY_USER}" \
        -v /var/run:/run \
        -v "${SCT_DIR}:${SCT_DIR}" \
        -v /sys/fs/cgroup:/sys/fs/cgroup:ro \
        -v /tmp:/tmp \
        -v /var/tmp:/var/tmp \
        -v "${HOME_DIR}:${HOME_DIR}" \
        -v /etc/passwd:/etc/passwd:ro \
        -v /etc/group:/etc/group:ro \
        -v /etc/sudoers:/etc/sudoers:ro \
        -v /etc/sudoers.d/:/etc/sudoers.d:ro \
        -v /etc/shadow:/etc/shadow:ro \
        -w "${SCT_DIR}" \
        -e JOB_NAME="${JOB_NAME}" \
        -e BUILD_URL="${BUILD_URL}" \
        -e BUILD_NUMBER="${BUILD_NUMBER}" \
        -e _SCT_BASE_DIR="${SCT_DIR}" \
        -e GIT_USER_EMAIL \
        -u ${USER_ID} \
        ${group_args[@]} \
        ${SCT_OPTIONS} \
        ${BUILD_OPTIONS} \
        ${JENKINS_OPTIONS} \
        ${AWS_OPTIONS} \
        --net=host \
        --name="${SCT_TEST_ID}_$(date +%s)" \
        ${DOCKER_REPO}:${VERSION} \
        /bin/bash -c "sudo ln -s '${SCT_DIR}' '${WORK_DIR}'; /sct/get-qa-ssh-keys.sh; ${TERM_SET_SIZE} eval '${CMD_TO_RUN}'"
}


subcommand="$*"
if [[ "$1" == "--execute-on-runner" ]]; then
    SCT_RUNNER_IP="$2"
    echo "SCT Runner IP: $SCT_RUNNER_IP"
    if [[ -n $SCT_RUNNER_IP ]] && [[ $SCT_RUNNER_IP =~ ^[0-9]+\.[0-9]+\.[0-9]+\.[0-9]+$ ]]; then
        eval $(ssh-agent)
        function clean_ssh_agent {
            echo "Cleaning SSH agent"
            eval $(ssh-agent -k)
        }
        trap clean_ssh_agent EXIT
        ssh-add ~/.ssh/scylla-qa-ec2
        ssh-add ~/.ssh/scylla-test
        echo "Going to run a Hydra commands on SCT runner '$SCT_RUNNER_IP'..."
        HOME_DIR="/home/ubuntu"
        echo "Syncing ${SCT_DIR} to the SCT runner instance..."
        ssh-keygen -R "$SCT_RUNNER_IP" || true
        rsync -ar -e "ssh -o StrictHostKeyChecking=no" --delete ${SCT_DIR} ubuntu@${SCT_RUNNER_IP}:/home/ubuntu/
        if [[ -z "$AWS_OPTIONS" ]]; then
          echo "AWS credentials were not passed using AWS_* environment variables!"
          echo "Checking if ~/.aws/credentials exists..."
          if [ -f ~/.aws/credentials ]; then
              echo "AWS credentials file found. Syncing to SCT Runner..."
              rsync -ar -e "ssh -o StrictHostKeyChecking=no" --delete ~/.aws ubuntu@${SCT_RUNNER_IP}:/home/ubuntu/
          else
              echo "Can't run SCT without AWS credentials!"
              exit 1
          fi
        else
            echo "AWS_* environment variables found and will passed to Hydra container."
        fi
        # Only copy GCE credential for GCE backend
        if [[ ${@:3} =~ " gce " && -f ~/.google_libcloud_auth.skilled-adapter-452 ]]; then
           echo "GCE credentials file found. Syncing to SCT Runner..."
           rsync -ar -e "ssh -o StrictHostKeyChecking=no" --delete ~/.google_libcloud_auth.skilled-adapter-452 ubuntu@${SCT_RUNNER_IP}:/home/ubuntu/
        fi

        SCT_DIR="/home/ubuntu/scylla-cluster-tests"
        USER_ID=1000
        group_args=()
        for gid in $(ssh -o StrictHostKeyChecking=no ubuntu@${SCT_RUNNER_IP} id -G); do
           group_args+=(--group-add "$gid")
        done

        DOCKER_HOST="-H ssh://ubuntu@${SCT_RUNNER_IP}"
        CMD=${@:3}
        subcommand=$CMD
    else
      echo "=========================================================================================================="
      echo "Invalid IP provided for '--execute-on-runner'. Run 'hydra create-runner-instance' or check ./sct_runner_ip"
      echo "=========================================================================================================="
      exit 2
    fi

fi
if [[ ${subcommand} == *'bash'* ]] || [[ ${subcommand} == *'python'* ]]; then
    CMD=${subcommand}
else
    CMD="./sct.py ${CMD}"
fi

run_in_docker "${CMD}" "${DOCKER_HOST}"
