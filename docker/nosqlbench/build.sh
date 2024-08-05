#!/bin/bash
# -*- coding: utf-8 -*-

print_usage() {
  echo ""
  echo "Usage: build.sh [OPTION] [ARG]"
  echo "-v|--version ARG NOSQLBENCH version, default: 5.21.2-release"
  echo "-d|--scylla-java-driver-version ARG scylla-java-driver version, default: 4.18.0.1"
  echo "-g|--nosqlbench-git ARG nosqlbench git repo, default: https://github.com/scylladb/nosqlbench.git"
  echo "-t|--tag ARG docker image name, default: scylladb/hydra-loaders:nosqlbench-{version-from-the-argument}"
  echo "-p|--push push image to container registry"
  echo "-h|--help print this message and exit"
  echo "-------------------------------------------------------"
  echo "Example: building nosqlbench with 5.21.2-release and scylla-java-driver 4.18.0.1, a custom git fork, and a custom docker image name"
  echo "./build.sh -v 5.21.2-release -d 4.18.0.1 -g https://github.com/CUSTOM-FORK/nosqlbench.git -i nosqlbench:custom"
  echo ""
}


OPTS=$(getopt -o v:d:g:t:hp --long version:,scylla-java-driver-version:,nosqlbench-git:,tag:,help,push -- "$@")

if [ $? != 0 ]; then
    echo "Failed to parse options." >&2
    exit 1
fi

eval set -- "$OPTS"

while true; do
    case "$1" in
        -v | --version )
            VERSION="$2"
            shift 2;;
        -d | --scylla-java-driver-version )
            SCYLLA_JAVA_DRIVER_VERSION="$2"
            shift 2;;
        -g | --nosqlbench-git )
            NOSQLBENCH_GIT="$2"
            shift 2;;
        -t | --tag )
            IMAGE_NAME="$2"
            shift 2;;
        -p | --push )
            DOCKER_HUB_PUSH="true"
            shift ;;
        -h | --help )
            print_usage
            exit 0;;
        * )
            break;;
    esac
done


if [[ -z "$VERSION" ]]; then
    VERSION="5.21.2-release"
fi

if [[ -z "$SCYLLA_JAVA_DRIVER_VERSION" ]]; then
    SCYLLA_JAVA_DRIVER_VERSION="4.18.0.1"
fi

if [[ -z "$NOSQLBENCH_GIT" ]]; then
    NOSQLBENCH_GIT="https://github.com/nosqlbench/nosqlbench.git"
fi

if [[ -z "$IMAGE_NAME" ]]; then
    IMAGE_NAME="scylladb/hydra-loaders:nosqlbench-$VERSION"
fi

if [[ -z "$DOCKER_HUB_PUSH" ]]; then
    docker build \
        -t "$IMAGE_NAME" \
        --build-arg "GIT_TAG=$VERSION" \
        --build-arg "GIT_URL=$NOSQLBENCH_GIT" \
        --build-arg "SCYLLADB_JAVA_DRIVER=$SCYLLA_JAVA_DRIVER_VERSION" \
        --target production \
        --compress \
        . || exit 1

    echo "Done. Image $IMAGE_NAME ready to be pushed to Container Registry."
else
    docker build \
        -t "$IMAGE_NAME" \
        --build-arg "GIT_TAG=$VERSION" \
        --build-arg "GIT_URL=$NOSQLBENCH_GIT" \
        --build-arg "SCYLLADB_JAVA_DRIVER=$SCYLLA_JAVA_DRIVER_VERSION" \
        --target production \
        --compress \
        --push \
        . || exit 1
fi
