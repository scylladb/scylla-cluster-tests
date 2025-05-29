#!/usr/bin/env bash
set -e

DOCKER_ENV_DIR=$(dirname $(readlink -f $0 ))
VERSION=v$(cat ${DOCKER_ENV_DIR}/version)

SCT_DIR=$(dirname $(dirname ${DOCKER_ENV_DIR}))
PY_PREREQS_FILE=requirements.txt

function docker_tag_exists() {
    echo "Contacting Docker Hub to check if Hydra image ${VERSION} already exists..."
    curl --silent -f -lSL https://index.docker.io/v1/repositories/"$1"/tags/"$2"  > /dev/null 2>&1
}

if docker_tag_exists scylladb/hydra ${VERSION}; then
    echo "Hydra ${VERSION} already exists in the Docker Hub. Nothing to do."
else
    echo "Hydra ${VERSION} doesn't exist in the Docker Hub!"
    echo "Will build locally and push."
fi

if [[ -n "$(docker images scylladb/hydra:${VERSION} -q)" ]]; then
    echo "Local image exists. Not building."
else
    echo "Hydra image with version $VERSION not found locally. Building..."
    uv lock
    docker build --network=host -t scylladb/hydra:${VERSION} .
    docker login
    echo "Tagging and pushing..."
    docker push scylladb/hydra:${VERSION}
    echo "Done."
fi
