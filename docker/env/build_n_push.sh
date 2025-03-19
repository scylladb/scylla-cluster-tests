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
    cd "${DOCKER_ENV_DIR}"
    REQUIREMENTS_IN=$(realpath --relative-to=${DOCKER_ENV_DIR} ${SCT_DIR}/requirements.in)
    uv pip compile $REQUIREMENTS_IN --generate-hashes --python-version=$(cat ${SCT_DIR}/.python-version) > ${SCT_DIR}/${PY_PREREQS_FILE}
    sed 's|\.\./\.\./requirements.in|requirements.in|' -i  ${SCT_DIR}/requirements.txt
    cp -f ${SCT_DIR}/${PY_PREREQS_FILE} .
    docker build --network=host -t scylladb/hydra:${VERSION} .
    rm -f ${PY_PREREQS_FILE}
    cd -
    docker login
    echo "Tagging and pushing..."
    docker push scylladb/hydra:${VERSION}
    echo "Done."
fi
