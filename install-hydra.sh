#!/usr/bin/env bash
HYDRA_LINK_PATH=/usr/local/bin/hydra
if ! docker --version > /dev/null; then
    echo "Docker not installed!!!"
    curl -fsSL get.docker.com -o get-docker.sh
    sh get-docker.sh
    # Docker post install, adds ability to run Docker as current user
    groupadd docker || true
    usermod -aG docker ${USER} || true
    echo "==================================================================================================="
    echo "!!!! In order to run Docker as a current user, please logout from your current session !!!!"
    echo "More info: https://docs.docker.com/install/linux/linux-postinstall/#manage-docker-as-a-non-root-user"
    echo "==================================================================================================="
fi
echo "Docker is installed."
if [ ! -L "${HYDRA_LINK_PATH}" ]; then
    echo "Installing  Hydra"
    ln -s $(pwd)/docker/env/hydra.sh ${HYDRA_LINK_PATH}
fi
echo "Hydra installed."
echo "==================================================================================================="
echo "To check that Hydra is installed, run 'hydra ls' anywhere in bash."
echo "It will run 'ls' command in the SCT Docker container."
echo "NOTE: When running Hydra for the first time it will build the SCT Docker image. Please be patient!"
echo "==================================================================================================="