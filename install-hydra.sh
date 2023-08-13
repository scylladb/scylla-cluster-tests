#!/usr/bin/env bash
HYDRA_LINK_PATH=/usr/local/bin/hydra
if ! docker --version ; then
    echo "Docker not installed!!!"
    curl -fsSL get.docker.com --retry 5 --retry-max-time 300  -o get-docker.sh
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
ln -vsf $(pwd)/docker/env/hydra.sh ${HYDRA_LINK_PATH}
echo "Hydra installed."
if ! aws --version; then
    echo "Installing AWS CLI..."
    yum install -y epel-release
    yum install -y python-devel python-pip
    pip install --upgrade pip
    grep -v '^#' requirements-python.txt| grep awscli |  xargs -t -L 1 pip install
fi
echo "AWS CLI installed."
echo "==================================     NOTES      ================================================="
echo "To check that Hydra is installed, run 'hydra bash' anywhere in bash."
echo "It will create a SCT Docker container and connect to its bash."
echo "When running Hydra for the first time it will build the SCT Docker image. Please be patient!"
echo "==================================================================================================="
echo "Please run 'aws configure' to configure AWS CLI"
