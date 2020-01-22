#!/usr/bin/env bash
yum install -y epel-release
yum -y update
yum install -y wget unzip

# Python dependencies
yum install -y python-devel python-pip
pip install --upgrade pip

# Needed for PhantomJS
yum install -y freetype-devel libpng-devel bzip2 bitmap-fonts fontconfig

# Needed for Cassandra Python driver
yum install -y gcc

# Install OpenSSH client - needed to ssh to DB servers/ Loaders/ monitors
yum install -y openssh-clients
# Install Git - needed to get current SCT branch
yum install -y git

# Install OpenSSL - needed to update encryption key
yum install -y openssl
yum install -y iproute
yum install -y which

# Install Docker
yum install -y sudo  # needed for Docker container build
curl -fsSL get.docker.com -o get-docker.sh
sh get-docker.sh
groupadd docker || true
usermod -aG docker $USER || true

# Make sdcm available in python path due to avocado runner bug
if [ "$1" == "docker" ]; then
    ln -s /sct/sdcm /usr/lib/python2.7/site-packages/sdcm
else
    pip install -r requirements-python.txt
    pre-commit install
    pre-commit install --hook-type commit-msg

    ln -s `pwd`/sdcm $(python -c "import site; print site.getsitepackages()[0]")/sdcm
    echo "========================================================="
    echo "Please run 'aws configure' to configure AWS CLI and then"
    echo "run 'get-qa-ssh-keys.sh' to retrieve AWS QA private keys"
    echo "========================================================="
fi
