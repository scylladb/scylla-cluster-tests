#!/usr/bin/env bash

if yum --help; then
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

  # Install gettext - needed for k8s-* backends
  yum install -y gettext

elif apt --help; then
  apt-get install -y --no-install-recommends build-essential cmake libssl-dev zlib1g-dev libffi-dev uuid-runtime

else
  echo "Unsupported distro"
  exit 1
fi

# Install kubectl needed for k8s-* backends

curl -o /tmp/kubectl -L "https://dl.k8s.io/release/$(curl -L -s https://dl.k8s.io/release/stable.txt)/bin/linux/amd64/kubectl"
install -o root -g root -m 0755 /tmp/kubectl /usr/local/bin/kubectl

# Install docker
if ! docker --version ; then
    curl -fsSL get.docker.com --retry 5 --retry-max-time 300 -o get-docker.sh
    sh get-docker.sh
    groupadd docker || true
    usermod -aG docker $USER || true
fi

# Configure Docker to use Google Container Registry mirrors
mkdir -p /etc/docker
cat > /etc/docker/daemon.json <<EOF
{
  "registry-mirrors": [
    "https://mirror.gcr.io"
  ]
}
EOF
systemctl restart docker || true


# Make sdcm available in python path
if [ "$1" == "docker" ]; then
    ln -s /sct/sdcm /usr/lib/python2.7/site-packages/sdcm
else
    pip install -r requirements-python.txt
    pre-commit install
    pre-commit install --hook-type commit-msg

    echo "========================================================="
    echo "Please run 'aws configure' to configure AWS CLI and then"
    echo "========================================================="
fi
