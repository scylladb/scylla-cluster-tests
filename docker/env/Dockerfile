ARG PYTHON_IMAGE_TAG=3.10.0-slim-bullseye

FROM python:$PYTHON_IMAGE_TAG as apt_base
ENV DEBIAN_FRONTEND=noninteractive
RUN apt-get update

# Add 3rd-party APT repositories.
FROM apt_base as apt_repos
RUN apt-get install -y --no-install-recommends apt-transport-https gnupg2 software-properties-common curl
RUN curl -fsSL https://download.docker.com/linux/debian/gpg | apt-key add -
RUN add-apt-repository "deb [arch=amd64] https://download.docker.com/linux/debian $(lsb_release -cs) stable"

# Download, build and install Python packages.
FROM apt_base as python_packages
ENV PIP_NO_CACHE_DIR=1
RUN apt-get install -y --no-install-recommends build-essential cmake libssl-dev zlib1g-dev libffi-dev
ADD requirements.txt .
RUN mkdir /build && \
    pip3 install -r requirements.txt --root=/build --prefix=./ --ignore-installed --no-warn-script-location

FROM python:$PYTHON_IMAGE_TAG
ARG KUBECTL_VERSION=1.23.14
ARG EKSCTL_VERSION=0.124.0
ENV PYTHONWARNINGS=ignore:unclosed \
    PYTHONFAULTHANDLER=1 \
    PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1
COPY --from=apt_repos /etc/apt/trusted.gpg /etc/apt/sources.list /etc/apt/
RUN DEBIAN_FRONTEND=noninteractive apt-get update && \
    apt-get install -y --no-install-recommends \
        binutils \
        curl \
        gettext \
        git \
        iproute2 \
        iptables \
        libnss-myhostname \
        openssh-client \
        rsync \
        sudo \
        unzip \
        wget \
        psmisc \
        procps \
        docker-ce-cli && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*
RUN curl -fsSLo /usr/local/bin/kubectl https://storage.googleapis.com/kubernetes-release/release/v$KUBECTL_VERSION/bin/linux/amd64/kubectl && \
    chmod +x /usr/local/bin/kubectl
RUN curl --silent --location  "https://github.com/weaveworks/eksctl/releases/download/v$EKSCTL_VERSION/eksctl_Linux_amd64.tar.gz" | tar xz -C /tmp && \
    mv /tmp/eksctl /usr/local/bin
COPY --from=python_packages /build /usr/local
