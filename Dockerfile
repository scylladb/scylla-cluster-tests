ARG PYTHON_IMAGE_TAG=3.14.0-slim-trixie

FROM python:$PYTHON_IMAGE_TAG AS apt_base
ENV DEBIAN_FRONTEND=noninteractive
RUN apt-get update

# Add 3rd-party APT repositories.
FROM apt_base AS apt_repos
RUN apt-get install -y --no-install-recommends gnupg2 apt-transport-https curl
RUN install -m 0755 -d /etc/apt/keyrings

RUN curl -fsSL https://download.docker.com/linux/debian/gpg -o /etc/apt/keyrings/docker.asc
RUN chmod a+r /etc/apt/keyrings/docker.asc
RUN echo \
  "deb [arch=$(dpkg --print-architecture) signed-by=/etc/apt/keyrings/docker.asc] https://download.docker.com/linux/debian \
  trixie stable" | tee -a /etc/apt/sources.list.d/docker.list

RUN echo 'deb [signed-by=/usr/share/keyrings/cloud.google.gpg] https://packages.cloud.google.com/apt cloud-sdk main' | tee -a /etc/apt/sources.list.d/google-cloud-sdk.list
RUN curl https://packages.cloud.google.com/apt/doc/apt-key.gpg -o /usr/share/keyrings/cloud.google.gpg
RUN chmod a+r /usr/share/keyrings/cloud.google.gpg

RUN curl -fsSL https://packages.confluent.io/deb/8.0/archive.key | gpg --dearmor > /etc/apt/keyrings/confluent.gpg
RUN chmod a+r /etc/apt/keyrings/confluent.gpg

RUN echo "deb [signed-by=/etc/apt/keyrings/confluent.gpg] https://packages.confluent.io/deb/8.0 stable main" > /etc/apt/sources.list.d/confluent.list && \
    echo "deb [signed-by=/etc/apt/keyrings/confluent.gpg] https://packages.confluent.io/clients/deb/ bookworm main" >> /etc/apt/sources.list.d/confluent.list

# Download, build and install Python packages.
FROM apt_base AS python_packages
COPY --from=apt_repos /etc/apt/keyrings/confluent.gpg /etc/apt/keyrings/confluent.gpg
COPY --from=apt_repos /etc/apt/sources.list.d/confluent.list /etc/apt/sources.list.d/confluent.list
ENV PIP_NO_CACHE_DIR=1
ENV UV_PROJECT_ENVIRONMENT="/usr/local/"
RUN apt-get update
RUN apt-get install -y --no-install-recommends \
    build-essential \
    cmake \
    libssl-dev \
    zlib1g-dev \
    libffi-dev \
    librdkafka-dev \
    libev4 \
    libev-dev
ADD uv.lock  .
ADD pyproject.toml .
COPY --from=ghcr.io/astral-sh/uv:latest /uv /uvx /bin/
RUN uv sync --frozen

FROM python:$PYTHON_IMAGE_TAG
ARG KUBECTL_VERSION=1.27.3
ARG EKSCTL_VERSION=0.165.0
ARG HELM_VERSION=3.12.2
ENV PYTHONWARNINGS="ignore:unclosed ignore::SyntaxWarning" \
    PYTHONFAULTHANDLER=1 \
    PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1
COPY --from=apt_repos /etc/apt/keyrings/docker.asc /etc/apt/keyrings/docker.asc
COPY --from=apt_repos /etc/apt/sources.list.d/docker.list /etc/apt/sources.list.d/docker.list
COPY --from=apt_repos /usr/share/keyrings/cloud.google.gpg /usr/share/keyrings/cloud.google.gpg
COPY --from=apt_repos /etc/apt/sources.list.d/google-cloud-sdk.list /etc/apt/sources.list.d/google-cloud-sdk.list
COPY --from=apt_repos /etc/apt/keyrings/confluent.gpg /etc/apt/keyrings/confluent.gpg
COPY --from=apt_repos /etc/apt/sources.list.d/confluent.list /etc/apt/sources.list.d/confluent.list
RUN DEBIAN_FRONTEND=noninteractive apt-get update && \
    apt-get install -y --no-install-recommends \
        google-cloud-sdk \
        google-cloud-sdk-gke-gcloud-auth-plugin \
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
        zstd \
        wget \
        psmisc \
        procps \
        docker-ce-cli \
        docker-compose-plugin \
        libev4 \
        libev-dev \
        librdkafka-dev && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*
RUN curl -fsSLo /usr/local/bin/kubectl https://dl.k8s.io/release/v$KUBECTL_VERSION/bin/linux/amd64/kubectl && \
    chmod +x /usr/local/bin/kubectl
RUN curl --silent --location "https://github.com/eksctl-io/eksctl/releases/download/v$EKSCTL_VERSION/eksctl_Linux_amd64.tar.gz" | tar xz -C /tmp && \
    mv /tmp/eksctl /usr/local/bin
RUN curl --silent --location  "https://get.helm.sh/helm-v$HELM_VERSION-linux-amd64.tar.gz" | tar xz -C /tmp && mv /tmp/linux-amd64/helm /usr/local/bin
COPY --from=python_packages /usr/local /usr/local
