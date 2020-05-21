FROM python:3.8

RUN DEBIAN_FRONTEND=noninteractive apt-get update && \
    apt-get install -y git openssl iproute2 sudo apt-transport-https \
    ca-certificates curl gnupg2 software-properties-common rsync

# Install Docker cli only
RUN curl -fsSL https://download.docker.com/linux/debian/gpg | sudo apt-key add - && \
    DEBIAN_FRONTEND=noninteractive add-apt-repository "deb [arch=amd64] https://download.docker.com/linux/debian $(lsb_release -cs) stable" && \
    DEBIAN_FRONTEND=noninteractive apt-get update && apt-get install -y docker-ce-cli  && \
    sudo groupadd docker && \
    sudo usermod -aG docker `whoami`

ADD requirements-python.txt requirements-python.txt
RUN pip3 install setuptools==46.4.0 pip==20.1.1
RUN pip3 install -r requirements-python.txt

ENV PYTHONWARNINGS ignore:unclosed
