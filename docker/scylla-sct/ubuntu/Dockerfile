ARG SOURCE_IMAGE

FROM ${SOURCE_IMAGE}

ENV USER scylla-test
SHELL ["/bin/bash", "-c"]
# Install sudo and other tools, disable autostart of scylla-server and scylla-housekeeping services,
# remove the login banner and add a new user.
#
# Password for the user is `test' and encrypted using openssl command:
#    $ echo -n test | openssl passwd -crypt -stdin -salt 00
#    00hzYw5m.HyAY
#
# For more details see man page for useradd(8)
RUN apt-get update && apt-get install -y --no-install-recommends apt-transport-https gnupg2 software-properties-common curl ssh openssh-server openssh-client
RUN curl -fsSL https://download.docker.com/linux/ubuntu/gpg | apt-key add -
RUN add-apt-repository "deb [arch=amd64] https://download.docker.com/linux/ubuntu $(lsb_release -cs) stable"

RUN apt-get update && \
    echo 'debconf debconf/frontend select Noninteractive' | debconf-set-selections && \
    apt install -y sudo && \
    adduser --disabled-password --gecos "" $USER || true  && \
    usermod -aG sudo $USER && \
    sudo -Hu $USER sh -c "mkdir -m 700 ~/.ssh" && \
    sudo -Hu $USER sh -c "echo 'mkdir -p /home/$USER/.cassandra' > ~/.ssh/rc" && \
    sudo -Hu $USER sh -c "echo 'sudo test -f /root/.cqlshrc && sudo cp /root/.cqlshrc /home/$USER/.cassandra/cqlshrc' >> ~/.ssh/rc" && \
    sudo -Hu $USER sh -c "echo 'sudo test -f /root/.cassandra/cqlshrc && sudo cp /root/.cassandra/cqlshrc /home/$USER/.cassandra/cqlshrc' >> ~/.ssh/rc" && \
    echo "$USER  ALL=(ALL)       NOPASSWD: ALL" >> /etc/sudoers && \
    apt install --no-install-recommends -y \
        iproute2 \
        syslog-ng \
        rsync \
        docker-ce-cli && \
    rm -rf /var/lib/apt/lists/* && \
    echo "autostart=false" >> /etc/supervisord.conf.d/scylla-server.conf && \
    echo "user=scylla" >> /etc/supervisord.conf.d/scylla-server.conf && \
    echo "autostart=false" >> /etc/supervisord.conf.d/scylla-housekeeping.conf && \
    echo "user=scylla" >> /etc/supervisord.conf.d/scylla-housekeeping.conf

COPY ./etc/scylla-manager.conf /etc/supervisord.conf.d/scylla-manager.conf
COPY ./etc/sshd.conf /etc/supervisord.conf.d/sshd.conf
COPY ./etc/sshd-service.sh /sshd-service.sh

RUN export SSH_VERSION=$(ssh -V 2>&1 | cut -f 1  --delimiter=' ' | cut -f 2 --delimiter='_') && \
    dpkg --compare-versions "$SSH_VERSION" ">=" "8.8p1" && \
    echo $'PubkeyAcceptedAlgorithms +ssh-rsa\nHostKeyAlgorithms +ssh-rsa' >> /etc/ssh/sshd_config  || true
