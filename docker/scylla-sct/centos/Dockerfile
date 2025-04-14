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
USER root


RUN curl -L https://download.docker.com/linux/centos/docker-ce.repo -o /etc/yum.repos.d/docker-ce.repo

RUN microdnf update && \
	microdnf -y install \
        iproute \
        sudo \
    	openssh-server \
 		docker-ce-cli \
        procps-ng \
        rsync && \
    microdnf clean all && \
    echo "autostart=false" >> /etc/supervisord.conf.d/scylla-server.conf && \
    echo "user=scylla" >> /etc/supervisord.conf.d/scylla-server.conf && \
    echo "autostart=false" >> /etc/supervisord.conf.d/scylla-housekeeping.conf && \
    echo "user=scylla" >> /etc/supervisord.conf.d/scylla-housekeeping.conf && \
    sed -i "\:/dev/stderr:d" /etc/bashrc && \
    useradd -G wheel -p 00hzYw5m.HyAY $USER && \
    echo "$USER  ALL=(ALL)       NOPASSWD: ALL" >> /etc/sudoers && \
    sudo -Hu $USER sh -c "mkdir -m 700 ~/.ssh" && \
    sudo -Hu $USER sh -c "echo 'mkdir -p /home/$USER/.cassandra' > ~/.ssh/rc" && \
    sudo -Hu $USER sh -c "echo 'sudo test -f /root/.cqlshrc && sudo cp /root/.cqlshrc /home/$USER/.cassandra/cqlshrc' >> ~/.ssh/rc" && \
    sudo -Hu $USER sh -c "echo 'sudo test -f /root/.cassandra/cqlshrc && sudo cp /root/.cassandra/cqlshrc /home/$USER/.cassandra/cqlshrc' >> ~/.ssh/rc"

COPY ./etc/scylla-manager.conf /etc/supervisord.conf.d/scylla-manager.conf
COPY ./etc/sshd.conf /etc/supervisord.conf.d/sshd.conf
COPY ./etc/sshd-service.sh /sshd-service.sh
