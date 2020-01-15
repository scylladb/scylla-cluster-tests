ARG SOURCE_IMAGE

FROM ${SOURCE_IMAGE}

# Install sshd, sudo and collectd:
RUN yum -y install openssh-server openssh-clients passwd initscripts sudo collectd net-tools rsync openssl file wget && \
    yum clean all

# configure ssh
RUN mkdir /var/run/sshd
RUN /usr/sbin/sshd-keygen
RUN useradd scylla-test && echo -e "test\ntest" | passwd --stdin scylla-test
ENV USER scylla-test
RUN mkdir -p /home/$USER/.ssh
ADD scylla-test.pub /home/$USER/.ssh/authorized_keys
RUN chown $USER /home/$USER/.ssh/authorized_keys
RUN chown -R $USER:$USER /home/$USER/.ssh/authorized_keys
RUN chmod 700 /home/$USER/.ssh/authorized_keys

# configure sudo
RUN usermod -aG wheel $USER
RUN chmod u+w /etc/sudoers
RUN echo "scylla-test  ALL=(ALL)       NOPASSWD: ALL" >> /etc/sudoers

EXPOSE 22

# Supervisord configuration:
ADD etc/sshd.conf /etc/supervisord.conf.d/sshd.conf
ADD etc/collectd.conf /etc/supervisord.conf.d/collectd.conf
ADD etc/scylla-manager.conf /etc/supervisord.conf.d/scylla-manager.conf
ADD etc/scylla-manager-agent.conf /etc/supervisord.conf.d/scylla-manager-agent.conf
RUN echo "autostart=false" >> /etc/supervisord.conf.d/scylla-server.conf
RUN echo "autostart=false" >> /etc/supervisord.conf.d/scylla-housekeeping.conf

# replacement of systemctl, since inside docker there is no D-Bus connection available
RUN curl https://raw.githubusercontent.com/gdraheim/docker-systemctl-replacement/master/files/docker/systemctl.py --output /usr/bin/systemctl
RUN chmod 777 /usr/bin/systemctl

RUN yum -y install git && \
    curl -LO https://storage.googleapis.com/golang/go1.13.linux-amd64.tar.gz && \
    tar -C /usr/local -xvzf go1.13.linux-amd64.tar.gz && \
    echo 'export GOPATH=$HOME/go' >> $HOME/.bashrc && \
    echo 'export PATH=$PATH:/usr/local/go/bin' >> $HOME/.bashrc && \
    source $HOME/.bashrc && \
    go get github.com/scylladb/scylla-bench

# enable once ycsb is needed
# RUN cd ~/ && \
#     curl -O --location https://github.com/brianfrankcooper/YCSB/releases/download/0.15.0/ycsb-0.15.0.tar.gz && \
#     tar xfvz ycsb-0.15.0.tar.gz
