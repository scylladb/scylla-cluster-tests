FROM scylladb/scylla

# Install sshd, sudo and collectd:
RUN yum -y update && \
   yum -y install openssh-server openssh-clients passwd initscripts sudo collectd && \
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

