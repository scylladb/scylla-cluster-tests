ARG SOURCE_IMAGE

FROM ${SOURCE_IMAGE}

ENV USER scylla-test

# Install sudo and other tools, disable autostart of scylla-server and scylla-housekeeping services,
# remove the login banner and add a new user.
#
# Password for the user is `test' and encrypted using openssl command:
#    $ echo -n test | openssl passwd -crypt -stdin -salt 00
#    00hzYw5m.HyAY
#
# For more details see man page for useradd(8)
RUN yum -y install \
        iproute \
        sudo \
        syslog-ng \
        rsync && \
    yum clean all && \
    echo $'[program:scylla-manager]\n\
command=/usr/bin/scylla-manager --developer-mode\n\
stdout_logfile=/dev/stdout\n\
stdout_logfile_maxbytes=0\n\
stderr_logfile=/dev/stderr\n\
stderr_logfile_maxbytes=0' > /etc/supervisord.conf.d/scylla-manager.conf && \
    echo "autostart=false" >> /etc/supervisord.conf.d/scylla-server.conf && \
    echo "autostart=false" >> /etc/supervisord.conf.d/scylla-housekeeping.conf && \
    sed -i "\:/dev/stderr:d" /etc/bashrc && \
    useradd -G wheel -p 00hzYw5m.HyAY $USER && \
    echo "$USER  ALL=(ALL)       NOPASSWD: ALL" >> /etc/sudoers && \
    sudo -Hu $USER sh -c "mkdir -m 700 ~/.ssh"
