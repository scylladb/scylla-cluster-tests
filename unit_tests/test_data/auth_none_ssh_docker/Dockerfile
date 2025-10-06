FROM ubuntu:25.10
RUN apt-get update && apt-get install -y openssh-server
RUN echo 'root:root' | chpasswd
RUN useradd -m test
RUN passwd -d test
RUN echo 'PermitRootLogin yes' > /etc/ssh/sshd_config \
    && echo 'PasswordAuthentication yes' >> /etc/ssh/sshd_config \
    && echo 'PermitEmptyPasswords yes' >> /etc/ssh/sshd_config \
    && echo 'AuthenticationMethods none' >> /etc/ssh/sshd_config \
    && echo 'UsePAM no' >> /etc/ssh/sshd_config
EXPOSE 22
CMD ["/usr/sbin/sshd", "-D"]
