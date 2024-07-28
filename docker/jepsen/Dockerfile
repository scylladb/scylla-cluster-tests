FROM debian:bookworm-slim

ENV container=docker
STOPSIGNAL SIGRTMIN+3

ENV LEIN_ROOT=true

# JDK21 from adoptium
RUN apt-get -qy update && apt-get install -y --no-install-recommends gnupg2 apt-transport-https software-properties-common curl
RUN curl -fsSL https://packages.adoptium.net/artifactory/api/gpg/key/public | gpg --dearmor -o /etc/apt/keyrings/adoptium.gpg
RUN echo "deb [signed-by=/etc/apt/keyrings/adoptium.gpg] https://packages.adoptium.net/artifactory/deb $(awk -F= '/^VERSION_CODENAME/{print$2}' /etc/os-release) main" | tee -a /etc/apt/sources.list.d/adoptium.list

ADD ./apt-preferences /etc/apt/preferences

#
# Jepsen dependencies
#
RUN apt-get -qy update && \
    apt-get -qy install \
        curl git iputils-ping libjna-java temurin-21-jdk wget

RUN wget https://raw.githubusercontent.com/technomancy/leiningen/stable/bin/lein && \
    mv lein /usr/bin && \
    chmod +x /usr/bin/lein && \
    lein self-install

ADD ./bashrc /root/.bashrc
ADD ./init.sh /init.sh
RUN chmod +x /init.sh

CMD ["/init.sh"]
