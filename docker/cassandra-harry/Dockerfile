FROM maven:3-eclipse-temurin-21 AS builder
RUN apt -y update && apt -y install make \
&& git clone https://github.com/apache/cassandra-harry.git \
&& cd cassandra-harry \
&& git checkout 42a50b445163783ee9b986053f0e9cee611b4688 \
&& make standalone

FROM openjdk:21-slim AS app
COPY --from=builder /cassandra-harry /cassandra-harry

RUN sed -i '2s;^;HARRY_HOME=/cassandra-harry\n;'  /cassandra-harry/scripts/cassandra-harry && \
    sed -i 's/external-.*-SNAPSHOT.jar/external-*-SNAPSHOT.jar/' /cassandra-harry/scripts/cassandra-harry && \
    ln -svf /cassandra-harry/scripts/cassandra-harry /usr/bin/cassandra-harry

WORKDIR /cassandra-harry
