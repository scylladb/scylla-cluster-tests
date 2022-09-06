FROM openjdk:8 as builder

RUN apt-get update && apt-get install -y \
    git \
    maven

RUN git clone https://github.com/fruch/YCSB.git -b add_status_count
RUN cd YCSB; mvn -pl dynamodb -am clean package -DskipTests
RUN cd /YCSB/dynamodb/target && mkdir -p YCSB && tar xvvf ycsb-dynamo*.tar.gz -C YCSB --strip-components 1

RUN cd YCSB; mvn -pl cassandra -am clean package -DskipTests
RUN cd /YCSB/cassandra/target && mkdir -p YCSB && tar xvvf ycsb-cassandra-*.tar.gz -C YCSB --strip-components 1

RUN cd YCSB; mvn -pl scylla -am clean package -DskipTests
RUN cd /YCSB/scylla/target && mkdir -p YCSB && tar xvvf ycsb-scylla-*.tar.gz -C YCSB --strip-components 1


FROM openjdk:8 as app
RUN echo 'networkaddress.cache.ttl=0' >> $JAVA_HOME/jre/lib/security/java.security
RUN echo 'networkaddress.cache.negative.ttl=0' >> $JAVA_HOME/jre/lib/security/java.security
COPY java.policy $JAVA_HOME/jre/lib/security/java.policy
COPY --from=builder /YCSB/dynamodb/target/YCSB /YCSB
COPY --from=builder /YCSB/cassandra/target/YCSB /YCSB
COPY --from=builder /YCSB/scylla/target/YCSB /YCSB
