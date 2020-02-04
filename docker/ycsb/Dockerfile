FROM openjdk:8 as builder

RUN apt-get update && apt-get install -y \
    git \
    maven

RUN git clone https://github.com/fruch/YCSB.git -b add_status_count
RUN cd YCSB; mvn -pl dynamodb -am clean package -DskipTests
RUN cd /YCSB/dynamodb/target && mkdir YCSB && tar xvvf ycsb-dynamo*.tar.gz -C YCSB --strip-components 1


FROM openjdk:8 as app
COPY --from=builder /YCSB/dynamodb/target/YCSB /YCSB
