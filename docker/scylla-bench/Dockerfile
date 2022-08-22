FROM golang:1.18

ARG version
ARG fork
ENV version=${version}
ENV fork=${fork:-scylladb/scylla-bench}

RUN apt-get update; apt-get -y install unzip curl

RUN curl -Lo sb.zip https://github.com/${fork}/archive/refs/${version}.zip ;\
    unzip sb.zip ;\
    cd ./scylla-bench-* ;\
    GO111MODULE=on go install .
