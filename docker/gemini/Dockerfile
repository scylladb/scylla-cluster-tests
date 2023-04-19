FROM golang:alpine as builder

WORKDIR /gemini

RUN apk add curl

ARG version
ENV version=${version:-1.7.6}

RUN curl -LO https://s3.amazonaws.com/downloads.scylladb.com/gemini/${version}/gemini_${version}_Linux_x86_64.tar.gz ;\
    tar -xvf gemini_${version}_Linux_x86_64.tar.gz ;\
    chmod a+x gemini

FROM bitnami/minideb:bullseye

ENV PATH="/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin:/gemini"
COPY --from=builder /gemini /gemini

WORKDIR /gemini
