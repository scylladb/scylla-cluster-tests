FROM golang:alpine as builder

WORKDIR /gemini

RUN apk add curl

ARG version
ENV version=${version:-1.8.7}

RUN curl -LO https://github.com/scylladb/gemini/releases/download/v${version}/gemini_${version}_linux_amd64.tar.gz ;\
    tar -xvf gemini_${version}_linux_amd64.tar.gz ;\
    chmod a+x gemini

FROM ubuntu:22.04

ENV PATH="/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin:/gemini"
COPY --from=builder /gemini /gemini

WORKDIR /gemini
