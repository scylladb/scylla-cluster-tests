FROM golang:alpine as builder

WORKDIR /gemini

RUN apk add curl

ARG version
ENV version=${version:-1.7.6}

RUN curl -LO https://s3.amazonaws.com/downloads.scylladb.com/gemini/${version}/gemini_${version}_Linux_x86_64.tar.gz ;\
    tar -xvf gemini_${version}_Linux_x86_64.tar.gz ;\
    chmod a+x gemini

FROM busybox

ENV PATH="/bin:/sbin:/gemini"
COPY --from=builder /gemini /gemini

WORKDIR /gemini
