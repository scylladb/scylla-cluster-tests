FROM rust:1.84-bookworm AS builder

ARG BRANCH
ARG REPO

ENV BRANCH=${BRANCH:-master}
ENV REPO=${REPO:-https://github.com/scylladb/cql-stress.git}

RUN git clone ${REPO} -b ${BRANCH}

RUN cd cql-stress && cargo build --release --bin cql-stress-cassandra-stress

FROM debian:bookworm-slim
RUN apt update && apt -y install \
    openssl \
  && rm -rf /var/lib/apt/lists/*

COPY --from=builder /cql-stress/target/release/cql-stress-cassandra-stress /usr/local/bin/
