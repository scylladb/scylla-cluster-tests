FROM rust:1.85-bookworm AS builder

ARG BRANCH
ARG REPO

ENV BRANCH=${BRANCH:-master}
ENV REPO=${REPO:-https://github.com/scylladb/cql-stress.git}
RUN apt-get update && apt-get install -y clang libclang-dev
RUN git clone ${REPO} -b ${BRANCH}

RUN cd cql-stress && RUSTFLAGS="--cfg fetch_extended_version_info" cargo build --release --bin cql-stress-cassandra-stress

FROM debian:bookworm-slim
RUN apt update && apt -y install \
    openssl \
  && rm -rf /var/lib/apt/lists/*

COPY --from=builder /cql-stress/target/release/cql-stress-cassandra-stress /usr/local/bin/
