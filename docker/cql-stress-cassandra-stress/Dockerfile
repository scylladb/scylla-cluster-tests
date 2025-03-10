FROM rust:1.85-bookworm AS builder

ARG BRANCH
ARG REPO

ENV BRANCH=${BRANCH:-master}
ENV REPO=${REPO:-https://github.com/scylladb/cql-stress.git}
RUN apt-get update && apt-get install -y clang libclang-dev
RUN git clone ${REPO} -b ${BRANCH}

RUN cd cql-stress && RUSTFLAGS="--cfg fetch_extended_version_info" cargo build --release --bin cql-stress-cassandra-stress


FROM rust:1.73-slim AS app

COPY --from=builder /cql-stress/target/release/cql-stress-cassandra-stress /usr/local/bin/
