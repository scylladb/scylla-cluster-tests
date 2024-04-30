FROM rust:1.77-buster AS builder
RUN cargo install --git https://github.com/scylladb/latte --branch main

FROM debian:buster-slim
# NOTE: 'libfontconfig openssl' libs are required by the 'latte' binary during execution
RUN apt update && apt -y install \
    libfontconfig \
    openssl \
  && rm -rf /var/lib/apt/lists/*
COPY --from=builder /usr/local/cargo/bin/latte /bin/latte

CMD ["bash -c"]
