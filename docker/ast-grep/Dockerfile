# Use a lightweight Linux base with a C compiler for building the grammar
FROM rust:1.96-slim AS rust-builder

RUN apt-get update && apt-get install -y git gcc make && rm -rf /var/lib/apt/lists/*

# 1. Install ast-grep globally
RUN cargo install ast-grep --locked

# 2. Clone and compile the Groovy Tree-sitter grammar
WORKDIR /opt
# Stage 1: Build the grammar
RUN cargo install ast-grep --locked

WORKDIR /opt
RUN git clone --depth=1 https://github.com/murtaza64/tree-sitter-groovy.git
WORKDIR /opt/tree-sitter-groovy/src
RUN gcc -shared -o groovy.so -fPIC parser.c

# Stage 2: Pure Runtime Engine
FROM rust:1.96-slim

COPY --from=rust-builder /usr/local/cargo/bin/ast-grep /usr/local/bin/ast-grep
COPY --from=rust-builder /opt/tree-sitter-groovy/src/groovy.so /opt/groovy.so

# Set the working directory to the mounted project folder
WORKDIR /workspace

# Force ast-grep to run inside the mounted folder using the engine's library
ENTRYPOINT ["ast-grep", "scan"]
