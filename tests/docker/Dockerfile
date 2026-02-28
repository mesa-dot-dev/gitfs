# Stage 1: Build the mesafs binary
FROM rust:1.93-bookworm AS builder

RUN apt-get update && apt-get install -y \
    build-essential \
    cmake \
    libfuse3-dev \
    libprotobuf-dev \
    pkg-config \
    protobuf-compiler \
    protobuf-compiler-grpc \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /src
COPY Cargo.toml Cargo.lock build.rs ./
COPY .git/ .git/
COPY lib/ lib/
COPY src/ src/

RUN cargo build --release

# Stage 2: Runtime image
FROM python:3.14-slim-bookworm

RUN apt-get update && apt-get install -y \
    build-essential \
    ca-certificates \
    cmake \
    findutils \
    fuse3 \
    git \
    && rm -rf /var/lib/apt/lists/*

# Allow non-root users to mount FUSE filesystems
RUN sed -i 's/#user_allow_other/user_allow_other/' /etc/fuse.conf || true

COPY --from=builder /src/target/release/mesafs /usr/local/bin/mesafs

RUN mkdir -p /mnt/mesafs /etc/mesafs
COPY tests/docker/config.toml /etc/mesafs/config.toml
COPY tests/docker/entrypoint.sh /usr/local/bin/entrypoint.sh
RUN chmod +x /usr/local/bin/entrypoint.sh

ENTRYPOINT ["/usr/local/bin/entrypoint.sh"]
CMD ["sleep", "infinity"]
