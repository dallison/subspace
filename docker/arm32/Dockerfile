# Native aarch64/amd64 image used to cross-compile Subspace for armhf.
# Colima on Apple Silicon runs this as linux/arm64; qemu-user-static runs the
# armhf binaries it produces.
FROM ubuntu:24.04

ENV DEBIAN_FRONTEND=noninteractive

RUN apt-get update && apt-get install -y --no-install-recommends \
        build-essential \
        ca-certificates \
        cmake \
        git \
        ninja-build \
        pkg-config \
        python3 \
        gcc-arm-linux-gnueabihf \
        g++-arm-linux-gnueabihf \
        libc6-dev-armhf-cross \
        qemu-user-static \
    && rm -rf /var/lib/apt/lists/*
