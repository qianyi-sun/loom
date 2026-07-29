FROM ubuntu@sha256:52df9b1ee71626e0088f7d400d5c6b5f7bb916f8f0c82b474289a4ece6cf3faf

ARG LOOM_CANDIDATE_SHA

LABEL org.opencontainers.image.source="https://github.com/qianyi-sun/loom"
LABEL org.opencontainers.image.description="Isolated QEMU runtime for Loom ephemeral CI runners"
LABEL loom.candidate.sha="${LOOM_CANDIDATE_SHA}"

RUN apt-get update \
    && DEBIAN_FRONTEND=noninteractive apt-get install -y --no-install-recommends \
        ca-certificates \
        cloud-image-utils \
        genisoimage \
        nftables \
        qemu-system-x86 \
        qemu-utils \
        util-linux \
    && rm -rf /var/lib/apt/lists/*

COPY deploy/ci-runners/qemu-entrypoint.sh /usr/local/bin/loom-ci-qemu-entrypoint

ENTRYPOINT ["/usr/local/bin/loom-ci-qemu-entrypoint"]
