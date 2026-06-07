#!/bin/sh
SIZE_MB=100
if [ "${LOOM_TEST_LARGE_ARTIFACT_GB:-}" = "1" ]; then
    SIZE_MB=1024
fi
dd if=/dev/zero of=/workspace/payload.bin bs=1M count="$SIZE_MB" status=none
