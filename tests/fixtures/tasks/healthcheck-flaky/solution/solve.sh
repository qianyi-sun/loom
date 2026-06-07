#!/bin/sh
# Make the healthcheck fail twice (2s of "not ready") then succeed.
( sleep 2 && touch /workspace/.ready ) &
echo ok > /workspace/ok.txt
