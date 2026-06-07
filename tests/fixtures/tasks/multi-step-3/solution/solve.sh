#!/bin/sh
# Combined oracle solution — writes all three step files. step_runner
# invokes solve.sh once per step; the script is idempotent across runs.
echo step1 > /workspace/step1.txt
echo step2 > /workspace/step2.txt
echo step3 > /workspace/step3.txt
