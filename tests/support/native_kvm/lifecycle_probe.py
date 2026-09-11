"""In-sandbox pulse used only to observe actual root-sandbox termination."""

import time
from pathlib import Path

assert Path("/proc/gvisor/kernel_is_gvisor").is_file()
pulse = Path("/output/lifecycle-pulse")
sequence = 0
while True:
    sequence += 1
    pulse.write_text(str(sequence))
    time.sleep(0.05)
