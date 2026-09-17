"""In-sandbox pulse used only to observe actual root-sandbox termination."""

import time
from pathlib import Path


def publish_pulse(pulse, sequence):
    # Deadline cleanup may kill this process between truncation and writing.
    # Publish a complete sample while preserving the preceding one until then.
    staged = pulse.with_name(pulse.name + ".next")
    staged.write_text(str(sequence))
    staged.replace(pulse)


def main():
    assert Path("/proc/gvisor/kernel_is_gvisor").is_file()
    pulse = Path("/output/lifecycle-pulse")
    sequence = 0
    while True:
        sequence += 1
        publish_pulse(pulse, sequence)
        time.sleep(0.05)


if __name__ == "__main__":
    main()
