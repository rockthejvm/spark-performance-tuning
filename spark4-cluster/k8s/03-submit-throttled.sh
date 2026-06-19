#!/usr/bin/env bash
# Step 3 — THROTTLED run.  limit.cores == request.cores (2) == Guaranteed QoS, NO burst room.
# The 4 task threads are squeezed into a 2-CPU CFS quota, so the job runs ~2x slower.
# While this blocks, open another terminal and run ./05-results.sh to watch cpu.stat.
set -euo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")"
exec ./_submit.sh throttled 2
