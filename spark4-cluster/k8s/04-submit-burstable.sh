#!/usr/bin/env bash
# Step 4 — BURSTABLE run.  limit.cores (4) > request.cores (2) == Burstable QoS.
# The same 4 task threads now get 4 CPUs of quota and run at full speed.
# Everything else is identical to step 3 — only limit.cores changed.
set -euo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")"
exec ./_submit.sh burstable 4
