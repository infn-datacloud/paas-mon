#!/bin/bash
set -euo pipefail

# -----------------------------------------------------------------------------
#  Start the consumer/processor. It reads those messages, converts them,
#         and writes to a different Kafka topic.
# -----------------------------------------------------------------------------

echo "[INFO] Starting processor (feeder-monitoring.py)..."

# exec replaces the shell, proper signal handling (e.g., Ctrl+C, docker stop)
exec python3 /app/processors/feeder-monitoring.py
