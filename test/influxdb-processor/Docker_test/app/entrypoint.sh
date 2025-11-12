#!/bin/bash
set -euo pipefail

# -----------------------------------------------------------------------------
# Step 1: Simulate the feeder. It publishes JSON messages to Kafka.
# Step 2: Start the consumer/processor. It reads those messages, converts them,
#         and writes to a different Kafka topic.
# -----------------------------------------------------------------------------

echo "[INFO] Starting feeder simulator (Feeder_simulator.py)..."
python3 /app/scripts/Feeder_simulator.py

echo "[INFO] Feeder simulator completed successfully."
echo "[INFO] Starting converter-processor (monitoring-converter-processor.py)..."

# exec replaces the shell, proper signal handling (e.g., Ctrl+C, docker stop)
exec python3 /app/scripts/monitoring-converter.py
