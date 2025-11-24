#!/bin/bash
set -euo pipefail

# -----------------------------------------------------------------------------
# Simulate the feeder. It publishes JSON messages to Kafka.
# -----------------------------------------------------------------------------

echo "[INFO] Starting feeder simulator (Feeder_simulator.py)..."
python3 /feeder_loader/scripts/Feeder_simulator.py

echo "[INFO] Feeder simulator completed successfully."

