#!/bin/bash
set -e

# Read token from mounted volume
export INFLUX_GRAFANA_TOKEN=$(cat /tokens/grafana/grafana_token.txt)

# Execute original entrypoint
exec /run.sh
