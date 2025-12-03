#!/bin/bash
set -e

# Read token from mounted volume
export INFLUX_TELEGRAF_TOKEN=$(cat /tokens/telegraf/telegraf_token.txt)

# Execute original entrypoint
exec telegraf
