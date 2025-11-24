#!/bin/bash
set -e

export INFLUX_TELEGRAF_TOKEN=$(cat /tokens/telegraf/telegraf_token.txt)

exec telegraf
