#!/bin/bash
set -euo pipefail

# Read admin token generated at build time
ADMIN_TOKEN=$(cat /generated_admin_token)

# Find bucket ID
BUCKET_ID=$(influx bucket list \
  --token "$ADMIN_TOKEN" \
  --org "$DOCKER_INFLUXDB_INIT_ORG" \
  --name "$DOCKER_INFLUXDB_INIT_BUCKET" \
  --json | jq -r '.[0].id')

# Create tokens directory structure
mkdir -p /tokens/admin /tokens/grafana /tokens/telegraf

# Save admin token persistently
echo "$ADMIN_TOKEN" > /tokens/admin/admin_token.txt

# Create Grafana read token
influx auth create \
  --token "$ADMIN_TOKEN" \
  --org "$DOCKER_INFLUXDB_INIT_ORG" \
  --read-bucket "$BUCKET_ID" \
  --description "Grafana read-only token" \
  --json | jq -r '.token' > /tokens/grafana/grafana_token.txt

# Create Telegraf write token
influx auth create \
  --token "$ADMIN_TOKEN" \
  --org "$DOCKER_INFLUXDB_INIT_ORG" \
  --write-bucket "$BUCKET_ID" \
  --description "Telegraf write-only token" \
  --json | jq -r '.token' > /tokens/telegraf/telegraf_token.txt



