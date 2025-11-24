## InfluxDB Initialization and Automatic Token Generation

This document describes the initialization workflow implemented for the InfluxDB service, including the automatic creation of all required authentication tokens (admin, Grafana read-only, Telegraf write-only).

### Overview

The InfluxDB container is built from a custom Dockerfile that generates a random admin token at build time and stores it inside the image. When the container starts for the first time, the official InfluxDB entrypoint detects that no database exists yet and performs the full initialization routine. During this phase, all scripts inside:

/docker-entrypoint-initdb.d/

are executed automatically. The script `create-all-tokens.sh` then generates the remaining tokens and stores them in named volumes.

### How the Initialization Works

#### 1. Build-time: Generate Admin Token
During docker build:
- A random admin token is generated (`openssl rand -hex 32`)
- It is written to `/generated_admin_token` inside the image
- It is exposed to the InfluxDB entrypoint via:
  DOCKER_INFLUXDB_INIT_ADMIN_TOKEN_FILE=/generated_admin_token


#### 2. First Container Startup: InfluxDB Setup

When the container starts, the entrypoint checks `/var/lib/influxdb2`.  
If the directory is empty, this is considered a first-run setup. InfluxDB:

1. Runs the automatic setup using DOCKER_INFLUXDB_INIT_* variables  
2. Loads the admin token from `/generated_admin_token`  
3. Creates the initial organization and bucket  
4. Executes all scripts in `/docker-entrypoint-initdb.d/`

#### 3. Token Generation Script

`create-all-tokens.sh` is executed automatically and:
- Reads the admin token from `/generated_admin_token`
- Retrieves the bucket ID
- Creates:
  - Grafana read-only token
  - Telegraf write-only token
- Saves tokens into the mounted volumes:
  /tokens/grafana/grafana_token.txt
  /tokens/telegraf/telegraf_token.txt
  /tokens/admin/admin_token.txt

These named volumes persist independently from container recreation.

### Persistence and Re-Initialization Rules

InfluxDB initialization only occurs once.

First startup (empty /var/lib/influxdb2):
- Setup is executed
- Token generation script runs
- Tokens are generated and saved

Subsequent startups (non-empty /var/lib/influxdb2):
- No setup is performed
- No scripts in /docker-entrypoint-initdb.d/ are executed
- Tokens remain valid
- Volumes persist

### Recommendations

1. Before the first `docker compose up`, ensure `/var/lib/influxdb2` is empty.
2. Do not delete the named volumes unless you want to regenerate all tokens (in that case, also wipe the InfluxDB data volume).
3. Tokens persist even if the container is recreated, as long as the volumes remain intact.
