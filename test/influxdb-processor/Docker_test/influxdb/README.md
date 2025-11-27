## InfluxDB Initialization and Automatic Token Management

This document explains how the custom InfluxDB container initializes itself,
creates all required authentication tokens, and persists them using named volumes.

--------------------------------------------------------------------------------
## Overview

The InfluxDB service uses a custom build process and a runtime initialization
mechanism based on scripts placed into the following directory **inside the InfluxDB image**:

    /docker-entrypoint-initdb.d/

The workflow consists of:
1. Build-time generation of an admin token
2. First-run InfluxDB setup
3. Automatic creation of Grafana and Telegraf tokens
4. Persistence of all tokens through named volumes

--------------------------------------------------------------------------------
## 1. Build-Time Initialization (Admin Token)

During the Docker image build (from [`Dockerfile.influxdb`](Dockerfile.influxdb)):

- A random admin token is generated via:

      openssl rand -hex 32

- The token is stored inside the image at:

      /generated_admin_token

- The file is passed to the InfluxDB entrypoint using:

      DOCKER_INFLUXDB_INIT_ADMIN_TOKEN_FILE=/generated_admin_token

No bucket, organization, or user is created at build time.
Only the admin token exists at this stage.

--------------------------------------------------------------------------------
## 2. First Container Startup (Automatic Setup)

When the container starts, the InfluxDB entrypoint checks:

      /var/lib/influxdb2

Behavior:

- If the directory is **empty** → first-run setup  
- If the directory is **not empty** → startup without initialization  

During first run, InfluxDB reads values from the following variables:

      DOCKER_INFLUXDB_INIT_MODE
      DOCKER_INFLUXDB_INIT_USERNAME
      DOCKER_INFLUXDB_INIT_PASSWORD
      DOCKER_INFLUXDB_INIT_ORG
      DOCKER_INFLUXDB_INIT_BUCKET
      DOCKER_INFLUXDB_INIT_RETENTION
      DOCKER_INFLUXDB_INIT_ADMIN_TOKEN_FILE

Then, InfluxDB executes all scripts found in:

      /docker-entrypoint-initdb.d/

including the token generation script.

--------------------------------------------------------------------------------
## 3. Token Generation Script (Grafana, Telegraf, Admin Copy)

The script:

      [`influxdb/create-all-tokens.sh`](influxdb/create-all-tokens.sh)

runs during the initialization phase.

It performs these steps:

1. Reads the admin token 
2. Retrieves the bucket ID 
3. Creates three tokens:
   - Grafana read-only token
   - Telegraf write-only token
   - Admin token copy 
4. Stores the tokens in the mounted named volumes:

         /tokens/grafana/grafana_token.txt
         /tokens/telegraf/telegraf_token.txt
         /tokens/admin/admin_token.txt

These paths correspond to the Docker volumes:

         grafana_tokens
         telegraf_tokens
         admin_tokens

which persist independently of container recreation.

--------------------------------------------------------------------------------
## 4. Persistence and Re-Initialization Rules

### First startup (empty `/var/lib/influxdb2`)
- InfluxDB performs full setup  
- `create-all-tokens.sh` is executed  
- All tokens are written to:

        /tokens/grafana/
        /tokens/telegraf/
        /tokens/admin/

### Subsequent startups (non-empty `/var/lib/influxdb2`)
- No setup is performed  
- No script inside `/docker-entrypoint-initdb.d/` is executed  
- All previously generated tokens remain valid  
- Named volumes preserve token files

### Token loss conditions
Tokens are deleted **only if the named volumes are explicitly removed**.

--------------------------------------------------------------------------------
## Recommendations

1. Before running:

         docker compose up

   ensure the host directory mapped to:

         /var/lib/influxdb2

   is empty, otherwise initialization will not run.

2. Do **not** delete the token volumes:

         grafana_tokens
         telegraf_tokens
         admin_tokens

   unless you intend to regenerate all tokens.

3. To fully reset InfluxDB authentication:
   - Remove the volume mapped to `/var/lib/influxdb2`
   - Remove all `/tokens/*` volumes
   - Rebuild the image
   - Run `docker compose up` again

--------------------------------------------------------------------------------





