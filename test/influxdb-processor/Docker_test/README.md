# InfluxDB Processor Test Environment (Docker-based)

This directory provides a self-contained Docker-based environment for testing the full data pipeline — from the feeder simulation to InfluxDB and Grafana visualization.

---

## Directory overview

- [docker-compose.yml](docker-compose.yml) — orchestrates all containers 
- [.env](.env) / [.env.example](.env.example) — environment configuration files 

- **app/**
  - [Dockerfile](app/Dockerfile), [entrypoint.sh](app/entrypoint.sh)
  - **scripts/**
    - [Feeder_simulator.py](app/scripts/Feeder_simulator.py) — simulates the feeder by publishing JSON messages to Kafka 
    - [monitoring-converter.py](app/scripts/monitoring-converter.py) — processes messages, converts them to InfluxDB Line Protocol via the json_to_lines function [msg_v13_model.py](app/scripts/modules/monitoring/msg_v13_model.py), and republishes them into a new Kafka topic
    - **input/** — contains test data file `output_feeder_msgs.json` (not versioned)
    - **modules/** 
      - [config.py](app/scripts/modules/config.py) — centralizes configuration for the environment, importing variables from [.env](.env)
      - **monitoring/** — defines models and utilities for message conversion:
        - [Influxdb_message.py](app/scripts/modules/monitoring/Influxdb_message.py)— defines `InfluxdbMessage` for LP and JSON output
        - [msg_v13_model.py](app/scripts/modules/monitoring/msg_v13_model.py) — defines the Pydantic model for feeder messages (v1.3.0)
      - **utilities/** — implements Kafka client and logging support:
        - provides the `KafkaClient` class (producer & consumer)
        - used by both [Feeder_simulator.py](app/scripts/Feeder_simulator.py) and [monitoring-converter.py](app/scripts/monitoring-converter.py)



- **data/**
  - Kafka, Telegraf, and Grafana persistent data (e.g. [telegraf.conf](data/telegraf/telegraf.conf))

---

## Setup instructions

### 1. Environment configuration

Before running the stack, create a `.env` file from the provided template:

```bash
cp .env.example .env

```
Then open .env and set your own values (username, password, tokens, etc.).

### 2.  Test data

Place your JSON input file at :
[app/scripts/input/output_feeder_msgs.json](app/scripts/input)  *(not versioned in Git)*


### 3.  Build and run the environment

### First setup (initial build)
Before the first launch, build the custom image for the app_python service.
```bash
docker compose build --no-cache app_python
docker compose up -d
```
### Subsequents runs
```bash
docker compose up -d
```

### Full environment reset (clean state)

If you need to reset all services and clear persistent data (e.g., Kafka topics, InfluxDB buckets):
```bash
docker compose down -v --remove-orphans
docker compose up -d --force-recreate --renew-anon-volumes
```
This will remove all local volumes, including stored metrics and Grafana dashboards.

### 4.  Container lifecycle

When the stack starts:

1. **Kafka broker** initializes first.

2. The **app_python** container runs [entrypoint.sh](app/entrypoint.sh):

  - executes [Feeder_simulator.py](app/scripts/Feeder_simulator.py) to publish JSON messages to the Kafka topic `paas_mon`;
  - then runs [monitoring-converter.py](app/scripts/monitoring-converter.py) to:
    - read from 'paas_mon',
    - convert messages to Influxdb Line Protocol using [msg_v13_model.py](app/scripts/modules/monitoring/msg_v13_model.py),
    - republish them to the output topic (`KAFKA_OUTPUT_TOPIC`, defined in `.env`).

3. **Telegraf** consumes from the output topic and writes to InfluxDB.
4. **Grafana** visualizes metrics from InfluxDB.
