## InfluxDB Monitoring Pipeline — Docker Test Environment

This directory provides all components required to test the full monitoring workflow of the PaaS system.  
In the real infrastructure, the feeder publishes messages to a Kafka topic, a monitoring component consumes and parses those messages, then writes the processed results to a second Kafka topic. These processed messages are ingested by Telegraf and stored into InfluxDB, while Grafana visualizes the monitoring data by querying InfluxDB.

The environment in this directory reproduces exactly this architecture:  
a Kafka broker for message transport, a feeder simulator that publishes sample messages, a processor that parses and transforms them, Telegraf for ingestion into InfluxDB, and Grafana for data visualization.

---


## Architecture Overview

This environment is composed of six main services, each responsible for a specific part of the monitoring workflow.

### **Kafka Broker**
A single-node Kafka service acting as both **controller** and **broker**, suitable for local and integration testing.  
Configuration parameters such as exposed ports, advertised hosts, and listener settings are defined in the `.env` file.  
The broker automatically creates topics when producers or consumers connect to a non-existing topic (`KAFKA_AUTO_CREATE_TOPICS_ENABLE=true`).

### **InfluxDB (custom image)**
InfluxDB is built locally at first startup using [`influxdb/Dockerfile.influxdb`](influxdb/Dockerfile.influxdb).  
During the image build, an **admin token** is generated once and stored inside the container at `/generated_admin_token` (this directory exists **only inside the image** and is **not** mounted as a volume).
For a detailed description of the InfluxDB token and initialization workflow, see [`influxdb/README.md`](influxdb/README.md).

At every container startup, the script [`influxdb/create-all-tokens.sh`](influxdb/create-all-tokens.sh) generates new authentication tokens for:
- **Grafana** → written to `/tokens/grafana` (backed by the `grafana_tokens` named volume)  
- **Telegraf** → written to `/tokens/telegraf` (backed by the `telegraf_tokens` named volume)  

These directories are created and populated **by the InfluxDB container** and become available to Grafana and Telegraf through the mounted volumes.

### **Telegraf (custom image)**
The Telegraf container is built locally using [`telegraf/Dockerfile.telegraf`](telegraf/Dockerfile.telegraf).  
On each run, its entrypoint script [`telegraf/telegraf-entrypoint.sh`](telegraf/telegraf-entrypoint.sh) reads the token file:
```
/tokens/telegraf/telegraf_token.txt
```
(where `/tokens/telegraf` comes from the `telegraf_tokens` named volume created by InfluxDB)  
and exports its content as the environment variable `INFLUX_TELEGRAF_TOKEN`.

Telegraf also mounts its configuration file:
```
./data/telegraf/telegraf.conf  →  /etc/telegraf/telegraf.conf
```
This file defines how Telegraf consumes messages from Kafka and writes them into InfluxDB.

### **Grafana (custom image)**
Grafana is built at first startup using [`grafana/Dockerfile.grafana`](grafana/Dockerfile.grafana).  
At every run, its entrypoint script [`grafana/grafana-entrypoint.sh`](grafana/grafana-entrypoint.sh) reads:
```
/tokens/grafana/grafana_token.txt
```
(from the `grafana_tokens` named volume created by InfluxDB)  
and exports it as `INFLUX_GRAFANA_TOKEN`.

Grafana mounts:
```
./data/grafana/dashboards     → /etc/grafana/dashboards
./data/grafana/provisioning   → /etc/grafana/provisioning
```
allowing automatic provisioning of test dashboards and datasource definitions at startup.

### **feeder_loader**
A Python-based container used to simulate the feeder.  
It executes the script:

```
./feeder_loader/scripts/Feeder_simulator.py
```

→ Link: [`feeder_loader/scripts/Feeder_simulator.py`](feeder_loader/scripts/Feeder_simulator.py)

The script publishes example messages to the Kafka topic defined in `.env` under:
```
KAFKA_INPUT_TOPICS
```
(which **must be a list**, even if containing a single topic).

The feeder messages are loaded from:
```
./feeder_loader/scripts/input/${FEEDER_INPUT_FILE}
```

→ Directory: [`feeder_loader/scripts/input`](feeder_loader/scripts/input)

where `FEEDER_INPUT_FILE` is set in `.env`.

### **app_processor**
A Python-based container responsible for consuming, parsing, and re-publishing feeder messages.  
It executes:

```
./app/scripts/feeder-monitoring-converter.py
```

→ Link: [`app/scripts/feeder-monitoring-converter.py`](app/scripts/feeder-monitoring-converter.py)

This component:
- consumes messages from the Kafka topic listed in `KAFKA_INPUT_TOPICS` (defined in the `.env` file)
- parses each message according to the `OUTPUT_FORMAT` specified in `.env`  
  - supported formats: `influx`, `json_v2`  
  - **Telegraf currently supports only `influx`**
- publishes the processed output to the topic specified in `.env` under:
```
KAFKA_OUTPUT_TOPIC
```

The parsing logic, message schemas, and LP/JSON serialization are implemented in:

→ [`app/scripts/modules/feedermonitoring`](app/scripts/modules/feedermonitoring)

Kafka client configuration is provided by the `MonitoringConfig` class, whose values are dynamically loaded from the `.env` file.

---


## Workflow Overview

This section describes the execution flow of the monitoring pipeline once the environment is started.

1. **Kafka and InfluxDB** start first and expose health checks.  
   All other services wait for these checks to pass.

2. **feeder_loader** publishes the sample feeder messages into the input Kafka topic and then exits.

3. **app_processor** consumes the messages in streaming, converts them according to the selected `OUTPUT_FORMAT`, and writes the processed output to the second Kafka topic.

4. **Telegraf** consumes the processed messages from Kafka and writes them into InfluxDB.

5. **Grafana** reads from InfluxDB and visualizes the ingested metrics through the pre-provisioned dashboards.

---

## Setup Instructions

Complete the following steps before launching the environment.

### **1. Prepare the `.env` file**

Copy the template:

```bash
cp .env.example .env
```

The only parameter that **must** be modified is:

```
FEEDER_INPUT_FILE=<name_of_test_json_file>
```

The corresponding JSON file (containing sample messages produced by the Feeder) must be placed inside:

[`feeder_loader/scripts/input`](feeder_loader/scripts/input)

All other variables can remain at their default values and the test environment will still run correctly.  
However, the default credentials for InfluxDB and Grafana defined in `docker-compose.yml` should be changed before real usage.

### **2. Build the images (first run only)**

```bash
docker compose build
```

This step is required because the **influxdb**, **telegraf**, and **grafana** services rely on custom Dockerfiles and may read configuration from `.env` during build.

---

## Running the Environment

Start the environment with:

```bash
docker compose up -d
```

Check that all containers are running:

```bash
docker ps -a
```

### **Container behavior**

- **feeder_loader**  
  Runs once. It publishes all sample messages to the input Kafka topic and then exits automatically. This is the expected behavior.

- **app_processor**  
  Remains active. It listens to the input topic, processes messages as soon as they appear, converts them according to `OUTPUT_FORMAT`, and writes the parsed output to the output topic.

- **Telegraf**  
  Starts when Kafka and InfluxDB are healthy. It reads its token from the named volume, loads its configuration, and begins ingesting processed messages into InfluxDB.

- **Grafana**  
  Starts once InfluxDB is healthy. It loads the datasource and dashboards automatically and becomes available at:  
  **http://localhost:3000**

> **Note:** Depending on provisioning timing and the arrival of the first processed data points, Grafana dashboards may take a few minutes before displaying metrics.

### **Optional: Kafka UI**

You may uncomment the `kafka-ui` service in `docker-compose.yml` to inspect Kafka topics through a web interface, useful for verifying that:
- the input topic receives feeder messages  
- the output topic receives processed messages  

### **Verifying data flow**

#### **InfluxDB**
Open:

```
http://localhost:8086
```

Check that the bucket defined in `.env`:

```
DOCKER_INFLUXDB_INIT_BUCKET
```

is created and receiving data.

#### **Grafana**
Open:

```
http://localhost:3000
```

(Default credentials: `admin` / `admin`)

Dashboards in  
[`data/grafana/dashboards`](data/grafana/dashboards)  
should be imported automatically.

### **Inspecting logs**

Use:

```bash
docker compose logs <container_name>
```

Useful containers to inspect:
- `feeder_loader` → publishing phase  
- `app_processor` → parsing and streaming  
- `telegraf` → ingestion into InfluxDB  
- `influxdb` → token generation and write operations  

---

## Notes

### **InfluxDB token lifecycle**
- The **admin token** is generated **once** during the InfluxDB image build and stored inside the container at `/generated_admin_token`.  
  It does **not** change across container restarts.
- The **Grafana** and **Telegraf** authentication tokens are generated by InfluxDB **every time the container starts**.  
  They are written into the named volumes:
  - `grafana_tokens` → `/tokens/grafana/grafana_token.txt`
  - `telegraf_tokens` → `/tokens/telegraf/telegraf_token.txt`
- Grafana and Telegraf load these tokens dynamically through their entrypoint scripts.

### **OUTPUT_FORMAT limitations**

The conversion of feeder messages is performed by the function  
`json_to_lines()` defined in:

[`app/scripts/modules/feedermonitoring/msg_v13_model.py`](app/scripts/modules/feedermonitoring/msg_v13_model.py)

This function supports two output formats:

- `influx`  
- `json_v2`

However:
- **Only `influx` is currently compatible with Telegraf ingestion**,  
  because Telegraf cannot reliably parse the JSON structure produced by the `json_v2` format.

For this reason, `OUTPUT_FORMAT=influx` is the recommended and default choice.

### **Kafka topic auto-creation**
The Kafka broker automatically creates topics when a producer or a consumer connects to a non-existing topic (`KAFKA_AUTO_CREATE_TOPICS_ENABLE=true`).  
This simplifies local testing but should be disabled in production environments.

### **Grafana dashboard provisioning**
Dashboard and datasource provisioning happens only at container startup.  
Modifications made through the Grafana UI are **not persisted** unless written back to the mounted directories under:

```
data/grafana/dashboards
data/grafana/provisioning
```

### **Delay before first data appears**
Due to initialization timing, provisioning, and ingestion latency, it is normal for Grafana dashboards to require **up to a few minutes** before displaying the first processed metrics.

