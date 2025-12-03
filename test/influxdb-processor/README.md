# InfluxDB Processor Test Environment

This directory contains the code and test setup for validating the data-processing
pipeline that converts JSON messages into InfluxDB Line Protocol and streams them
through Kafka into an InfluxDB + Grafana monitoring stack.

Feeder → Kafka → Processor → Kafka → Telegraf → InfluxDB → Grafana

## Structure

- **processor/** — contains the actual processing logic. 
  It defines the core classes and the `json_to_lines()` function responsible for
  converting incoming JSON messages into InfluxDB Line Protocol format.

- **Docker_test/** — provides a complete Docker-based environment to test the
  processor in a realistic pipeline. 
  It includes Kafka, InfluxDB, Telegraf, Grafana, and a Python container running
  the Feeder and Processor scripts.

## Purpose

This environment is designed to:
- validate the correctness of the JSON → Line Protocol transformation;
- test message flow across Kafka topics and Telegraf ingestion;
- verify data persistence and visualization in InfluxDB and Grafana.

## How to Run

See [Docker_test/README.md](Docker_test/README.md) for detailed setup and
execution instructions.

