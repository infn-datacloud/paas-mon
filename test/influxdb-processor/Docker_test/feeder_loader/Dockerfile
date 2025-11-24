# feeder_loader/Dockerfile
ARG PYTHON_VERSION=3.10
ARG POETRY_VERSION=1.8.3

FROM ghcr.io/withlogicco/poetry:${POETRY_VERSION}-python-${PYTHON_VERSION}-slim AS requirements
WORKDIR /tmp

COPY ./pyproject.toml ./poetry.lock* /tmp/

RUN poetry export -f requirements.txt --output requirements.txt --without-hashes --without dev

FROM python:${PYTHON_VERSION}-slim AS production

# bash required
RUN apt-get update && apt-get install -y --no-install-recommends \
      bash ca-certificates && \
    rm -rf /var/lib/apt/lists/*

COPY --from=requirements /tmp/requirements.txt /requirements.txt
RUN python -m pip install --upgrade pip && \
    pip install --user --no-cache-dir -r /requirements.txt

WORKDIR /feeder_loader
COPY ./test/influxdb-processor/Docker_test/feeder_loader/scripts /feeder_loader/scripts

COPY ./test/influxdb-processor/Docker_test/feeder_loader/start-feeder.sh /usr/local/bin/start-feeder.sh
RUN chmod +x /usr/local/bin/start-feeder.sh 

ENTRYPOINT ["/usr/local/bin/start-feeder.sh"]



