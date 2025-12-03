# app/Dockerfile
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

WORKDIR /app
COPY ./test/influxdb-processor/Docker_test/app/processors /app/processors

COPY ./test/influxdb-processor/Docker_test/app/entrypoint.sh /usr/local/bin/entrypoint.sh
RUN chmod +x /usr/local/bin/entrypoint.sh 

ENTRYPOINT ["/usr/local/bin/entrypoint.sh"]



