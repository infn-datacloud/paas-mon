ARG PYTHON_VERSION=3.12
ARG POETRY_VERSION=2.1

FROM harbor.cloud.infn.it/datacloud-middleware/poetry:${POETRY_VERSION}-python${PYTHON_VERSION} AS base

WORKDIR /app

COPY ./pyproject.toml ./poetry.lock ./README.md /app/
COPY ./kafka-components/processors /app/

RUN poetry install --without dev

USER ${USERNAME}