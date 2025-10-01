ARG PYTHON_VERSION=3.10
ARG POETRY_VERSION=1.8.3

# Create requirements.txt from poetry dependencies
FROM ghcr.io/withlogicco/poetry:${POETRY_VERSION}-python-${PYTHON_VERSION}-slim AS requirements

WORKDIR /tmp

COPY ./pyproject.toml ./poetry.lock* /tmp/

RUN poetry export \
    -f requirements.txt \
    --output requirements.txt \
    --without-hashes \
    --without dev
    
# Build the final image
FROM python:${PYTHON_VERSION}-slim AS production

COPY --from=requirements /tmp/requirements.txt /requirements.txt

# Upgrade pip and install requirements
RUN pip install --user --upgrade pip && \
    pip install --user --no-cache-dir --upgrade -r /requirements.txt

COPY ./kafka-components/src /kafka-components/

WORKDIR /kafka-components
