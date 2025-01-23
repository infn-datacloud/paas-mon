FROM python:3.10

RUN python3 -m pip install \
    kafka-python \
    pandas \
    pyyaml 

COPY ./src /kafka-components/

WORKDIR /kafka-components