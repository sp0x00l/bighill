FROM ubuntu:22.04 AS downloader

RUN apt-get update && \
    apt-get install -y wget unzip && \
    rm -rf /var/lib/apt/lists/*

RUN mkdir /tmp/pagila/    
RUN wget -O /tmp/pagila/pagila-schema.sql https://raw.githubusercontent.com/devrimgunduz/pagila/master/pagila-schema.sql
RUN wget -O /tmp/pagila/pagila-data.sql https://raw.githubusercontent.com/devrimgunduz/pagila/master/pagila-data.sql

FROM postgres:latest
LABEL bighill="data"

COPY --from=downloader /tmp/pagila /tmp/pagila
