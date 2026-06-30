FROM ubuntu:22.04 AS downloader

RUN apt-get update && \
    apt-get install -y wget tar && \
    rm -rf /var/lib/apt/lists/*

RUN mkdir /tmp/sakila/
RUN wget -O /tmp/sakila-db.tar.gz https://downloads.mysql.com/docs/sakila-db.tar.gz
RUN tar -xzf /tmp/sakila-db.tar.gz -C /tmp/sakila/
    
FROM mysql:latest
LABEL bighill="data"

COPY --from=downloader /tmp/sakila /tmp/sakila
