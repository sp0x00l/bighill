FROM ubuntu:22.04 AS downloader

RUN apt-get update && \
    apt-get install -y wget unzip && \
    rm -rf /var/lib/apt/lists/*

RUN mkdir /tmp/movies/    
RUN wget -O /tmp/movies/movies.json https://raw.githubusercontent.com/prust/wikipedia-movie-data/master/movies.json

FROM mongo:latest
LABEL bighill="data"

COPY --from=downloader /tmp/movies /tmp/movies
