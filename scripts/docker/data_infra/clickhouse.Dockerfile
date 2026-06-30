FROM ubuntu:22.04 AS downloader

RUN apt-get update && \
    apt-get install -y wget jq && \
    rm -rf /var/lib/apt/lists/*

RUN mkdir /tmp/movies/
RUN wget -O /tmp/movies/movies.json https://raw.githubusercontent.com/prust/wikipedia-movie-data/master/movies.json
RUN jq -c '.[] | {title: (.title // ""), release_year: (.year // 0), cast_members: (.cast // []), genres: (.genres // []), href: (.href // null), extract: (.extract // null)}' /tmp/movies/movies.json > /tmp/movies/movies.jsonl

FROM clickhouse/clickhouse-server:latest
LABEL bighill="data"

COPY --from=downloader /tmp/movies /tmp/movies
