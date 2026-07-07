FROM migrate/migrate:latest AS migrate
LABEL bighill="services"

WORKDIR /app/migrations/
COPY ./build/tmp/db-migrations .

WORKDIR /usr/local/src
COPY ./scripts/entry-point/migrate-db-entrypoint.sh .
RUN apk update && apk add --no-cache bash postgresql-client && rm -rf /var/cache/apk/*

ENTRYPOINT ["sh", "/usr/local/src/migrate-db-entrypoint.sh"]
