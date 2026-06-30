#! /usr/bin/env sh

echo "Setting up Pagila sample datadabase. https://github.com/devrimgunduz/pagila"

psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" -d pagila -f /tmp/pagila/pagila-schema.sql
psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" -d pagila -f /tmp/pagila/pagila-data.sql

echo "Sample data imported."
