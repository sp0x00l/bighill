#! /usr/bin/env sh

echo "Setting up Sakila sample datadabase. https://dev.mysql.com/doc/sakila/en/"

mysql -u root -p"$MYSQL_ROOT_PASSWORD" sakila < /tmp/sakila/sakila-db/sakila-schema.sql
mysql -u root -p"$MYSQL_ROOT_PASSWORD" sakila < /tmp/sakila/sakila-db/sakila-data.sql

echo "Sample data imported."
