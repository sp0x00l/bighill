#! /usr/bin/env sh

echo "Setting up wikipedia movies sample datadabase. https://raw.githubusercontent.com/prust/wikipedia-movie-data/master/movies.json"
until mongosh --host localhost --port 27017 --eval 'db.runCommand({ ping: 1 })' >/dev/null 2>&1; do
    sleep 2
done

echo "MongoDB started."
mongoimport --host localhost --port 27017 \
  --username root \
  --password example \
  --authenticationDatabase admin \
  --db sample_db \
  --collection movies \
  --file /tmp/movies/movies.json \
  --jsonArray

echo "Sample data imported."