#! /usr/bin/env sh

docker rmi -f $(docker images -q -f "label=bighill=services")
docker rmi -f $(docker images -q -f "label=bighill=data")

docker system prune -a
docker volume prune -f
docker network prune -f
