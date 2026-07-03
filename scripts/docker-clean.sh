#! /usr/bin/env sh

# docker rmi -f $(docker images -q -f "label=bighill=services")

docker rmi -f $(docker images -q -f "label=service="event_service"")

docker system prune -a
docker volume prune -f
docker network prune -f
