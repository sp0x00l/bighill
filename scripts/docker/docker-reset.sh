#! /usr/bin/env sh

reset_docker_containers() 
{
    local SERVICE_CONTAINERS=$(docker container ls -a -q -f "label=bighill=services")
    if [ -n "$SERVICE_CONTAINERS" ]; then
        docker container rm $SERVICE_CONTAINERS
    else
        echo "No service containers found"
    fi
   
    local DATA_CONTAINERS=$(docker container ls -a -q -f "label=bighill=data")
    if [ -n "$DATA_CONTAINERS" ]; then
        docker container rm $DATA_CONTAINERS
    else
        echo "No data containers found"
    fi
}

reset_docker_containers
