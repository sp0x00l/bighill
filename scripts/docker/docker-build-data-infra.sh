#! /usr/bin/env sh

build_data_infra()
{
    echo "building data infra"
    local CURRENT_DIR=$(pwd)
    local BIGHILL_ROOT=$(git rev-parse --show-toplevel)
    cd $BIGHILL_ROOT/scripts/docker/data_infra

    local DOCKERFILES=($(ls -1 *.Dockerfile))
    for DOCKERFILE in ${DOCKERFILES[@]}; do
        docker build --no-cache -f $DOCKERFILE .
    done

    docker pull projectnessie/nessie:latest
    docker pull minio/minio:latest

    cd $CURRENT_DIR
}

build_data_infra
