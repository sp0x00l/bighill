#! /usr/bin/env bash

set -euo pipefail

echo "Protobuf generation"

BUILD_FOR="${1:-go}"

build()
{
    local CURRENT_DIR=$(pwd)
    local SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
    local PROJECT_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"
    local DATA_CONTRACT_ROOT=$PROJECT_ROOT/data_contracts
    cd $DATA_CONTRACT_ROOT

    if [ ! -f "$DATA_CONTRACT_ROOT/build/protobufs/go.mod" ]; then
        echo "data_contracts not installed. Run 'make install' first." >&2
        exit 1
    fi

    find "$DATA_CONTRACT_ROOT/build/protobufs" -mindepth 1 -maxdepth 1 -type d ! -name ".gocache" -exec rm -rf {} +

    for file in $(find ./protobufs -name "*.proto"); do
        echo "Processing $file"
        echo "Building for go"
        file_name=$(echo $file | rev | cut -c 7- | rev)
        mkdir -p tmp/build/$file_name
        mkdir -p build/$file_name
        cp $file tmp/build/$file_name
        cd tmp
        protoc --proto_path=build/$file_name/ \
            --go_out=$DATA_CONTRACT_ROOT/build/$file_name \
            --go-grpc_out=$DATA_CONTRACT_ROOT/build/$file_name \
            --go_opt=paths=source_relative \
            --go-grpc_opt=paths=source_relative \
            build/$file_name/*.proto
        
        if [ "$BUILD_FOR" = "python" ]; then
            echo "Building for Python"
            cd $DATA_CONTRACT_ROOT
            mkdir -p tmp/py/build/$file_name
            cp $file tmp/py/build/$file_name
            
            cd tmp/py
            python -m grpc_tools.protoc \
                -I. --python_out=. \
                --grpc_python_out=. \
                build/$file_name/*.proto 
        else 
            echo "Not building for Python"
        fi
        
        cd $DATA_CONTRACT_ROOT
    done

    if [ "$BUILD_FOR" = "python" ]; then
        if [ -e "$DATA_CONTRACT_ROOT/build/python" ]; then
            rm -rf "$DATA_CONTRACT_ROOT/build/python"
        fi
        mkdir -p $DATA_CONTRACT_ROOT/build
        mv $DATA_CONTRACT_ROOT/tmp/py/build $DATA_CONTRACT_ROOT/build/python
    fi
    
    cd $DATA_CONTRACT_ROOT
    if [ -e "$DATA_CONTRACT_ROOT/tmp" ]; then
        rm -rf "$DATA_CONTRACT_ROOT/tmp"
    fi
    
    cd $CURRENT_DIR
}

build 

echo "done" 
