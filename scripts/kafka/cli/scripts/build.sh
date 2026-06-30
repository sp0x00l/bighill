#! /usr/bin/env sh

if [ ! -d "build" ]; then
  mkdir -p build
fi


rm go.mod 2> /dev/null
go mod init kafka-cli
go mod tidy

go build -v -o build/kafka-cli 
