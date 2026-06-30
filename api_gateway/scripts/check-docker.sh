#! /usr/bin/env sh

echo "Checking if docker is running..."

curl -s --unix-socket /var/run/docker.sock http/_ping 2>&1 >/dev/null
if [ ! $? -eq 0 ]; then
    echo "docker is not running. Please start docker and try again."
    exit 1
fi  

echo "docker is running."
