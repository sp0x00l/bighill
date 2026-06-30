#! /usr/bin/env sh

if ! brew services list | grep -q kafka.plist; then
    echo "kafka is not running, starting kafka"
    brew services start kafka
else
    echo "kafka is running"
    brew services restart kafka
fi

sleep 5

