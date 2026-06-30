#! /usr/bin/env sh

# note: requires docker, db and services to be running

# watchexec --exts go --watch . -watch template.yml --restart 'clear; ./scripts/stop.sh; ./scripts/build.sh; ./scripts/run.sh;'