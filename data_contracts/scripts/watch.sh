#! /usr/bin/env zsh

watchexec -w scripts/build.sh -w ./*.proto --restart 'clear; ./scripts/build.sh'