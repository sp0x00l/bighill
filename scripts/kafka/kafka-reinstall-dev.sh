#! /usr/bin/env zsh

# set -e

if [[ "$OSTYPE" == "darwin"* ]]; then
    echo "installing dev env on MacOS"
else
    echo "command install-dev only supports MacOS"
    exit
fi   

install_kafka()
{
    if brew list kafka &>/dev/null; then
        echo "kafka already installed"
    else
        echo "installing kafka"
        brew install kafka
        # this creates the etc/kafka/server.properties
        # and var/lib/kraft-combined-logs
        mv $(brew --prefix)/etc/kafka/kraft/server.properties $(brew --prefix)/etc/kafka/server.properties
        kafka-storage format -t $(kafka-storage random-uuid) -c $(brew --prefix)/etc/kafka/server.properties
    fi
}

uninstall_kafka()
{
    brew services stop kafka
    brew uninstall kafka
    local BREW_PREFIX=$(brew --prefix)
    [ -e "$BREW_PREFIX/var/lib/kafka" ] && rm -rf "$BREW_PREFIX/var/lib/kafka"
    [ -e "$BREW_PREFIX/etc/kafka" ] && rm -rf "$BREW_PREFIX/etc/kafka"
    [ -e "$BREW_PREFIX/var/lib/kraft-combined-logs" ] && rm -rf "$BREW_PREFIX/var/lib/kraft-combined-logs"
    [ -e "$BREW_PREFIX/var/log/kafka" ] && rm -rf "$BREW_PREFIX/var/log/kafka"

    brew cleanup
}

uninstall_kafka
install_kafka
