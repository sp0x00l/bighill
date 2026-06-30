#! /usr/bin/env zsh

echo

if [ -d "$(brew --prefix)/var/postgresql@14" ]; then
    echo "PostgreSQL 14 already installed"
else
    echo "Installing PostgreSQL 14"
    brew install postgresql@14

    ln -sfv $(brew --prefix)/opt/postgresql@14/*.plist ~/Library/LaunchAgents

    echo "Adding start stop aliases to .zshrc"
    echo "" >> ~/.zshrc
    echo "alias pg_start=\"launchctl load ~/Library/LaunchAgents/homebrew.mxcl.postgresql@14.plist\"" >> ~/.zshrc
    echo "alias pg_stop=\"launchctl unload ~/Library/LaunchAgents/homebrew.mxcl.postgresql@14.plist\"" >> ~/.zshrc
    echo "alias pg_restart=\"pg_stop && pg_start\"" >> ~/.zshrc

    . ~/.zshrc
fi

if [ -d "/Applications/pgAdmin 4.app" ]; then
    echo "pgAdmin 4 already installed"
else
    echo "Installing pgAdmin 4"
    brew install --cask pgadmin4
fi

pg_start 



