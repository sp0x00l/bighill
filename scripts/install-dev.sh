#! /usr/bin/env zsh
set -euo pipefail

if [[ "$OSTYPE" == "darwin"* ]]; then
    echo "installing dev env on MacOS"
else
    echo "command install-dev only supports MacOS"
    exit 1
fi

ROOT_DIR="$(pwd)"
PYTHON_VERSION="3.11.9"

is_installed()
{
    command -v "$1" >/dev/null 2>&1
}

install_brew()
{
    if is_installed brew; then
        echo "brew already installed"
    else
        echo "installing brew"
        /bin/bash -c "$(curl -fsSL https://raw.githubusercontent.com/Homebrew/install/HEAD/install.sh)"
    fi
}

install()
{
    if is_installed "$1"; then
        echo "$1 already installed"
    else
        echo "installing $1"
        brew install "$1"
    fi
}

install_brew_formula()
{
    if brew list "$1" >/dev/null 2>&1; then
        echo "$1 already installed"
    else
        echo "installing $1"
        brew install "$1"
    fi
}

install_docker()
{
    if [ -d "/Applications/Docker.app" ]; then
        echo "docker already installed"
    else
        echo "installing docker"
        brew install --cask docker
    fi
}

install_postgres()
{
    install_brew_formula postgresql@17
}

link_postgres_launch_agent()
{
    local BREW_PREFIX
    local LAUNCH_AGENTS_DIR

    BREW_PREFIX="$(brew --prefix)"
    LAUNCH_AGENTS_DIR="$HOME/Library/LaunchAgents"

    mkdir -p "$LAUNCH_AGENTS_DIR"
    ln -sfv "$BREW_PREFIX"/opt/postgresql@17/*.plist "$LAUNCH_AGENTS_DIR"
}

install_pgvector()
{
    install_brew_formula pgvector
}

install_pgadmin()
{
    if [ -d "/Applications/pgAdmin 4.app" ]; then
        echo "pgAdmin 4 already installed"
    else
        echo "installing pgAdmin 4"
        brew install --cask pgadmin4
    fi
}

install_cpp_pdf_dependencies()
{
    install_brew_formula cmake
    install_brew_formula pkg-config
    install_brew_formula poppler
}

add_zshrc_line()
{
    local LINE="$1"
    touch "$HOME/.zshrc"
    if ! grep -Fqx "$LINE" "$HOME/.zshrc"; then
        echo "$LINE" >> "$HOME/.zshrc"
    fi
}

install_python()
{
    if is_installed pyenv; then
        echo "pyenv already installed"
    else
        echo "installing pyenv"
        brew install pyenv
    fi

    add_zshrc_line ""
    add_zshrc_line "# Pyenv"
    add_zshrc_line 'export PYENV_ROOT="$HOME/.pyenv"'
    add_zshrc_line 'command -v pyenv >/dev/null || export PATH="$PYENV_ROOT/bin:$PATH"'
    add_zshrc_line 'eval "$(pyenv init --path)"'
    add_zshrc_line 'eval "$(pyenv init -)"'

    export PYENV_ROOT="${PYENV_ROOT:-$HOME/.pyenv}"
    export PATH="$PYENV_ROOT/bin:$PYENV_ROOT/shims:$PATH"
    eval "$(pyenv init --path)"
    eval "$(pyenv init -)"

    if pyenv versions --bare | grep -Fxq "$PYTHON_VERSION"; then
        echo "Python ${PYTHON_VERSION} already installed via pyenv"
    else
        echo "installing Python ${PYTHON_VERSION} via pyenv"
        pyenv install "$PYTHON_VERSION"
    fi

    pyenv global "$PYTHON_VERSION"
    pyenv rehash
}

install_python_tooling()
{
    echo "installing Python developer tooling"
    pyenv exec python -m pip install --upgrade pip
    pyenv exec python -m pip install --upgrade aws-sam-cli grpcio-tools
    pyenv exec python -m pip install --upgrade -e "$ROOT_DIR/training_jobs[runtime]"
    pyenv rehash
}

install_go_tools()
{
    install go
    install golangci-lint

    echo "installing ginkgo"
    go install -mod=mod github.com/onsi/ginkgo/v2/ginkgo@latest
}

install_protobuf()
{
    if is_installed protoc; then
        echo "protobuf already installed"
    else
        echo "installing protobuf"
        brew install protobuf
    fi

    if is_installed protoc-gen-go; then
        echo "protoc-gen-go already installed"
    else
        echo "installing protoc-gen-go"
        go install google.golang.org/protobuf/cmd/protoc-gen-go@latest
    fi

    if is_installed protoc-gen-go-grpc; then
        echo "protoc-gen-go-grpc already installed"
    else
        echo "installing protoc-gen-go-grpc"
        go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@latest
    fi

    if is_installed protoc-gen-js; then
        echo "protoc-gen-js already installed"
    else
        echo "installing protoc-gen-js"
        brew install protoc-gen-js
    fi

    if is_installed protoc-gen-grpc-web; then
        echo "protoc-gen-grpc-web already installed"
    else
        echo "installing protoc-gen-grpc-web"
        brew install protoc-gen-grpc-web
    fi
}

install_rust()
{
    install rust

    if is_installed rustup; then
        echo "upgrading rust stable toolchain"
        rustup update stable
        rustup default stable
    else
        echo "rustup not found; upgrading Homebrew rust"
        brew upgrade rust || true
    fi
}

install_kafka()
{
    if brew list kafka >/dev/null 2>&1; then
        echo "kafka already installed"
        return
    fi

    echo "installing kafka"
    brew install kafka

    local KRAFT_CONFIG
    KRAFT_CONFIG="$(brew --prefix)/etc/kafka/kraft/server.properties"
    local SERVER_CONFIG
    SERVER_CONFIG="$(brew --prefix)/etc/kafka/server.properties"

    if [ -f "$KRAFT_CONFIG" ] && [ ! -f "$SERVER_CONFIG" ]; then
        mv "$KRAFT_CONFIG" "$SERVER_CONFIG"
    fi

    if [ -f "$SERVER_CONFIG" ]; then
        kafka-storage format -t "$(kafka-storage random-uuid)" -c "$SERVER_CONFIG" --ignore-formatted
    fi
}

install_temporal()
{
    if is_installed temporal; then
        echo "temporal already installed"
    else
        echo "installing temporal"
        brew install temporal
    fi
}

install_open_tofu()
{
    if is_installed tofu; then
        echo "open tofu already installed"
    else
        echo "installing open tofu"
        brew install opentofu
    fi
}

install_data_infra_dependencies()
{
    install_docker

    if docker compose version >/dev/null 2>&1; then
        echo "docker compose already installed"
    else
        echo "docker compose is required for cicd-style local infra."
        echo "Start Docker Desktop after installation and rerun make start-infra if needed."
    fi
}

install_ollama()
{
    install_brew_formula ollama
}

build_datafusion_query_engine()
{
    local QUERY_ENGINE_DIR
    QUERY_ENGINE_DIR="$ROOT_DIR/data_stream_service/internal/infra/queryengine"

    echo "building DataFusion query engine"
    make -C "$QUERY_ENGINE_DIR" build
}

install_brew
install_postgres
link_postgres_launch_agent
install_pgvector
install_pgadmin
install_cpp_pdf_dependencies
install_python
install_python_tooling
install_go_tools
install_protobuf
install_rust
build_datafusion_query_engine
install redis
install_kafka
install_temporal
install_data_infra_dependencies
install_ollama
install_open_tofu
install yq

echo ""
echo "Final steps:"
echo "  1. Run make install to generate module replacements and protobuf output."
echo "  2. Run make start-infra for Postgres with pgvector, Redis, Kafka, Temporal, Polaris, the local TEI-compatible embedding endpoint, and local data sources."
echo "  3. Run make start-test to start the local services and API gateway."
echo "  4. Run make build-query-engine to rebuild DATA_STREAM_SERVICE_QUERY_ENGINE_MODE=datafusion after Rust changes."
echo "  5. Start Ollama and pull the configured local model before using local inference generation: brew services start ollama && ollama pull llama3.1:8b."

cd "$ROOT_DIR"
