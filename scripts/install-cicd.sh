#! /usr/bin/env sh

set -eu

# This script provisions the dependencies needed for CI and for running `make test` on Linux.
# It intentionally avoids macOS specific tooling used by install-dev.

check_prerequisites() {
    if [ "$(uname)" != "Linux" ]; then
        echo "install-cicd is intended for Linux runners; skipping."
        exit 0
    fi

    if ! command -v sudo >/dev/null 2>&1; then
        echo "sudo is required to install CI dependencies."
        exit 1
    fi

    if ! command -v apt-get >/dev/null 2>&1; then
        echo "apt-get not found; install the required packages manually."
        exit 1
    fi
}

install_system_packages() {
    echo "Updating apt repositories..."
    sudo apt-get update -y

    echo "Installing system packages..."
    sudo DEBIAN_FRONTEND=noninteractive apt-get install -y \
        build-essential \
        cmake \
        curl \
        git \
        lsof \
        netcat-openbsd \
        pkg-config \
        python3 \
        python3-pip \
        redis-server \
        redis-tools \
        postgresql \
        postgresql-contrib \
        libpq-dev \
        protobuf-compiler \
        openjdk-17-jre-headless \
        jq \
        zip \
        unzip
}

install_docker() {
    if command -v docker >/dev/null 2>&1; then
        return 0
    fi

    echo "Installing Docker..."
    sudo DEBIAN_FRONTEND=noninteractive apt-get install -y docker.io
    sudo systemctl start docker 2>/dev/null || sudo service docker start 2>/dev/null || true
    sudo usermod -aG docker "$USER" 2>/dev/null || true
}

install_docker_compose() {
    if docker compose version >/dev/null 2>&1; then
        return 0
    fi

    echo "Installing docker compose plugin via apt..."
    sudo DEBIAN_FRONTEND=noninteractive apt-get install -y docker-compose-plugin || true

    if docker compose version >/dev/null 2>&1; then
        return 0
    fi

    install_docker_compose_standalone
}

install_docker_compose_standalone() {
    
    local COMPOSE_ARCH
    local COMPOSE_VERSION="v2.24.5"
    local MAX_RETRIES=5
    local RETRY_COUNT=0

    echo "Installing docker-compose standalone..."
    local ARCH="$(uname -m)"
    case "$ARCH" in
        aarch64|arm64) COMPOSE_ARCH="aarch64" ;;
        x86_64|amd64) COMPOSE_ARCH="x86_64" ;;
        *) COMPOSE_ARCH="aarch64" ;;
    esac

    local COMPOSE_URL="https://github.com/docker/compose/releases/download/${COMPOSE_VERSION}/docker-compose-linux-${COMPOSE_ARCH}"

    while [ $RETRY_COUNT -lt $MAX_RETRIES ]; do
        if sudo curl -fsSL --retry 3 --retry-delay 5 "$COMPOSE_URL" -o /usr/local/bin/docker-compose; then
            sudo chmod +x /usr/local/bin/docker-compose
            sudo mkdir -p /usr/local/lib/docker/cli-plugins
            sudo ln -sf /usr/local/bin/docker-compose /usr/local/lib/docker/cli-plugins/docker-compose
            return 0
        fi
        RETRY_COUNT=$((RETRY_COUNT + 1))
        echo "docker-compose download failed, retry $RETRY_COUNT of $MAX_RETRIES..."
        sleep 10
    done

    echo "Warning: Failed to download docker-compose after $MAX_RETRIES attempts."
    echo "Attempting fallback: using docker-compose from pip..."
    python3 -m pip install --upgrade --user --break-system-packages docker-compose || {
        echo "Failed to install docker-compose via pip."
        exit 1
    }
}

install_go() {
    if command -v go >/dev/null 2>&1; then
        return 0
    fi

    local GO_VERSION="1.24.3"
    local ARCH
    local GO_ARCH
    local GO_TARBALL

    echo "Installing Go toolchain..."
    ARCH="$(uname -m)"
    case "$ARCH" in
        aarch64|arm64) GO_ARCH="arm64" ;;
        x86_64|amd64) GO_ARCH="amd64" ;;
        *) GO_ARCH="arm64" ;;
    esac

    GO_TARBALL="go${GO_VERSION}.linux-${GO_ARCH}.tar.gz"
    curl -fsSL "https://go.dev/dl/${GO_TARBALL}" -o "/tmp/${GO_TARBALL}"
    if [ -e /usr/local/go ]; then
        sudo rm -rf /usr/local/go
    fi
    sudo tar -C /usr/local -xzf "/tmp/${GO_TARBALL}"
    rm -f "/tmp/${GO_TARBALL}"
    export PATH="/usr/local/go/bin:$PATH"
}

install_go_protobuf_plugins() {
    if ! command -v protoc >/dev/null 2>&1; then
        echo "protobuf-compiler was not installed correctly."
        exit 1
    fi

    echo "Installing Go protobuf plugins..."
    export GOPATH="${GOPATH:-$HOME/go}"
    export PATH="$GOPATH/bin:/usr/local/go/bin:$PATH"

    go install google.golang.org/protobuf/cmd/protoc-gen-go@latest
    go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@latest

    if [ -n "${GITHUB_PATH:-}" ]; then
        echo "$GOPATH/bin" >> "$GITHUB_PATH"
    fi
}

install_python_tooling() {
    echo "Installing Python tooling..."
    # PEP 668: Python 3.12+ on Debian requires --break-system-packages for system-wide pip installs
    python3 -m pip install --upgrade --user --break-system-packages pip
    python3 -m pip install --upgrade --user --break-system-packages aws-sam-cli grpcio-tools
}

install_aws_cli() {
    if command -v aws >/dev/null 2>&1; then
        return 0
    fi

    local ARCH
    local AWS_ARCH

    echo "Installing AWS CLI..."
    ARCH="$(uname -m)"
    case "$ARCH" in
        aarch64|arm64) AWS_ARCH="aarch64" ;;
        x86_64|amd64) AWS_ARCH="x86_64" ;;
        *) AWS_ARCH="aarch64" ;;
    esac

    curl -fsSL "https://awscli.amazonaws.com/awscli-exe-linux-${AWS_ARCH}.zip" -o /tmp/awscliv2.zip
    cd /tmp && unzip -q -o awscliv2.zip
    sudo /tmp/aws/install --update
    rm -rf /tmp/aws /tmp/awscliv2.zip
}

install_open_tofu() {
    if command -v tofu >/dev/null 2>&1; then
        return 0
    fi

    local ARCH
    local TOFU_ARCH
    local TOFU_VERSION="1.10.7"

    echo "Installing OpenTofu..."
    ARCH="$(uname -m)"
    case "$ARCH" in
        aarch64|arm64) TOFU_ARCH="arm64" ;;
        x86_64|amd64) TOFU_ARCH="amd64" ;;
        *) TOFU_ARCH="amd64" ;;
    esac

    local ZIP="tofu_${TOFU_VERSION}_linux_${TOFU_ARCH}.zip"
    local URL="https://github.com/opentofu/opentofu/releases/download/v${TOFU_VERSION}/${ZIP}"
    curl -fsSL "$URL" -o "/tmp/${ZIP}"
    cd /tmp && unzip -q -o "${ZIP}"
    sudo install -m 0755 /tmp/tofu /usr/local/bin/tofu
    rm -f "/tmp/${ZIP}" /tmp/tofu
}

install_yq() {
    local ARCH
    local YQ_ARCH
    local YQ_URL
    local MAX_RETRIES=3
    local RETRY_COUNT=0

    echo "Installing yq..."
    ARCH="$(uname -m)"
    if [ "$ARCH" = "aarch64" ] || [ "$ARCH" = "arm64" ]; then
        YQ_ARCH="arm64"
    else
        YQ_ARCH="amd64"
    fi

    YQ_URL="https://github.com/mikefarah/yq/releases/latest/download/yq_linux_${YQ_ARCH}"

    while [ $RETRY_COUNT -lt $MAX_RETRIES ]; do
        if sudo curl -fsSL --retry 3 --retry-delay 5 "$YQ_URL" -o /usr/local/bin/yq; then
            sudo chmod +x /usr/local/bin/yq
            return 0
        fi
        RETRY_COUNT=$((RETRY_COUNT + 1))
        echo "yq download failed, retry $RETRY_COUNT of $MAX_RETRIES..."
        sleep 5
    done

    echo "Warning: Failed to download yq after $MAX_RETRIES attempts, continuing..."
}

configure_path() {
    local LOCAL_BIN="$HOME/.local/bin"

    mkdir -p "$LOCAL_BIN"
    case ":$PATH:" in
        *":$LOCAL_BIN:"*) ;;
        *) export PATH="$LOCAL_BIN:$PATH" ;;
    esac

    if [ -n "${GITHUB_PATH:-}" ]; then
        echo "$LOCAL_BIN" >> "$GITHUB_PATH"
        echo "/usr/local/go/bin" >> "$GITHUB_PATH"
    fi
}

check_prerequisites
install_system_packages
install_docker
install_docker_compose
install_go
install_go_protobuf_plugins
install_python_tooling
install_aws_cli
install_open_tofu
install_yq
configure_path
