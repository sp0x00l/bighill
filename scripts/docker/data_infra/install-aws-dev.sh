#! /usr/bin/env sh

oracle_container_service_login() {
    export USER_NAME=$1
    export USER_PASSWORD=$2
/usr/bin/expect << 'EOF1'
    spawn /bin/bash -c "docker login container-registry.oracle.com"
    
    set timeout -1
    expect {
        -re {Username:} { send $env(USER_NAME)'\r'; exp_continue }
        -re {Password:} { send $env(USER_PASSWORD)'\r'; exp_continue }
        eof
    }
EOF1
}

if [ -z "$ORACLE_CONTAINER_SERVICE_USER" ] || [ -z "$ORACLE_CONTAINER_SERVICE_PASSWORD" ]; then
    echo "ORACLE_CONTAINER_SERVICE_USER and ORACLE_CONTAINER_SERVICE_PASSWORD must be set"
    echo "Create an account at https://container-registry.oracle.com/ and get your login credentials"
    exit 1
fi

oracle_container_service_login $ORACLE_CONTAINER_SERVICE_USER $ORACLE_CONTAINER_SERVICE_PASSWORD
docker pull container-registry.oracle.com/database/express:latest

