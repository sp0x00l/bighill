#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck disable=SC1091
source "${SCRIPT_DIR}/common.sh"
PROJECT_ROOT="$(get_project_root)"

gather_db_migrations() {
    echo "Gathering db migrations..."
    
    # Clean previous migrations
    rm -rf "$PROJECT_ROOT/build/tmp/db-migrations"
    mkdir -p "$PROJECT_ROOT/build/tmp/db-migrations"
    
    # Find all service directories
    local SERVICE_DIRS=$(find "$PROJECT_ROOT" -maxdepth 1 -type d -name "*_service" | sort)
    
    for SERVICE_DIR in $SERVICE_DIRS; do
        local SERVICE_NAME=$(basename "$SERVICE_DIR")
        
        # Check if service has migrations
        if [ -d "$SERVICE_DIR/db/migrations" ]; then
            local DB_NAME=$("$PROJECT_ROOT/database/scripts/db-name.sh" "$SERVICE_NAME" 2>/dev/null || echo "")
            
            if [ -z "$DB_NAME" ]; then
                echo "  Skipping $SERVICE_NAME - no database configured"
            else
                echo "  Copying migrations for $DB_NAME from $SERVICE_NAME"
                mkdir -p "$PROJECT_ROOT/build/tmp/db-migrations/$DB_NAME"
                if compgen -G "$SERVICE_DIR/db/migrations/*" > /dev/null; then
                    cp -r "$SERVICE_DIR/db/migrations/"* "$PROJECT_ROOT/build/tmp/db-migrations/$DB_NAME/"
                else
                    echo "    No migration files found in $SERVICE_NAME"
                fi
            fi
        fi
    done
    
    # Ensure directory is not empty (Docker COPY fails on empty dirs)
    if [ -z "$(ls -A "$PROJECT_ROOT/build/tmp/db-migrations" 2>/dev/null)" ]; then
        echo "  No migrations found, creating placeholder"
        echo "# No migrations" > "$PROJECT_ROOT/build/tmp/db-migrations/.keep"
    fi
    
    echo "Migration files gathered in $PROJECT_ROOT/build/tmp/db-migrations/"
    ls -la "$PROJECT_ROOT/build/tmp/db-migrations/"
}

gather_db_migrations "$@"
