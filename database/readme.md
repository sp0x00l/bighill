# Bighill Database

## Overview

This is the Bighill local development database setup.

## Install

* The `make install-dev` command will install PostgreSQL and the visual admin tool pgAdmin4.
* The development environment usernames and passwords can be found in `scripts/db-dev-config.sh`. You'll need these to create a connection to the database in pgAdmin4. See the note below on database naming.
* pgAdmin4 should be used to write and test SQL scripts.
* The install scripts also set up the standard `pg_start`, `pg_stop`, and `pg_restart` aliases. These are important to configure so that other PostgreSQL commands also work correctly (e.g., `pg_ctl` and `pg_isready`).

## Run

* `make db-setup` creates the databases, schemas, and roles.
* `make db-migrate` creates the tables, functions, and triggers. Migrate can be called with a `service name` parameter in order to migrate a single service. Without a `service name` parameter, it runs all migration scripts found in all root `<service>/db/migrations` directories.
* To migrate a particular service:

```bash
    # Example of running the migration on the data_registry_service
    make db-migrate SERVICE=data_registry_service
```

* `make db-truncate` wipes the database, and `make db-delete` will remove the database completely.

## Layout

* The scripts are divided into two parts.
* Within the `scripts` directory, you'll find the `make` utility scripts.
* Within the `scripts/setup` directory, you'll find the database creation scripts. IMPORTANT: These scripts are intended to be re-used in all environments and to be used in the official `docker` `postgres` init directory (see the docker-compose file).
* The root-level `data` directory is temporary and is where you'll find the actual database files.

### Database per Service

* These scripts install a database for every service.
* There are multiple databases running on one PostgreSQL server.

### Coupling of Service Name and Database Name

* The name of the database must reflect the service name that owns it.
* The migration scripts search for a service directory with the name given.
* The migration scripts also parse the service name, remove the `_service`, and use the prefix to search for the database to migrate. For example, `data_registry_service` will become `data_registry`, and the database will be `bighill_data_registry_db`.
* The schema created in each database shares the same name as the database itself. The public schema is not used for security reasons. User roles (not admin) are created for the schema.
