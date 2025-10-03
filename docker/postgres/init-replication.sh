#!/bin/bash
set -e

# Create replication user for Debezium
psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" <<-EOSQL
    CREATE USER my_repl_user WITH REPLICATION PASSWORD 'my_repl_password';
    GRANT ALL PRIVILEGES ON DATABASE postgres TO my_repl_user;
EOSQL
