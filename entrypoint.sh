#!/bin/bash

set -e

# Wait for PostgreSQL to be ready
until pg_isready -h postgres -p 5432 -U "$POSTGRES_USER" >/dev/null 2>&1; do
  echo "Waiting for PostgreSQL to be ready..."
  sleep 1
done

# Create the Airflow database if it doesn't exist
if ! psql -h postgres -U "$POSTGRES_USER" -lqt | cut -d \| -f 1 | grep -qw "$POSTGRES_DB"; then
  echo "Creating Airflow database..."
  createdb -h postgres -U "$POSTGRES_USER" "$POSTGRES_DB"
fi

# Continue with the default Airflow entrypoint command
exec airflow "$@"
