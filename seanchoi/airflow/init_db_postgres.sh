#!/bin/bash
set -e

psql -v ON_ERROR_STOP=1 --username "airflow" --dbname "airflow" <<-EOSQL
    CREATE DATABASE metastore;
EOSQL
