#!/bin/bash

set -o errexit
set -o nounset
set -o pipefail

echo "Creating Airflow configuration file at ${AIRFLOW_HOME}/airflow.cfg"
airflow config list > /dev/null

echo "Resetting Airflow migrations"
airflow db reset -y

echo "Initializing Airflow metadata database"
airflow db init

echo "Upgrading Airflow database schema"
airflow db upgrade
