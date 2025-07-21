# airflow3_setup

Investigate way to install and setup airflow 3.0

# Steps: 

## Setup folders:
 
mkdir -p ./dags ./logs ./plugins

## Setup uid environment variable: 

echo -e "AIRFLOW_UID=$(id -u)" > .env


## Initialize Airflow

docker compose up airflow-init

## Start airflow service 

docker-compose up -d --pull always --force-recreate --build

