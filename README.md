# Verify Awesome ETL Pipeline 
This is an ETL pipeline example for Verify to ingest datasets that are received on a weekly base. 

## DAG tasks
1) Fetch data 
2) Process data
3) Load processed data to DB 

## How to run the project?
This project uses docker-compose to run an airflow dag with some python scripts. 
Execute *make start* to run the project locally. 
The environment variables used are shown in the *.env.example* file. 

