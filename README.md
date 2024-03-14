# Veryfi Awesome ETL Pipeline 
This is an ETL pipeline example for Veryfi to ingest datasets that are received on a weekly base. 

## DAG tasks
1) Fetch data - Since the data is in the json file I'll skip the fetch data task because of timing
2) Process data
3) Load processed data to DB 

## How to run the project?
This project uses docker-compose to run an airflow dag with some python scripts.   
Execute *make start* to run the project locally.   
The environment variables used are shown in the *.env.example* file. 

1) run make start
2) open localhost:8080 
3) login with user: airflow password: airflow
4) create two db connections with the following 
```
Connection id: openfoodfacts
Connection type: postgres
Database: openfoodfacts
Login: airflow
Password: airflow
Host: postgres
Port: 5432
```

```
Connection id: postgres_localhost
Connection type: postgres
Database: airflow
Login: airflow
Password: airflow
Host: postgres
Port: 5432
```

5) connect your local db ide to:
```
Host: localhost || 0.0.0.0
Database: airflow
Login: airflow
Password: airflow
Port: 5432
```
6) inside localhost, create a new db named `openfoodfacts`
7) unpause setup_db dag 
8) unpause prep_and_insert_data dag