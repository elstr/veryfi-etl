import psycopg2

from airflow.decorators import dag
from airflow.operators.postgres_operator import PostgresOperator
from airflow.operators.python_operator import PythonOperator
from datetime import datetime

default_args = {
	'owner': 'airflow',
	'start_date': datetime(2024, 3, 1),
	'catchup': False,
}

def _create_db():
	# TODO: use env for values
	conn = psycopg2.connect(
		dbname="openfoodfacts", 
		user="airflow",
		password="airflow",
		host="postgres"
	)
	conn.autocommit = True  
	cursor = conn.cursor()

	# Check if the database exists
	cursor.execute("SELECT 1 FROM pg_database WHERE datname='openfoodfacts'")
	exists = cursor.fetchone()

	if not exists:
		cursor.execute("CREATE DATABASE openfoodfacts")
	
	cursor.close()
	conn.close()


@dag(schedule="@once", default_args=default_args, catchup=False)
def setup_db():
	
	create_db = PythonOperator(
		task_id='create_db',
		python_callable=_create_db
	)
  
	insert_data = PostgresOperator(
		task_id="insert_data",
		postgres_conn_id="openfoodfacts",
		sql="sql/init_db.sql",
	)

	create_db >> insert_data

 
setup_db()