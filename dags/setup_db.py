from airflow.decorators import dag
from airflow.operators.postgres_operator import PostgresOperator
from datetime import datetime

default_args = {
	'owner': 'airflow',
	'start_date': datetime(2024, 3, 1),
	'catchup': False,
}

@dag(schedule="@once", default_args=default_args, catchup=False)
def setup_db():
	insert_data = PostgresOperator(
		task_id="insert_data",
		postgres_conn_id="openfoodfacts",
		sql="sql/init_db.sql",
	)

	insert_data

 
setup_db()