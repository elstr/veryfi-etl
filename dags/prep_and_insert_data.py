import json
import os

from airflow.decorators import dag, task

from datetime import datetime

input_file_path = '/opt/airflow/data/veryfi_off_dataset.json' 
default_args = {"start_date": datetime(2021, 1, 1)}

@dag(schedule="@daily", default_args=default_args, catchup=False)
def prep_and_insert_data():
  @task
  def get_ids():
    """
    Retrieves the ids from the database to make easier inserts
    """
    return 123
  
  @task
  def prep_data(ids):
    """
    Transforms the data to insert to the db
    """
    print('ids: ' + ids)
    return 'this would be my data'
  
  @task
  def insert_data(data):
    """
    Inserts data into db
    """
    print('data param: '+ data)
    return 'true'
  
  insert_data(prep_data(get_ids()))

prep_and_insert_data()