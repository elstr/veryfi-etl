import json

from airflow.hooks.postgres_hook import PostgresHook
from airflow.decorators import dag, task

from datetime import datetime

input_file_path = '/opt/airflow/data/veryfi_off_dataset.json' 
default_args = {"start_date": datetime(2021, 1, 1)}

TABLES_DATA = [
    {'table_name': 'additives', 'fields': ['id', 'additives_tags']},
    {'table_name': 'allergens', 'fields': ['id', 'allergens']},
    {'table_name': 'brands', 'fields': ['id', 'brands_tags']},
    {'table_name': 'categories', 'fields': ['id', 'categories_tags']},
    {'table_name': 'countries', 'fields': ['id', 'countries_tags']},
    {'table_name': 'data_quality_errors_tags', 'fields': ['id', 'data_quality_errors_tags']},
    {'table_name': 'food_groups', 'fields': ['id', 'food_groups', 'food_groups_tags']},
    {'table_name': 'ingredients_analysis_tags', 'fields': ['id', 'ingredients_analysis_tags']},
    {'table_name': 'ingredients_tags', 'fields': ['id', 'ingredients_tags']},
    {'table_name': 'labels', 'fields': ['id', 'labels_tags']},
    {'table_name': 'manufacturing_places', 'fields': ['id', 'manufacturing_places_tags']},
    {'table_name': 'nutrient_levels_tags', 'fields': ['id', 'nutrient_levels_tags']},
    {'table_name': 'packaging', 'fields': ['id', 'packaging']},
    {'table_name': 'pnns_groups', 'fields': ['id', 'pnns_groups']},
    {'table_name': 'popularity_tags', 'fields': ['id', 'popularity_tags']},
    {'table_name': 'purchase_places', 'fields': ['id', 'purchase_places']},
    {'table_name': 'stores', 'fields': ['id', 'stores']},
    {'table_name': 'traces', 'fields': ['id', 'traces']},
]

@dag(schedule="@daily", default_args=default_args, catchup=False)
def prep_and_insert_data():
  @task
  def get_data_from_db():
    """
    Fetches data from multiple tables based on the provided list of table information.
    
    :input TABLES_DATA: A list of dictionaries, each containing the 'table_name' and 'fields' to query.
                        Example: [{'table_name': 'additives', 'fields': ['id', 'additives_tags']},
                                  {'table_name': 'allergens', 'fields': ['id', 'allergens']}]
    :output: A dictionary where each key is a table name and its value is the fetched data for that table.
    """
    results = {}
    hook = PostgresHook(postgres_conn_id='openfoodfacts')

    with hook.get_conn() as conn:
        with conn.cursor() as cursor:
            for table in TABLES_DATA:
                table_name = table['table_name']
                fields = ", ".join(table['fields'])
                sql_query = f"SELECT {fields} FROM {table_name};"
                cursor.execute(sql_query)
                results[table_name] = cursor.fetchall()
    return results
  
  @task
  def prep_data(db_data):
    """
    Transforms the data to insert to the db
    """
    with open(input_file_path, 'r') as file:
      data = json.load(file)
    
    products_to_insert = []
    for item in data:
      products_to_insert.append({
      'abbreviated_product_name': item.get('abbreviated_product_name'),
      'code': item.get('code'),
      'completeness': item.get('completeness'),
      'created_datetime': item.get('created_datetime'),
      'created_t': item.get('created_t'),
      'creator': item.get('creator'),
      'ecoscore_grade': item.get('ecoscore_grade'),
      'ecoscore_score': item.get('ecoscore_score'),
      'first_packaging_code_geo': item.get('first_packaging_code_geo'),
      'generic_name': item.get('generic_name'),
      'last_image_datetime': item.get('last_image_datetime'),
      'last_image_t': item.get('last_image_t'),
      'last_modified_by': item.get('last_modified_by'),
      'last_modified_datetime': item.get('last_modified_datetime'),
      'last_modified_t': item.get('last_modified_t'),
      'main_category': item.get('main_category'),
      'no_nutrition_data': item.get('no_nutrition_data'),
      'nova_group': item.get('nova_group'),
      'nutriscore_grade': item.get('nutriscore_grade'),
      'nutriscore_score': item.get('nutriscore_score'),
      'owner': item.get('owner'),
      'pnns_groups_1': item.get('pnns_groups_1'),
      'pnns_groups_2': item.get('pnns_groups_2'),
      'product_name': item.get('product_name'),
      'product_quantity': item.get('product_quantity'),
      'quantity': item.get('quantity'),
      'serving_quantity': item.get('serving_quantity'),
      'serving_size': item.get('serving_size'),
      'unique_scans_n': item.get('unique_scans_n'),
      'url': item.get('url]')
      })   

    # TODO: build relations
      # product_additives / product_allergens / etc
    # return entities to insert
    return [products_to_insert]
  
  @task
  def insert_data(data):

    """
    Inserts data into db
    """
    # hook = PostgresHook(postgres_conn_id='openfoodfacts')
    # with hook.get_conn() as conn:
        # with conn.cursor() as cursor:
          # exe inserts
  
    return 'true'
  
  insert_data(prep_data(get_data_from_db()))

prep_and_insert_data()