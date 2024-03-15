import json
import pandas as pd


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
    {'table_name': 'popularity_tags', 'fields': ['id', 'popularity_tags']},
    {'table_name': 'packaging', 'fields': ['id', 'packaging_tags']},
    {'table_name': 'pnns_groups', 'fields': ['id', 'pnns_groups']},
    {'table_name': 'purchase_places', 'fields': ['id', 'purchase_places']},
    {'table_name': 'stores', 'fields': ['id', 'stores']},
    {'table_name': 'traces', 'fields': ['id', 'traces_tags']},
]

def prep_table_tag_to_id(db_data, table_name, tag_column):
    """
    Prepares a df and a tag-to-id mapping for the specified table, handling multiple comma-separated tags.
    """
    columns = [table['fields'] for table in TABLES_DATA if table['table_name'] == table_name][0]
    
    # Create df
    df = pd.DataFrame(db_data[table_name], columns=columns)
    
    # Lower the tag name and strip spaces
    df[tag_column] = df[tag_column].str.lower().str.strip()
    
    # Explode the DataFrame based on comma-separated tags to ensure each tag gets its own row
    df = df.assign(**{tag_column: df[tag_column].str.split(',')}).explode(tag_column)
    
    # Create tag-to-id mapping
    tag_to_id = df.set_index(tag_column)['id']
    
    return tag_to_id

def transform_product_composition(item):
   return {
      'alcohol_100g': item.get('alcohol_100g'),
      'added_sugars_100g': item.get('added-sugars_100g'),
      'alpha_linolenic_acid_100g': item.get('alpha-linolenic-acid_100g'),
      'arachidic_acid_100g': item.get('arachidic-acid_100g'),
      'arachidonic_acid_100g': item.get('arachidonic-acid_100g'),
      'behenic_acid_100g': item.get('behenic-acid_100g'),
      'bicarbonate_100g': item.get('bicarbonate_100g'),
      'biotin_100g': item.get('biotin_100g'),
      'caffeine_100g': item.get('caffeine_100g'),
      'calcium_100g': item.get('calcium_100g'),
      'carbohydrates_100g': item.get('carbohydrates_100g'),
      'carbon_footprint_from_meat_or_fish_100g': item.get('carbon-footprint-from-meat-or-fish_100g'),
      'chloride_100g': item.get('chloride_100g'),
      'cholesterol_100g': item.get('cholesterol_100g'),
      'chromium_100g': item.get('chromium_100g'),
      'cocoa_100g': item.get('cocoa_100g'),
      'copper_100g': item.get('copper_100g'),
      'energy_from_fat_100g': item.get('energy-from-fat_100g'),
      'energy_kcal_100g': item.get('energy-kcal_100g'),
      'energy_kj_100g': item.get('energy-kj_100g'),
      'energy_100g': item.get('energy_100g'),
      'fat_100g': item.get('fat_100g'),
      'fiber_100g': item.get('fiber_100g'),
      'folates_100g': item.get('folates_100g'),
      'fruits_vegetables_nuts_estimate_from_ingredients_100g': item.get('fruits-vegetables-nuts-estimate-from-ingredients_100g'),
      'fruits_vegetables_nuts_estimate_100g': item.get('fruits-vegetables-nuts-estimate_100g'),
      'fruits_vegetables_nuts_100g': item.get('fruits-vegetables-nuts_100g'),
      'gamma_linolenic_acid_100g': item.get('gamma-linolenic-acid_100g'),
      'insoluble_fiber_100g': item.get('insoluble-fiber_100g'),
      'iodine_100g': item.get('iodine_100g'),
      'iron_100g': item.get('iron_100g'),
      'lactose_100g': item.get('lactose_100g'),
      'magnesium_100g': item.get('magnesium_100g'),
      'manganese_100g': item.get('manganese_100g'),
      'molybdenum_100g': item.get('molybdenum_100g'),
      'monounsaturated_fat_100g': item.get('monounsaturated-fat_100g'),
      'nucleotides_100g': item.get('nucleotides_100g'),
      'nutrition_score_fr_100g': item.get('nutrition-score-fr_100g'),
      'omega_3_fat_100g': item.get('omega-3-fat_100g'),
      'omega_6_fat_100g': item.get('omega-6-fat_100g'),
      'omega_9_fat_100g': item.get('omega-9-fat_100g'),
      'pantothenic_acid_100g': item.get('pantothenic-acid_100g'),
      'ph_100g': item.get('ph_100g'),
      'phosphorus_100g': item.get('phosphorus_100g'),
      'phylloquinone_100g': item.get('phylloquinone_100g'),
      'polyols_100g': item.get('polyols_100g'),
      'polyunsaturated_fat_100g': item.get('polyunsaturated-fat_100g'),
      'potassium_100g': item.get('potassium_100g'),
      'proteins_100g': item.get('proteins_100g'),
      'salt_100g': item.get('salt_100g'),
      'saturated_fat_100g': item.get('saturated-fat_100g'),
      'selenium_100g': item.get('selenium_100g'),
      'sodium_100g': item.get('sodium_100g'),
      'soluble_fiber_100g': item.get('soluble-fiber_100g'),
      'starch_100g': item.get('starch_100g'),
      'stearic_acid_100g': item.get('stearic-acid_100g'),
      'sugars_100g': item.get('sugars_100g'),
      'trans_fat_100g': item.get('trans-fat_100g'),
      'vitamin_a_100g': item.get('vitamin-a_100g'),
      'vitamin_b12_100g': item.get('vitamin-b12_100g'),
      'vitamin_b1_100g': item.get('vitamin-b1_100g'),
      'vitamin_b2_100g': item.get('vitamin-b2_100g'),
      'vitamin_b6_100g': item.get('vitamin-b6_100g'),
      'vitamin_b9_100g': item.get('vitamin-b9_100g'),
      'vitamin_c_100g': item.get('vitamin-c_100g'),
      'vitamin_d_100g': item.get('vitamin-d_100g'),
      'vitamin_e_100g': item.get('vitamin-e_100g'),
      'vitamin_k_100g': item.get('vitamin-k_100g'),
      'vitamin_pp_100g': item.get('vitamin-pp_100g'),
      'zinc_100g': item.get('zinc_100g'),
   }

def transform_product_tags(tag_to_id, item, key, table_name):
    print('KEY', key)
    """
    Transforms product tags (like additives or allergens) into their IDs, handling multiple comma-separated tags.
    """
    # Retrieve tags from the item, ensuring they're in a list format and split if comma-separated
    raw_tags = item.get(key, [])
    if isinstance(raw_tags, str):
        tags = [tag.strip() for tag in raw_tags.lower().split(',')]
    elif raw_tags is None:
        tags = []
    else:
        # Flatten the list in case of lists within lists and split comma-separated tags
        tags = [inner_tag.strip() for tag in raw_tags for inner_tag in tag.lower().split(',') if inner_tag]

    # Find corresponding IDs for the tags
    ids = tag_to_id.loc[tag_to_id.index.intersection(tags)].tolist()
    result = {'product_code': item.get('code'), table_name+'_id': ids}

    return result

   

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
  def prep_data(db_data ):
    """
    Transforms the data to insert to the db
    """
    products_to_insert = []
    product_composition = []
    products_additives = []
    products_allergens = []
    products_countries = []
    products_brands = []
    products_categories = []
    products_data_quality_errors_tags = []
    products_food_groups = []
    products_ingredients_tags = []
    products_ingredients_analysis_tags = []
    products_popularity_tags = []
    products_nutrient_levels_tags = []
    products_traces = []
    products_labels = []
    products_manufacturing_places = []
    products_packaging = []
    products_pnns_groups = []
    products_purchase_places = []
    products_stores = []

    #  prep tags to id mappings
    additives_tag_to_id = prep_table_tag_to_id(db_data, 'additives', 'additives_tags')
    brands_tag_to_id = prep_table_tag_to_id(db_data, 'brands', 'brands_tags')
    categories_tag_to_id = prep_table_tag_to_id(db_data, 'categories', 'categories_tags')
    data_quality_errors_tags_tag_to_id = prep_table_tag_to_id(db_data, 'data_quality_errors_tags', 'data_quality_errors_tags')
    food_groups_tag_to_id = prep_table_tag_to_id(db_data, 'food_groups', 'food_groups_tags')
    ingredients_tags_tag_to_id = prep_table_tag_to_id(db_data, 'ingredients_tags', 'ingredients_tags')
    ingredients_analysis_tags_tag_to_id = prep_table_tag_to_id(db_data, 'ingredients_analysis_tags', 'ingredients_analysis_tags')
    popularity_tags_tag_to_id = prep_table_tag_to_id(db_data, 'popularity_tags', 'popularity_tags')
    nutrient_levels_tags_tag_to_id = prep_table_tag_to_id(db_data, 'nutrient_levels_tags', 'nutrient_levels_tags')
    labels_tags_tag_to_id = prep_table_tag_to_id(db_data, 'labels', 'labels_tags')
    manufacturing_places_tags_tag_to_id = prep_table_tag_to_id(db_data, 'manufacturing_places', 'manufacturing_places_tags')
    packaging_tags_tag_to_id = prep_table_tag_to_id(db_data, 'packaging', 'packaging_tags')
    traces_tags_tag_to_id = prep_table_tag_to_id(db_data, 'traces', 'traces_tags')
    countries_tag_to_id = prep_table_tag_to_id(db_data, 'countries', 'countries_tags')
    allergens_tag_to_id = prep_table_tag_to_id(db_data, 'allergens', 'allergens')
    pnns_groups_tags_tag_to_id = prep_table_tag_to_id(db_data, 'pnns_groups', 'pnns_groups')
    purchase_places_tags_tag_to_id = prep_table_tag_to_id(db_data, 'purchase_places', 'purchase_places')
    stores_tags_tag_to_id = prep_table_tag_to_id(db_data, 'stores', 'stores')

    with open(input_file_path, 'r') as file:
      data = json.load(file)
    

    for item in data:
      product_composition = transform_product_composition(item)
      products_countries.append(transform_product_tags(countries_tag_to_id, item, 'countries_tags', 'countries'))
      products_allergens.append(transform_product_tags(allergens_tag_to_id, item, 'allergens', 'allergens'))
      products_additives.append(transform_product_tags(additives_tag_to_id, item, 'additives_tags', 'additives' ))
      products_brands.append(transform_product_tags(brands_tag_to_id, item, 'brands_tags', 'brands'))
      products_categories.append(transform_product_tags(categories_tag_to_id, item, 'categories_tags', 'categories'))
      products_data_quality_errors_tags.append(transform_product_tags(data_quality_errors_tags_tag_to_id, item, 'data_quality_errors_tags', 'data_quality_errors_tags'))
      products_food_groups.append(transform_product_tags(food_groups_tag_to_id, item, 'food_groups_tags', 'food_groups'))
      products_ingredients_tags.append(transform_product_tags(ingredients_tags_tag_to_id, item, 'ingredients_tags', 'ingredients_tags'))
      products_ingredients_analysis_tags.append(transform_product_tags(ingredients_analysis_tags_tag_to_id, item,'ingredients_analysis_tags', 'ingredients_analysis_tags'))
      products_popularity_tags.append(transform_product_tags(popularity_tags_tag_to_id, item, 'popularity_tags', 'popularity_tags'))
      products_nutrient_levels_tags.append(transform_product_tags(nutrient_levels_tags_tag_to_id, item, 'nutrient_levels_tags', 'nutrient_levels_tags'))
      products_labels.append(transform_product_tags(labels_tags_tag_to_id, item, 'labels_tags', 'labels'))
      products_manufacturing_places.append(transform_product_tags(manufacturing_places_tags_tag_to_id, item, 'manufacturing_places_tags', 'manufacturing_places'))
      products_packaging.append(transform_product_tags(packaging_tags_tag_to_id, item, 'packaging_tags', 'packaging'))
      products_purchase_places.append(transform_product_tags(purchase_places_tags_tag_to_id, item, 'purchase_places', 'purchase_places'))
      products_stores.append(transform_product_tags(stores_tags_tag_to_id, item, 'stores', 'stores'))
      products_traces.append(transform_product_tags(traces_tags_tag_to_id, item, 'traces_tags', 'traces'))
      
      products_pnns_groups.append(transform_product_tags(pnns_groups_tags_tag_to_id, item, 'pnns_groups', 'ppns_groups'))  # -> todo: change transformation to make two pnns_groups

      product = {
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
        'url': item.get('url]'),
      }
      product.update(product_composition)
      products_to_insert.append(product)   


    return {
      'products':products_to_insert,
      'product_countries':products_countries,
      'product_allergens':products_allergens,
      'product_additives':products_additives,
      'product_brands':products_brands,
      'product_categories':products_categories,
      'product_data_quality_errors_tags':products_data_quality_errors_tags,
      'product_food_groups':products_food_groups,
      'product_ingredients_tags':products_ingredients_tags,
      'product_ingredients_analysis_tags':products_ingredients_analysis_tags,
      'product_popularity_tags':products_popularity_tags,
      'product_nutrient_levels_tags':products_nutrient_levels_tags,
      'product_labels':products_labels,
      'product_manufacturing_places':products_manufacturing_places,
      'product_packaging':products_packaging,
      'product_purchase_places':products_purchase_places,
      'product_stores':products_stores,
      'product_traces':products_traces,
      'product_pnns_groups':products_pnns_groups
    }
  
  @task
  def insert_data(data):
    hook = PostgresHook(postgres_conn_id='openfoodfacts')
    with hook.get_conn() as conn:
        with conn.cursor() as cursor:
            for table, values in data.items():
                join_table_inserts = {}

                for value in values:
                    # Handling for single insert (non-list values)
                    non_list_values = {k: v for k, v in value.items() if not isinstance(v, list) and v is not None}
                    if non_list_values:
                        columns = ', '.join(non_list_values.keys())
                        placeholders = ', '.join(['%s'] * len(non_list_values))
                        sql = f"INSERT INTO {table} ({columns}) VALUES ({placeholders})"
                        cursor.execute(sql, list(non_list_values.values()))

                    # Handling for many-to-many relationships (list values)
                    for key, val in value.items():
                        if isinstance(val, list) and val:
                            filtered_vals = [v for v in val if v is not None]
                            if filtered_vals:
                                join_table = f"{table}_{key}"  # e.g. product_traces
                                if join_table not in join_table_inserts:
                                    join_table_inserts[join_table] = []
                                join_table_inserts[join_table].extend([(value['product_code'], v) for v in filtered_vals])

                # Execute batch inserts for many-to-many relationships
                for join_table, batch_data in join_table_inserts.items():
                    print('table', table)
                    print('batch_data', batch_data)
                    key = key.replace(f"product_{table}_", "") # replace prefix: product_traces_traces_id -> traces_id
                    sql = f"INSERT INTO {table} (product_code, {key}) VALUES (%s, %s)"
                    cursor.executemany(sql, batch_data)

            conn.commit()  # Commit all changes

  # def insert_data(data):
  #   """
  #   Inserts data into the database, prioritizing single value inserts before handling many-to-many relationships.
  #   """
  #   hook = PostgresHook(postgres_conn_id='openfoodfacts')
  #   with hook.get_conn() as conn:
  #       with conn.cursor() as cursor:
  #           for table, values in data.items():
  #               join_table_inserts = {}
  #               single_insert_values = []

  #               for value in values:
  #                   product_code = value.get('product_code')
  #                   for key, val in value.items():
  #                       if isinstance(val, list):  # handle many-to-many relationships
  #                           join_table = f"{table}_{key}"
  #                           if join_table not in join_table_inserts:
  #                               join_table_inserts[join_table] = []
                            
  #                           # Filter out None or empty strings from the list
  #                           filtered_vals = [v for v in val if v]
  #                           if filtered_vals:  # only add to batch if there are values
  #                               batch_data = [(product_code, v) for v in filtered_vals]
  #                               join_table_inserts[join_table].extend(batch_data)

  #                           # join_table_inserts[join_table].extend([(product_code, v) for v in val if v])
  #                       elif val:  
  #                           single_insert_values.append((val,))

  #               # Execute single insert operation for the current table
  #               if single_insert_values:
  #                   common_col = 'code' if table == 'products' else 'product_code'
  #                   # Assuming 'product_code' is a common column, adjust as necessary
  #                   sql = f"INSERT INTO {table} ({common_col}) VALUES (%s)"
  #                   cursor.executemany(sql, single_insert_values)

  #               # Execute batch inserts for many-to-many relationships
  #               for join_table, batch_data in join_table_inserts.items():
  #                   key = key.replace(f"product_{table}_", "") # replace prefix: product_traces_traces_id -> traces_id
  #                   sql = f"INSERT INTO {table} (product_code, {key}) VALUES (%s, %s)"
  #                   cursor.executemany(sql, batch_data)

  #           conn.commit()  # Commit all changes


  
  insert_data(prep_data(get_data_from_db()))

prep_and_insert_data()