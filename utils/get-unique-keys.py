# This is used to extract the json keys to a .txt file 
# so it's was esier to analyze the dataset and build the DB schema
# input: dataset.json
# output: .txt with unique keys 

import json
import os

input_file_path = '../veryfi_off_dataset.json' 
output_file_path = 'outputs'
output_file_name = 'unique_keys.txt'


# Step 1: Read the JSON file
def read_json_file(file_path):
    with open(file_path, 'r') as file:
        data = json.load(file)
    return data

# Step 2: Extract unique keys and save them into an array
def extract_unique_keys(data):
    keys_set = set()
    def extract_keys_from_element(element):
        if isinstance(element, dict):
            for key, value in element.items():
                keys_set.add(key)
                extract_keys_from_element(value)
        elif isinstance(element, list):
            for item in element:
                extract_keys_from_element(item)
    
    extract_keys_from_element(data)
    return list(keys_set)

# Step 3: Save the array of keys into a text file
def save_keys_to_file(keys, output_file_path):
    keys.sort() # Sort the keys so it's easier to analyze 
    print(output_file_path)
    with open(output_file_path, 'w') as file:
        for key in keys:
            file.write(f"{key}\n")


def process_json_file(input_file_path, output_file_path):
    output_file = os.path.join(output_file_path, output_file_name)
    data = read_json_file(input_file_path)
    unique_keys = extract_unique_keys(data)
    save_keys_to_file(unique_keys, output_file)


process_json_file(input_file_path, output_file_path)

