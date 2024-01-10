import pandas as pd
from kubernetes import client, config
import csv
from get_details_for_kuber_cluster import convert_memory_to_ki, convert_cpu_to_millicore
import pandas as pd
from datetime import datetime
import logging
import argparse

def create_action_plan(k8s_view_file, timestamp):
    # Read the CSV file into a pandas DataFrame
    data = pd.read_csv(k8s_view_file)

    # Check if the DataFrame is empty
    if data.empty:
        logging.debug("No data found in the file. Exiting...")
        return None, None

    total_node_allocated_cpu = 0
    total_node_allocated_memory = 0
    total_remaining_node_request_cpu = 0
    total_remaining_node_request_memory = 0
    total_remaining_node_limit_cpu = 0
    total_remaining_node_limit_memory = 0
    k8s_descending_data = []

    for index, row in data.iterrows():
        if row['view'] == 'NODE_VIEW':
            total_node_allocated_cpu += convert_cpu_to_millicore(row['allocated_cpu'])
            total_node_allocated_memory += convert_memory_to_ki(row['allocated_memory'])
            total_remaining_node_request_cpu += convert_cpu_to_millicore(row['remaining_cpu_request'])
            total_remaining_node_request_memory += convert_memory_to_ki(row['remaining_memory_request'])
            total_remaining_node_limit_cpu += convert_cpu_to_millicore(row['remaining_cpu_limit'])
            total_remaining_node_limit_memory += convert_memory_to_ki(row['remaining_memory_limit'])
            row['remaining_cpu_request_percentage'] = round(float(float(convert_cpu_to_millicore(row['remaining_cpu_request'])) / float(convert_cpu_to_millicore(row['allocated_cpu']))) * 100)
            row['remaining_memory_request_percentage'] = round(float(float(convert_memory_to_ki(row['remaining_memory_request'])) / float(convert_memory_to_ki(row['allocated_memory']))) * 100)
            row['remaining_cpu_limit_percentage'] = round(float(float(convert_cpu_to_millicore(row['remaining_cpu_limit'])) / float(convert_cpu_to_millicore(row['allocated_cpu']))) * 100)
            row['remaining_memory_limit_percentage'] = round(float(float(convert_memory_to_ki(row['remaining_memory_limit'])) / float(convert_memory_to_ki(row['allocated_memory']))) * 100)
            k8s_descending_data.append(row)

    sorted_k8s_data = sorted(k8s_descending_data, key=lambda x: (
        -x['remaining_cpu_request_percentage'],
        -x['remaining_memory_request_percentage'],
        -x['remaining_cpu_limit_percentage'],
        -x['remaining_memory_limit_percentage']
    ))

    logging.debug("\nSorted:", sorted_k8s_data)

    csv_file_path = f'k8s_action_plan_{timestamp}.csv'

    df = pd.DataFrame(sorted_k8s_data)
    df.to_csv(csv_file_path, index=False)

    logging.debug(f"Total remaining node allocated cpu: {total_node_allocated_cpu}")
    logging.debug(f"Total remaining node allocated memory: {total_node_allocated_memory}")
    logging.debug(f"Total remaining node request cpu: {total_remaining_node_request_cpu}")
    logging.debug(f"Total remaining node request memory: {total_remaining_node_request_memory}")
    logging.debug(f"Total remaining node limit cpu: {total_remaining_node_limit_cpu}")
    logging.debug(f"Total remaining node limit memory: {total_remaining_node_limit_memory}")

    return sorted_k8s_data[0]['node_name'], csv_file_path

def parse_arguments():
  parser = argparse.ArgumentParser()
  parser.add_argument('-c', '--file_name', help='Specify the k8s_output file name', required=True)
  args = parser.parse_args()
  return args.file_name

if __name__ == "__main__":
    file_name = parse_arguments()
    now = datetime.now()
    timestamp = now.strftime("%d_%m_%Y_%H_%M_%S")
    create_action_plan(file_name, timestamp)