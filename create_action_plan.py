import pandas as pd
from kubernetes import client, config
import csv
from get_details_for_kuber_cluster import convert_memory_to_ki, convert_cpu_to_millicore
import pandas as pd

file_path = "k8s_output.csv"

# Read the CSV file into a pandas DataFrame
data = pd.read_csv(file_path)

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

print("\nSorted:", sorted_k8s_data)

csv_file_path = 'k8s_descending.csv'

df = pd.DataFrame(sorted_k8s_data)
df.to_csv(csv_file_path, index=False)

print(f"Total remaining node allocated cpu: {total_node_allocated_cpu}")
print(f"Total remaining node allocated memory: {total_node_allocated_memory}")
print(f"Total remaining node request cpu: {total_remaining_node_request_cpu}")
print(f"Total remaining node request memory: {total_remaining_node_request_memory}")
print(f"Total remaining node limit cpu: {total_remaining_node_limit_cpu}")
print(f"Total remaining node limit memory: {total_remaining_node_limit_memory}")
