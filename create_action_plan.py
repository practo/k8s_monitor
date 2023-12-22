import pandas as pd
from kubernetes import client, config
import csv
from get_details_for_kuber_cluster import convert_memory_to_ki, convert_cpu_to_millicore

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
csv_header = ['view', 'node_name', 'allocated_cpu', 'allocated_memory', 'remaining_cpu_request', 'remaining_memory_request', 'remaining_cpu_limit', 'Rremaining_memory_limit', 'pod_name', 'container_number', 'limit_cpu', 'request_cpu', 'limit_memory', 'request_memory', 'remaining_cpu_request_percentage', 'remaining_memory_request_percentage', 'remaining_cpu_limit_percentage', 'remaining_memory_limit_percentage']

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
        print(row)

sorted_k8s_data = sorted(k8s_descending_data, key=lambda x: (
    -x['remaining_cpu_request_percentage'],
    -x['remaining_memory_request_percentage'],
    -x['remaining_cpu_limit_percentage'],
    -x['remaining_memory_limit_percentage']
))
k8s_descending_data.insert(0, csv_header)

# print("\nSorted:", sorted_k8s_data)
csv_file_path = 'k8s_descending.csv'

with open(csv_file_path, 'w', newline='') as csvfile:
    csv_writer = csv.writer(csvfile)
    csv_writer.writerows(sorted_k8s_data)

print(f"Total remaining node allocated cpu: {total_node_allocated_cpu}")
print(f"Total remaining node allocated memory: {total_node_allocated_memory}")
print(f"Total remaining node request cpu: {total_remaining_node_request_cpu}")
print(f"Total remaining node request memory: {total_remaining_node_request_memory}")
print(f"Total remaining node limit cpu: {total_remaining_node_limit_cpu}")
print(f"Total remaining node limit memory: {total_remaining_node_limit_memory}")




# # Load the Kubernetes configuration
# config.load_kube_config()

# # Create a Kubernetes API client
# api_client = client.CoreV1Api()

# # Iterate through the data
# for index, row in data.iterrows():
#     if row['view'] == 'NODE_VIEW' and float(float(convert_cpu_to_millicore(row['remaining_cpu_request'])) / float(convert_cpu_to_millicore(row['allocated_cpu']))) > 0.2 and float(float(convert_memory_to_ki(row['remaining_memory_request'])) / float(convert_memory_to_ki(row['allocated_memory']))) > 0.2:
#         node_name = row['node_name']

#         # Set the node unschedulable
#         api_client.patch_node(node_name, {"spec": {"unschedulable": True}})

#         # Delete all pods in the node
#         api_client.delete_collection_namespaced_pod(namespace='default', label_selector=f"node={node_name}")
