import pandas as pd
from kubernetes import client, config
from get_details_for_kuber_cluster import convert_memory_to_ki, convert_cpu_to_millicore

file_path = "k8s_output.csv"

# Read the CSV file into a pandas DataFrame
data = pd.read_csv(file_path)

# Load the Kubernetes configuration
config.load_kube_config()

# Create a Kubernetes API client
api_client = client.CoreV1Api()

# Iterate through the data
for index, row in data.iterrows():
    if row['view'] == 'NODE_VIEW' and float(float(convert_cpu_to_millicore(row['remaining_cpu_request'])) / float(convert_cpu_to_millicore(row['allocated_cpu']))) > 0.2 and float(float(convert_memory_to_ki(row['remaining_memory_request'])) / float(convert_memory_to_ki(row['allocated_memory']))) > 0.2:
        node_name = row['node_name']

        # Set the node unschedulable
        api_client.patch_node(node_name, {"spec": {"unschedulable": True}})

        # Delete all pods in the node
        api_client.delete_collection_namespaced_pod(namespace='default', label_selector=f"node={node_name}")

