from kubernetes import client, config
import pandas as pd
from datetime import datetime
import argparse

def run(cluster, node):
    # Load the Kubernetes configuration
    config.load_kube_config(context=cluster)

    # Create a Kubernetes API client
    api_client = client.CoreV1Api()

    file_path = "k8s_descending.csv"

    # Read the CSV file into a pandas DataFrame
    data = pd.read_csv(file_path)

    # Iterate through the data
    for index, row in data.iterrows():
        if index == int(node):
            node_name = row['node_name']
            print(f"Draining Node-Name: {node_name}")
            drain_node(api_client, node_name)

def drain_node(api_client, node_name):
    # Set the node unschedulable
    api_client.patch_node(node_name, {"spec": {"unschedulable": True}})

    # Get all pods on the node
    pods = api_client.list_pod_for_all_namespaces(field_selector=f'spec.nodeName={node_name}')

    # Evict all pods from the node
    for pod in pods.items:
        api_client.delete_namespaced_pod(pod.metadata.name, pod.metadata.namespace)

    # Delete the node
    api_client.delete_node(node_name)

def parse_arguments():
  parser = argparse.ArgumentParser()
  parser.add_argument('-c', '--kubecontext', help='Specify the Kubernetes Cluster', required=True)
  parser.add_argument('-n', '--node', help='Specify the node index from the output file to drain', required=True)
  args = parser.parse_args()
  return args.kubecontext, args.node

if __name__ == "__main__":
  cluster, node = parse_arguments()
  run(cluster, node)
