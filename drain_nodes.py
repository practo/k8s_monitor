from kubernetes import client, config
import pandas as pd
from datetime import datetime
import argparse
# from kubernetes.client.models import V1beta1Eviction

def run(cluster, node):
    # Load the Kubernetes configuration
    config.load_kube_config(context=cluster)

    # Create a Kubernetes API client
    api_client = client.CoreV1Api()

    file_path = "k8s_descending.csv"

    # Read the CSV file into a pandas DataFrame
    data = pd.read_csv(file_path)

    # Select the row where view = 'NODE_VIEW' and node_name is the variable node
    selected_row = data[(data['view'] == 'NODE_VIEW') & (data['node_name'] == node)]

    # Iterate through the selected row
    for index, row in selected_row.iterrows():
        node_name = row['node_name']
        print(f"Draining Node-Name: {node_name}")
        drain_node(api_client, node_name)

def drain_node(api_client, node_name):
    # Set the node unschedulable
    api_client.patch_node(node_name, {"spec": {"unschedulable": True}})

    # eviction = V1beta1Eviction(
    #     delete_options=client.V1DeleteOptions(grace_period_seconds=0),
    # )
    # api_client.create_node_proxy_eviction(node_name, body=eviction)

    # Delete the node
    api_client.delete_node(node_name)

# def drain_node(api_client, node_name):
#     # Set the node unschedulable
#     api_client.patch_node(node_name, {"spec": {"unschedulable": True}})

#     # Get all pods on the node
#     pods = api_client.list_pod_for_all_namespaces(field_selector=f'spec.nodeName={node_name}')

#     # Evict all pods from the node
#     for pod in pods.items:
#         api_client.delete_namespaced_pod(pod.metadata.name, pod.metadata.namespace)

#     # Delete the node
#     api_client.delete_node(node_name)

def parse_arguments():
  parser = argparse.ArgumentParser()
  parser.add_argument('-c', '--kubecontext', help='Specify the Kubernetes Cluster', required=True)
  parser.add_argument('-n', '--node', help='Specify the node name from the output file to drain', required=True)

  args = parser.parse_args()
  return args.kubecontext, args.node

if __name__ == "__main__":
  cluster, node = parse_arguments()
  run(cluster, node)
