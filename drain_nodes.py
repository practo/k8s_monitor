from kubernetes import client, config
import pandas as pd
from datetime import datetime
import argparse
import time
from kubernetes.client import V1DeleteOptions
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


######################################################################################################################################



def drain_node(api_client, node_name, grace_period = -1, ignore_daemonsets = False):
    # Set the node unschedulable
    api_client.patch_node(node_name, {"spec": {"unschedulable": True}})
    # Evict all pods from the node
    # api_client.delete_node(node_name)
    # pods = api_client.list_pod_for_all_namespaces(field_selector=f'spec.nodeName={node_name}')

    # Evict pods gracefully, respecting PodDisruptionBudgets
    # eviction = client.V1beta1Eviction(metadata={"name": node_name})
    # if grace_period > 0:
    #     eviction.delete_options = client.V1DeleteOptions(grace_period_seconds=grace_period)
    # if ignore_daemonsets:
    #     eviction.delete_options.propagation_policy = 'Background'  # Evict DaemonSet-managed pods

    # api = client.AppsV1Api()
    # while True:
    #     try:
    #         api.create_namespaced_pod_eviction(node_name, eviction)
    #         break
    #     except client.exceptions.ApiException as e:
    #         if e.status == 429:  # Too many requests, retry with exponential backoff
    #             time.sleep(2**retry_count)
    #             retry_count += 1
    #         else:
    #             raise

    # print(f"Node {node_name} drained successfully.")

    pods = api_client.list_pod_for_all_namespaces(field_selector=f'spec.nodeName={node_name}')

    # Delete all pods from the node
    for pod in pods.items:
      delete_options = V1DeleteOptions(grace_period_seconds=0)
      api_client.delete_namespaced_pod(
         name=pod.metadata.name,
         namespace=pod.metadata.namespace,
         body=delete_options
      )
    api_client.delete_node(node_name)


######################################################################################################################################

def parse_arguments():
  parser = argparse.ArgumentParser()
  parser.add_argument('-c', '--kubecontext', help='Specify the Kubernetes Cluster', required=True)
  parser.add_argument('-n', '--node', help='Specify the node name from the output file to drain', required=True)

  args = parser.parse_args()
  return args.kubecontext, args.node

if __name__ == "__main__":
  cluster, node = parse_arguments()
  run(cluster, node)
