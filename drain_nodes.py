from kubernetes import client, config
import pandas as pd
from datetime import datetime
import argparse
import time
from kubernetes.client import V1DeleteOptions
import logging

def drain_node(cluster, node, action_plan_file_path, force_delete):
  # Load the Kubernetes configuration
  config.load_kube_config(context=cluster)

  # Create a Kubernetes API client
  api_client = client.CoreV1Api()

  # Read the CSV file into a pandas DataFrame
  data = pd.read_csv(action_plan_file_path)

  # Select the row where view = 'NODE_VIEW' and node_name is the variable node
  selected_row = data[(data['view'] == 'NODE_VIEW') & (data['node_name'] == node)]

  # Check if the selected row is empty
  if selected_row.empty:
    logging.error(f"No node with name '{node}' found in the CSV file.")
  else:
    remaining_cpu_request_percentage = selected_row['remaining_cpu_request_percentage'].values[0]
    if remaining_cpu_request_percentage > 20 or force_delete == 'True':
      logging.info(f"Draining Node-Name: {node}")
      drain_and_delete_node(api_client, node)
      logging.info(f"Node {node} drained successfully.")
    else:
      logging.info(f"Node-Name: {node} cannot be drained as remaining_cpu_request_percentage is {remaining_cpu_request_percentage}.")

def drain_and_delete_node(api_client, node_name, grace_period = -1, ignore_daemonsets = False):
    # Set the node unschedulable
    api_client.patch_node(node_name, {"spec": {"unschedulable": True}})
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

def parse_arguments():
  parser = argparse.ArgumentParser()
  parser.add_argument('-c', '--kubecontext', help='Specify the Kubernetes Cluster', required=True)
  parser.add_argument('-n', '--node', help='Specify the node name from the output file to drain', required=True)
  parser.add_argument('-f', '--file_path', help='Specify the file_path of the action plan creaated', required=True)
  parser.add_argument('-F', '--force_delete', help='Force drain the node', default='False')

  args = parser.parse_args()
  return args.kubecontext, args.node, args.file_path, args.force_delete

if __name__ == "__main__":
  cluster, node, file_path, force_delete = parse_arguments()
  drain_node(cluster, node, file_path, force_delete)
