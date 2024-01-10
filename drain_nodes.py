from kubernetes import client, config
import pandas as pd
from datetime import datetime
import argparse
import time
from kubernetes.client import V1DeleteOptions
import logging
from get_details_for_kuber_cluster import convert_memory_to_ki, convert_cpu_to_millicore

def drain_node(cluster, node, action_plan_file_path, force_delete, dry_run):
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
      if not can_pods_be_rescheduled(cluster, node, action_plan_file_path):
        logging.info(f"Pods in the node: {node} identified to be deleted cannot be accomodated in any other node. Exiting.")
      else:
        logging.info(f"Identified {node} to be drained")
        if dry_run == 'True':
          logging.info(f"DRY RUN: To drain {node} run: python drain_nodes.py -c {cluster} -n {node} -f {action_plan_file_path}")
        else:
          logging.info(f"Draining Node-Name: {node}")
          drain_and_delete_node(api_client, node)
          logging.info(f"Node {node} drained successfully.")
    else:
      logging.info(f"Node-Name: {node} cannot be drained as remaining_cpu_request_percentage is {remaining_cpu_request_percentage}.")

def drain_and_delete_node(api_client, node_name, grace_period = -1, ignore_daemonsets = False):
    # Set the node unschedulable
    nodes = api_client.list_node(pretty=True)
    logging.info(f"Total number of nodes before draining: {len(nodes.items)}")
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
    nodes = api_client.list_node(pretty=True)
    logging.info(f"Total number of nodes immediately after draining: {len(nodes.items)}")
    logging.info(f"Sleeping for 5mins")
    time.sleep(300)
    nodes = api_client.list_node(pretty=True)
    logging.info(f"Total number of nodes 5mins after draining: {len(nodes.items)}")

def parse_arguments():
  parser = argparse.ArgumentParser()
  parser.add_argument('-c', '--kubecontext', help='Specify the Kubernetes Cluster', required=True)
  parser.add_argument('-n', '--node', help='Specify the node name from the output file to drain', required=True)
  parser.add_argument('-f', '--file_path', help='Specify the file_path of the action plan creaated', required=True)
  parser.add_argument('-F', '--force_delete', help='Force drain the node', default='False')
  parser.add_argument('--log', help='Set the log level', default='INFO')

  args = parser.parse_args()
  return args.kubecontext, args.node, args.file_path, args.force_delete, args.log

def can_pods_be_rescheduled(cluster, node, action_plan_file_path):
  # Load the Kubernetes configuration
  config.load_kube_config(context=cluster)

  # Create a Kubernetes API client
  api_client = client.CoreV1Api()
  pods = api_client.list_pod_for_all_namespaces(field_selector=f'spec.nodeName={node}')
  for pod in pods.items:
    allocated = False
    request_cpu = 0
    request_memory = 0
    daemon_set = False
    if pod.metadata.owner_references is not None:
      for owner in pod.metadata.owner_references:
        if owner.kind == 'DaemonSet':
          daemon_set = True
          allocated = True
          logging.debug(f"Pod {pod.metadata.name} is owned by a DaemonSet. Skipping calculation.")
          break
    else:
      daemon_set = False
    if not daemon_set:
      containers = pod.spec.containers
      for individual_container in containers:
        requests = individual_container.resources.requests
        if requests and 'memory' in requests:
          request_memory += convert_memory_to_ki(requests['memory'])
        else:
          request_memory += 0
        if requests and 'cpu' in requests:
          request_cpu += convert_cpu_to_millicore(requests['cpu'])
        else:
          request_cpu += 0
      data = pd.read_csv(action_plan_file_path)
      selected_row = data[(data['view'] == 'NODE_VIEW')]
      for index, individual_node in selected_row.iterrows():
        remaining_cpu_request = convert_cpu_to_millicore(individual_node['remaining_cpu_request'])
        remaining_memory_request = convert_memory_to_ki(individual_node['remaining_memory_request'])
        if remaining_cpu_request > request_cpu and remaining_memory_request > request_memory:
          logging.debug(f"Pod {pod.metadata.name} can be accomodated in node {individual_node['node_name']}")
          individual_node['remaining_cpu_request'] = str(remaining_cpu_request - request_cpu) + 'm'
          individual_node['remaining_memory_request'] = str(remaining_memory_request - request_memory) + 'Ki'
          allocated = True
          break
        else:
          logging.debug(f"Pod {pod.metadata.name} CANNOT be accomodated in node {individual_node['node_name']}")
          allocated = False
          continue
    if not allocated:
      return False
  return True


if __name__ == "__main__":
  cluster, node, file_path, force_delete, log_level = parse_arguments()
  numeric_level = getattr(logging, log_level.upper(), None)
  if not isinstance(numeric_level, int):
    raise ValueError(f'Invalid log level: {log_level}')
  logging.basicConfig(level=numeric_level, format='%(asctime)s - %(levelname)s - %(message)s', filename='logs/main.log')
  drain_node(cluster, node, file_path, force_delete, 'False')