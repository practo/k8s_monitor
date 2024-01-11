import logging
import argparse
from kubernetes import client, config

def print_number_of_nodes(cluster):
    # Load the Kubernetes configuration
    config.load_kube_config(context=cluster)

    # Create a Kubernetes API client
    api_client = client.CoreV1Api()

    nodes = api_client.list_node(pretty=True)
    logging.info(f"Total number of nodes: {len(nodes.items)}")

def print_number_of_namespaces(cluster):
    # Load the Kubernetes configuration
    config.load_kube_config(context=cluster)

    # Create a Kubernetes API client
    api_client = client.CoreV1Api()

    namespaces = api_client.list_namespace(pretty=True)
    logging.info(f"Total number of namespaces: {len(namespaces.items)}")
    namespace_names = [namespace.metadata.name for namespace in namespaces.items]
    logging.info(f"Namespace names: {namespace_names}")

def parse_arguments():
  parser = argparse.ArgumentParser()
  parser.add_argument('-c', '--kubecontext', help='Specify the Kubernetes Cluster', required=True)
  args = parser.parse_args()
  return args.kubecontext

if __name__ == "__main__":
  kubecontext = parse_arguments()
  logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s', filename='logs/main.log')
  print_number_of_nodes(kubecontext)
  print_number_of_namespaces(kubecontext)