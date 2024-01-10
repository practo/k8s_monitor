from get_details_for_kuber_cluster import create_k8s_view
from create_action_plan import create_action_plan
from drain_nodes import drain_node
import argparse
import logging
from datetime import datetime

def parse_arguments():
    """
    Parse command line arguments.

    Returns:
        tuple: A tuple containing the parsed arguments.
    """
    parser = argparse.ArgumentParser()
    parser.add_argument('-c', '--kubecontext', help='Specify the Kubernetes Cluster', required=True)
    parser.add_argument('-l', '--label_name', help='Specify the label name to add to the node', required=True)
    parser.add_argument('-v', '--label_value', help='Specify the label value to add to the node', required=True)
    parser.add_argument('--log', help='Set the log level', default='INFO')
    parser.add_argument('--dry_run', '--dry_run', help='Dry run', default='True')
    args = parser.parse_args()
    return args.kubecontext, args.label_name, args.label_value, args.log, args.dry_run

if __name__ == "__main__":
    # Parse command line arguments
    cluster, label_name, label_value, log_level, dry_run = parse_arguments()

    # Set up logging
    numeric_level = getattr(logging, log_level.upper(), None)
    if not isinstance(numeric_level, int):
        raise ValueError(f'Invalid log level: {log_level}')
    logging.basicConfig(level=numeric_level, format='%(asctime)s - %(levelname)s - %(message)s', filename='logs/main.log')

    # Now you can use logging.info(), logging.debug(), etc. throughout your code
    logging.debug("Running in Debug Mode")

    # Get current timestamp
    now = datetime.now()
    timestamp = now.strftime("%d_%m_%Y_%H_%M_%S")

    # Call function to create Kubernetes view
    logging.info(f"Creating Kubernetes view for {cluster}")
    file_name = create_k8s_view(cluster, label_name, label_value, timestamp)
    logging.info(f"Kubernetes view created successfully and written to {file_name}")
    logging.info(f"Creating action plan for {cluster}")
    node_name, action_plan_path = create_action_plan(file_name, timestamp)
    if node_name is None:
        logging.info("No node found to drain. Exiting...")
    else:
        drain_node(cluster, node_name, action_plan_path, 'False', dry_run)
