from kubernetes import client, config

from datetime import datetime
import argparse
import csv
import re

allowed_pattern = re.compile(r'^(default|kube-node-lease|kube-public|kube-system)$')

def convert_memory_to_ki(input_mem):
  if is_numeric(input_mem):
    return float(float(input_mem)/1024)
  if input_mem[-2:] == 'Mi':
    return 1024*float(input_mem[:-2])
  elif input_mem[-2:] == 'Ki':
    return float(input_mem[:-2])
  elif input_mem[-2:] == 'Gi':
    return 1024*1024*float(input_mem[:-2])

def convert_cpu_to_millicore(input_cpu):
  if is_numeric(input_cpu): # If only int/float, then it is in full core units
    return float(input_cpu)*1000
  else:
    return float(input_cpu[:-1])

def is_numeric(s):
  try:
    float(s)
    return True
  except ValueError:
    return False

def run(cluster):
  # Configure the Kubernetes client
  config.load_kube_config(context=cluster)
  core_api = client.CoreV1Api()

  # Get all nodes
  pods = core_api.list_pod_for_all_namespaces(watch=False)
  nodes = core_api.list_node(pretty=True)

  csv_headers = ['view', 'node_name', 'allocated_cpu', 'allocated_memory', 'remaining_cpu_request', 'remaining_memory_request', 'remaining_cpu_limit', 'Rremaining_memory_limit', 'pod_name', 'container_number', 'limit_cpu', 'request_cpu', 'limit_memory', 'request_memory']
  data = []
  data.append(csv_headers)
  values_for_view = {
    'container_view': 'CONTAINER_VIEW',
    'pod_view': 'POD_VIEW',
    'node_view': 'NODE_VIEW'
  }

  # Print information about each node
  print("Nodes in minikube cluster:")
  pods_with_no_cpu_limit = []
  pods_with_no_cpu_request = []
  pods_with_no_memory_limit = []
  pods_with_no_memory_request = []
  for node in nodes.items:
    node_name = node.metadata.name
    node_status = node.status.phase
    print(f"\tNode-Name: {node.metadata.name}")
    print(f"\tNode-Status: {node.status.phase}")
    allocated_cpu = float(node.status.allocatable["cpu"]) * 1000
    allocated_memory = node.status.allocatable["memory"]
    print(f"\tAllocated CPU: {allocated_cpu}")
    print(f"\tAllocated Memory: {allocated_memory}")
    target_node_name = node.metadata.name
    filtered_pods = [pod for pod in pods.items if pod.spec.node_name == target_node_name]
    print("\n\tPods in node:")
    total_cpu_limit = 0
    total_cpu_request = 0
    total_memory_limit = 0
    total_memory_request = 0
    for pod in filtered_pods:
      if pod.status.phase in ['Succeeded', 'Failed', 'Unknown']:
        continue
      if allowed_pattern.match(pod.metadata.name):
        print(f"Skipping pod: '{pod.metadata.name}'")
        continue
      print(f"\t\tName: {pod.metadata.name}")
      cpu_usage = 0
      memory_usage = 0
      # if pod.status.container_statuses:
      containers = pod.spec.containers
      count_of_container = 1
      total_cpu_limit_for_pod = 0
      total_cpu_request_for_pod = 0
      total_memory_limit_for_pod = 0
      total_memory_request_for_pod = 0
      for individual_container in containers:
        limits = individual_container.resources.limits
        requests = individual_container.resources.requests
        print(f"\t\t\tContainer Number: {count_of_container}")
        print(f"\t\t\t\tLimits: {limits}")
        print(f"\t\t\t\tRequests: {requests}")
        limit_cpu = None
        request_cpu = None
        limit_memory = None
        request_memory = None
        limit_cpu_to_dump = None
        request_cpu_to_dump = None
        limit_memory_to_dump = None
        request_memory_to_dump = None

        if limits and 'memory' in limits:
          limit_memory = limits['memory']
        else:
          limit_memory = '0Mi'
          pods_with_no_memory_limit.append(pod.metadata.name)
        if limit_memory:
          limit_memory = convert_memory_to_ki(limit_memory)
          total_memory_limit_for_pod = total_memory_limit_for_pod + limit_memory
          limit_memory_to_dump = str(limit_memory) + 'Ki'

        # print(limit_memory)
        if limits and 'cpu' in limits:
          limit_cpu = limits['cpu']
        else:
          limit_cpu = '0m'
          pods_with_no_cpu_limit.append(pod.metadata.name)
        if limit_cpu:
          limit_cpu = convert_cpu_to_millicore(limit_cpu)
          total_cpu_limit_for_pod = total_cpu_limit_for_pod + limit_cpu
          limit_cpu_to_dump = str(limit_cpu) + 'm'

        # print(limit_cpu)
        if requests and 'memory' in requests:
          request_memory = requests['memory']
        else:
          request_memory = '0Mi'
          pods_with_no_memory_request.append(pod.metadata.name)
        if request_memory:
          request_memory = convert_memory_to_ki(request_memory)
          total_memory_request_for_pod = total_memory_request_for_pod + request_memory
          request_memory_to_dump = str(request_memory) + 'Ki'

        # print(request_memory)
        if requests and 'cpu' in requests:
          request_cpu = requests['cpu']
        else:
          request_cpu = '0m'
          pods_with_no_cpu_request.append(pod.metadata.name)
        if request_cpu:
          request_cpu = convert_cpu_to_millicore(request_cpu)
          total_cpu_request_for_pod = total_cpu_request_for_pod + request_cpu
          request_cpu_to_dump = str(request_cpu) + 'm'

        # print(request_cpu)
        data_to_be_dumped = [values_for_view['container_view'], node.metadata.name, None, None, None, None, None, None, pod.metadata.name, count_of_container, limit_cpu_to_dump, request_cpu_to_dump, limit_memory_to_dump, request_memory_to_dump]
        data.append(data_to_be_dumped)
        count_of_container = count_of_container + 1
      data_to_be_dumped = [values_for_view['pod_view'], node.metadata.name, str(allocated_cpu) + 'm', allocated_memory, None, None, None, None, pod.metadata.name, None, str(total_cpu_limit_for_pod) + 'm', str(total_cpu_request_for_pod) + 'm', str(total_memory_limit_for_pod) + 'Ki', str(total_memory_request_for_pod) + 'Ki']
      data.append(data_to_be_dumped)
      total_memory_limit = total_memory_limit + total_memory_limit_for_pod
      total_cpu_limit = total_cpu_limit + total_cpu_limit_for_pod
      total_memory_request = total_memory_request + total_memory_request_for_pod
      total_cpu_request = total_cpu_request + total_cpu_request_for_pod
      # ---

    remaining_cpu_request = allocated_cpu - total_cpu_request
    remaining_cpu_limit = allocated_cpu - total_cpu_limit
    remaining_memory_limit = float(allocated_memory[:-2]) - total_memory_limit
    remaining_memory_request = float(allocated_memory[:-2]) - total_memory_request

    data_to_be_dumped = [values_for_view['node_view'], node.metadata.name, str(allocated_cpu) + 'm', allocated_memory, str(remaining_cpu_request) + 'm', str(remaining_memory_request) + 'Ki', str(remaining_cpu_limit) + 'm', str(remaining_memory_limit) + 'Ki', None, None, None, None, None, None]
    data.append(data_to_be_dumped)

    print(f"\n\tPod with no CPU Limit: {', '.join(pods_with_no_cpu_limit)}")
    print(f"\tPod with no CPU Request: {', '.join(pods_with_no_cpu_request)}")
    print(f"\tPod with no Memory Limit: {', '.join(pods_with_no_memory_limit)}")
    print(f"\tPod with no Memory Request: {', '.join(pods_with_no_memory_request)}")

    print(f'\n\t\tRequest\tLimit')
    print(f'\tMemory\t{round(remaining_memory_request/float(allocated_memory[:-2])*100)}\t{round(remaining_memory_limit/float(allocated_memory[:-2])*100)}')
    print(f'\tCPU\t{round(remaining_cpu_request/allocated_cpu*100)}\t{round(remaining_cpu_limit/allocated_cpu*100)}')


  print("\nTotal nodes:", len(nodes.items))
  csv_file_path = 'k8s_output.csv'

  with open(csv_file_path, 'w', newline='') as csvfile:
    csv_writer = csv.writer(csvfile)
    csv_writer.writerows(data)

  print(f'\nData has been written to {csv_file_path}')




def parse_arguments():
  parser = argparse.ArgumentParser()
  parser.add_argument('-c', '--kubecontext', help='Specify the Kubernetes Cluster', required=True)
  args = parser.parse_args()
  return args.kubecontext

if __name__ == "__main__":
  cluster = parse_arguments()
  run(cluster)

