from services.kube import Kube
import pandas as pd
import json
import requests
import subprocess

kube_client = Kube()

kube_client.load()

def extract_all_pod_details(json_data):
    """
    Process JSON data to extract pod name, namespace, node name, and container ID for all pods.
    
    :param json_data: List of pod JSON objects
    :return: List of tuples containing (pod_name, namespace, node_name, container_id)
    """
    pod_details = []

    for pod in json_data:
        # Extract the pod name and namespace
        pod_name = pod['metadata']['name']
        namespace = pod['metadata']['namespace']
        node_name = pod['spec'].get('nodeName', 'N/A')

        # Check if the pod has container statuses
        if 'status' in pod and 'containerStatuses' in pod['status']:
            for container_status in pod['status']['containerStatuses']:
                # Extract the container ID
                container_id = container_status.get('containerID', 'N/A')

                # Append the result for each container
                # {"namespace":namespace, "pod_name":pod_name, "node_name":node_name, "task":container_id}
                pod_details.append({"namespace":namespace, "pod_name":pod_name, "node_name":node_name, "task":container_id.split("://")[-1]})
    return pod_details

containerids_dicts = extract_all_pod_details(kube_client.pod_metadatas['items'])
df_containerids = pd.DataFrame(containerids_dicts)

urls = ["http://172.31.29.111:8000/data"]

final_df_list = []
for url in urls:
    command = ["curl", "-s", url]
    datas = json.loads(subprocess.run(command, stdout=subprocess.PIPE).stdout.decode('utf-8'))
    datas = json.loads(datas["response"])
    if len(datas)!=0:
        df_gpu = pd.DataFrame(datas)
        try:
            df_gpu_merged = pd.merge(df_containerids, df_gpu, on='task', how='inner')
            final_df_list.extend(df_gpu_merged.to_dict(orient='records'))
        except Exception as e:
            print("ERROR", e)

if not final_df_list:
    raise ValueError("Error: The output from curl is empty.")
else:
    print(final_df_list)