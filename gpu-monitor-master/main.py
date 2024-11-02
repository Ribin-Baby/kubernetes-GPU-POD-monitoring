import time
from prometheus_client.core import GaugeMetricFamily, REGISTRY, CounterMetricFamily
from prometheus_client import start_http_server
from prometheus_client import GC_COLLECTOR, PLATFORM_COLLECTOR, PROCESS_COLLECTOR
import subprocess
import concurrent.futures
from services.kube import Kube
import pandas as pd
import json

kube_client = Kube()

class CustomCollector(object):
    def __init__(self):
        # self.urls = [
        #     "http://172.31.29.111:8000/data",
        #     ]

        self.nodes = kube_client.get_nodes()
        self.index = 0
        self.runnning_process = [
    {
        "namespace": "default",
        "pod_name": "gpu-load-dummy",
        "node_name": "ip-dummy1",
        "task": "container-id",
        "pid": "10000",
        "status": "RUNNING",
        "process_name": "python",
        "gpu_name": "NVIDIA L4",
        "used_gpu_memory [MiB]": "0",
        "gpu": "0",
        "gpu_util": "0",
        "mem_util": "0"
    }
]

        self.executor = concurrent.futures.ThreadPoolExecutor(max_workers=1)
        self.future = self.executor.submit(self.run_curl_script)
        self.last_result = self.runnning_process  # Use the static data as initial default

    def run_curl_script(self):
        data_list = []
        for node in self.nodes:
            node = node.split("::")[-1]
            url = "http://" + node + ":8000/data"
            command = ["curl", "-s", url]
            try:
                datas = json.loads(subprocess.run(command, stdout=subprocess.PIPE).stdout.decode('utf-8'))
                datas = json.loads(datas["response"])
            except Exception as e:
                print("ERROR:", e)
                continue
            if len(datas)!=0:
                data_list.extend(datas)
        return data_list
    
    def filter_curl_results(self, datas):
        kube_client.load()
        containerids_dicts = self.extract_all_pod_details(kube_client.pod_metadatas['items'])
        df_containerids = pd.DataFrame(containerids_dicts)
        
        final_df_list = []
        try:
            df_gpu = pd.DataFrame(datas)
            df_gpu_merged = pd.merge(df_containerids, df_gpu, on='task', how='inner')
            final_df_list.extend(df_gpu_merged.to_dict(orient='records'))
        except Exception as e:
            print("ERROR:", e, datas)
            final_df_list = []
        if len(final_df_list)==0:
            final_df_list = self.last_result
        return final_df_list
        
    def extract_all_pod_details(self, json_data):
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

    def collect(self):
        labels = ["pod_name", "namespace", "node_name", "gpu"]
        if self.future.done():
            try:
                print("called", self.index, "th time")
                self.index += 1
                results = self.future.result()
                self.last_result = self.filter_curl_results(results)
                self.future = self.executor.submit(self.run_curl_script)
                # print("UPDATED", self.last_result )
            except Exception as e:
                print(f"Error during bash script execution: {e}")
        
        for result_dict in self.last_result:
            pod_name = result_dict["pod_name"]
            namespace = result_dict["namespace"]
            node_name = result_dict["node_name"]
            gpu_id = result_dict["gpu"]

            gauge_pid = GaugeMetricFamily('kube_pod_running_gpu_pid', 'What pid is the gpu pod running', labels=labels)
            gauge_pid.add_metric([pod_name, namespace, node_name, gpu_id], value=int(result_dict['pid']))
            yield gauge_pid

            gauge_name = GaugeMetricFamily('kube_pod_container_name', 'Container name', labels=labels)
            gauge_name.add_metric([pod_name, namespace, node_name, gpu_id], value=1)
            yield gauge_name

            gauge_gpu_id = GaugeMetricFamily('kube_pod_used_gpu_id', 'kuberbetes pod used gpu id', labels=labels)
            gauge_gpu_id.add_metric([pod_name, namespace, node_name, gpu_id], value=int(result_dict['gpu']))
            yield gauge_gpu_id

            gauge_util = GaugeMetricFamily('kube_pod_utilization_gpu_percent', 'kuberbetes pod gpu utilization(%)', labels=labels)
            gauge_util.add_metric([pod_name, namespace, node_name, gpu_id], value=int(result_dict['gpu_util']))
            yield gauge_util

            gauge_usage = GaugeMetricFamily('kube_pod_gpu_memory_used_MiB', 'kuberbetes pod gpu used in MiB', labels=labels)
            gauge_usage.add_metric([pod_name, namespace, node_name, gpu_id], value=int(result_dict['used_gpu_memory [MiB]']))
            yield gauge_usage

            mem_gpu_util = GaugeMetricFamily('kube_pod_utilization_gpu_mem_percent', 'kuberbetes pod gpu Memory utilization(%)', labels=labels)
            mem_gpu_util.add_metric([pod_name, namespace, node_name, gpu_id], value=int(result_dict['mem_util']))
            yield mem_gpu_util


if __name__ == "__main__":
    port = 9066
    frequency = 0.5
    
    REGISTRY.unregister(GC_COLLECTOR)
    REGISTRY.unregister(PLATFORM_COLLECTOR)
    REGISTRY.unregister(PROCESS_COLLECTOR)
    start_http_server(port)
    REGISTRY.register(CustomCollector())
    while True:
        time.sleep(frequency)

