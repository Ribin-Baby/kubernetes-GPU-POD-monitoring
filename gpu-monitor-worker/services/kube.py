import os
from io import StringIO
import pandas as pd

class Kube:
    def __init__(self):
        self.pod_metadatas = {}

    def get_containerids(self):
        command = 'microk8s ctr task ls | awk \'NR==1 {print "task,pid,status"} NR>1 {print $1 "," $2 "," $3}\' OFS=\',\''
        with os.popen(command) as pipe:
            output = pipe.read()
        data = StringIO(output)
        df = pd.read_csv(data, dtype={'task': "string", 'pid': "string", 'status': 'string'})
        return df
 
    def get_nvidia_smi_memory(self):
        command = """nvidia-smi --query-compute-apps=pid,process_name,gpu_name,used_memory --format=csv,nounits"""
        with os.popen(command) as pipe:
            output = pipe.read()
        data = StringIO(output)
        df = pd.read_csv(data, sep=", ", engine="python", dtype={'pid': "string", 'process_name': "string", 'gpu_name': "string", 'used_gpu_memory [MiB]': 'string'})
        return df
    
    def add_smi_output(self, pid):
        command = f"nvidia-smi pmon -c 1 | grep {pid}" + " | awk '{print $1, $2, $4, $5}'"
        with os.popen(command) as pipe:
            output = pipe.read()
            try:
                gpu_id, pid, gpu_util, mem_util = output.split()
                return {"pid":pid, "gpu":gpu_id, "gpu_util":"0" if gpu_util == "-" else gpu_util, "mem_util":"0" if mem_util == "-" else mem_util}
            except:
                return {"pid":pid, "gpu":"0", "gpu_util":"0", "mem_util":"0"}

    def get_nvidia_smi_util(self, df):
        df_local = df['pid'].apply(self.add_smi_output).apply(pd.Series)
        if isinstance(df_local, pd.core.frame.Series):
            return df
        elif isinstance(df_local, pd.core.frame.DataFrame):
            df_local['pid'] = df_local['pid'].astype("string")
            return df.merge(df_local, on='pid', how='inner')
