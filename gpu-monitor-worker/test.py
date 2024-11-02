from services.kube import Kube
import pandas as pd

kube_client = Kube()

df_ctr = kube_client.get_containerids()
df_smi = kube_client.get_nvidia_smi_memory()

# Filter df1 rows where 'pid' is in df2['pid']
filtered_df = df_ctr.merge(df_smi, on='pid', how='inner')
df_gpu_util = kube_client.get_nvidia_smi_util(filtered_df)
print(df_gpu_util.to_json(orient="records"))
