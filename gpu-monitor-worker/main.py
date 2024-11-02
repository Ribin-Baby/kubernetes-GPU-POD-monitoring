from fastapi import FastAPI
from typing import List
from services.kube import Kube
import uvicorn

app = FastAPI()
kube_client = Kube()

"""
# Sample JSON data
dummy_data = [
    {
        "task": "<container-id>",
        "pid": 00000,
        "status": "RUNNING",
        "process_name": "python",
        "gpu_name": "NVIDIA L4",
        "used_gpu_memory [MiB]": 0,
        "gpu": "0",
        "gpu_util": "0",
        "mem_util": "0"
    }
]
"""

@app.get("/")
async def reload():
    return {"response": "http://localhost:8000/docs -> for how to use !"}

# Endpoint to return JSON data
@app.get("/data")
async def get_data():
    df_ctr = kube_client.get_containerids()
    df_smi = kube_client.get_nvidia_smi_memory()

    # Filter df1 rows where 'pid' is in df2['pid']
    filtered_df = df_ctr.merge(df_smi, on='pid', how='inner')
    df_gpu_util = kube_client.get_nvidia_smi_util(filtered_df)
    return {"response":df_gpu_util.to_json(orient="records")}

if __name__ == "__main__":
    uvicorn.run("main:app", host="0.0.0.0", port=8000, reload=True)