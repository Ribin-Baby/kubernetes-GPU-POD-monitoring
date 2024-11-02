import os
import json

class Kube:
    def __init__(self):
        self.pod_metadatas = {}
        
    def load(self):
        command = "microk8s kubectl get pods --all-namespaces -o=json"
        with os.popen(command) as pipe:
            output = pipe.read()
            self.pod_metadatas = json.loads(output)
    
    def get_nodes(self):
        command = "microk8s kubectl get nodes -l nvidia.com/gpu.present=true -o custom-columns=NAME:.metadata.name,INTERNAL-IP:.status.addresses[0].address  --no-headers | awk '{print $1\"::\"$2}' | paste -sd ' '"
        with os.popen(command) as pipe:
            output = pipe.read()
        return output.split()