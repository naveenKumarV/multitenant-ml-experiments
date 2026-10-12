from pathlib import Path
import hashlib
import importlib.metadata
import json
import platform
import sys
from datetime import datetime, timezone
from kubernetes import client, config
import torch

root=Path(__file__).resolve().parents[1]
config.load_kube_config(config_file=str(root/'.runtime/kubeconfig'),context='kind-scholarly-prototype')
core=client.CoreV1Api()
packages=(root/'requirements.lock').read_text()
result={'captured_utc':datetime.now(timezone.utc).isoformat(),'platform':platform.platform(),
 'architecture':platform.machine(),'python':sys.version,'torch':torch.__version__,
 'kubernetes':client.VersionApi().get_code().git_version,'dependency_lock':packages,
 'installed_packages':{d.metadata['Name']:d.version for d in importlib.metadata.distributions()},
 'nodes':[{'name':n.metadata.name,'os':n.status.node_info.os_image,'kernel':n.status.node_info.kernel_version,
           'container_runtime':n.status.node_info.container_runtime_version,'allocatable':n.status.allocatable}
          for n in core.list_node().items],
 'controller_images':[{'namespace':p.metadata.namespace,'pod':p.metadata.name,
  'images':[{'name':c.name,'image':c.image,'image_id':c.image_id} for c in (p.status.container_statuses or [])]}
  for ns in ['kueue-system','volcano-system'] for p in core.list_namespaced_pod(ns).items],
 'sources':{str(p.relative_to(root)):hashlib.sha256(p.read_bytes()).hexdigest()
   for folder in ['src','scripts','config','vendor'] for p in sorted((root/folder).glob('*')) if p.is_file()}}
(root/'results/environment.json').write_text(json.dumps(result,indent=2)+'\n')
print('Saved environment, controller image digests, and source checksums.')
